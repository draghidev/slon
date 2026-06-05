using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using Slon.Pipelines;
using Slon.Runtime.CompilerServices;
using Draghi.Pipelining;
using Slon.Buffers;
using Slon.Pg.Protocol.Flows;
using Slon.Transport;

namespace Slon.Pg.Protocol;

enum ProtocolStatus
{
    Created,
    Ready,
    Draining,
    Completed
}

// Lifecycle contract the protocol framework calls into. Explicit-impl on PgClientFlow forces
// any direct caller to cast (which makes the bypass visible in code).
internal interface IProtocolFlow
{
    void Start();
    void Complete(Exception? exception = null);
    void Abort();
}

interface IProtocolStatic<T>
{
    ref readonly T Value { get; }
}

sealed class PgClientProtocolOptions
{
    public PgClientProtocolOptions()
    {
        DefaultClientEncoding = Encoding.UTF8;
    }

    public PgClientProtocolOptions(PgClientOptions options)
    {
        DefaultClientEncoding = options.Encoding;
        ReadTimeout = options.ReadTimeout;
        HeartbeatInterval = options.HeartbeatInterval;
        FlowActivationTimeout = options.ConnectionTimeout;
    }

    public TimeProvider TimeProvider { get; set; } = TimeProvider.System;
    // How much time to give CompleteAsync before forcefully aborting flows.
    public TimeSpan CompletionTimeout { get; set; } = TimeSpan.FromSeconds(10);
    /// The scheduler used to dispatch pipeline wake-signal continuations.
    /// Defaults to null, in which case the pipeline falls back to the ThreadPool.
    /// Set to a custom <see cref="PipelineScheduler"/> implementation to route continuations elsewhere.
    public PipelineScheduler? ExecutionScheduler { get; set; }
    /// The scheduler used to dispatch item activations (notifying consumers their item is ready).
    /// Defaults to null, in which case activations fall back to the ThreadPool.
    public PipelineScheduler? ActivationScheduler { get; set; }
    public Encoding DefaultClientEncoding { get; set; }
    public TimeSpan FlowActivationTimeout { get; set; }
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(1);
    public TimeSpan ReadTimeout { get; set; } = Timeout.InfiniteTimeSpan;
}

sealed class PgClientProtocol
{
    readonly PgClientProtocolOptions _options;
    IOutputWriter<byte> _pipeWriter = null!;
    PgProtocolDataWriter _protocolDataWriter = null!;
    PipeSegmentEnumerator<BackendMessageBatch.Segmenter, BackendMessageBatch> _pipeSegmentEnumerator = null!;
    PgDecoder _pgDecoder = null!;

    int _pipelineStalls;
    Heartbeat? _heartbeat;
    Action? _poolConnectionIdleSignal;

    // Framework state, formerly inherited from Protocol<TFlow>.
    readonly CancellationTokenSource _abortCts;
    readonly CancellationToken _abortToken;
    Pipeline<PgClientFlow, Policy, PgClientFlowSource, PgClientFlowSource.Enumerator> _pipeline = null!;
    PgClientFlowSource _source;
    readonly Lock _syncRoot = new();
    ProtocolStatus _status = ProtocolStatus.Created;
    // Track draining count so overlapping recovery starts/ends don't signal ready too early.
    // Any concurrent CompleteAsync (which also transitions to draining) is respected the same way.
    int _drainingCount;

    PgClientProtocol(PgClientProtocolOptions options)
    {
        _options = options;
        _abortCts = new(Timeout.InfiniteTimeSpan, options.TimeProvider);
        _abortToken = _abortCts.Token;
        FlowControl = new Control(this);
    }

    public string CurrentSearchPath { get; set; } = "public";

    Control FlowControl { get; }
    CancellationToken AbortToken => _abortToken;
    public int PipelineDepth => _pipeline.Depth;
    ProtocolStatus Status => _status;

    // Source-side accessors. The PgClientFlowSource's pre-park hook reads these to decide whether
    // it must flush before the executor goes idle.
    internal long UnflushedBytes => _protocolDataWriter.UnflushedBytes;
    internal ValueTask FlushAsync(CancellationToken cancellationToken) => _protocolDataWriter.FlushAsync(cancellationToken);

    // Pool-unit accessors. PgConnection forwards its IPoolConnection<PgConnection> implementation
    // to these. Keeps the protocol package decoupled from Slon.Pools' typed context.
    internal bool IsIdle => PipelineDepth is 0;
    internal bool IsCompleted => Status is ProtocolStatus.Completed;
    internal int CompareTo(PgClientProtocol? other)
    {
        // null instances are always better, they represent empty connection slots.
        if (other is null)
            return 1;

        // Arbitrary factor for stalls :)
        var score = PipelineDepth + (_pipelineStalls * 4);
        var otherScore = other.PipelineDepth + (other._pipelineStalls * 4);

        return score < otherScore ? -1 : score == otherScore ? 0 : 1;
    }

    public static PgClientProtocol Create(PgClientProtocolOptions protocolOptions)
        => new(protocolOptions);

    void Initialize(TransportConnection connection, Action? onIdle)
    {
        _pipeWriter = connection.Writer as IOutputWriter<byte> ?? new PipeStreamingWriter(connection.Writer);
        _protocolDataWriter = new(_pipeWriter, PgClientOptions.PreStartupEncoding, connection.WaitWritable);
        _pipeSegmentEnumerator = new(connection.Reader, new(), ownsReader: true);
        _pgDecoder = new(_pipeSegmentEnumerator, AbortToken, _options.ReadTimeout);

        // onIdle being non-null is the signal that an external orchestrator (pool, PgConnection)
        // is driving us, including the heartbeat tick. When null, we run our own heartbeat so
        // standalone consumers of the protocol package get working flow activation timeouts out
        // of the box.
        if (onIdle is null)
        {
            _heartbeat = new(_options.HeartbeatInterval, _options.TimeProvider);
            _heartbeat.Register(period => Heartbeat(period));
        }
        else
        {
            _poolConnectionIdleSignal = onIdle;
        }
    }

    public void Start(PgClientOptions options, TransportConnection connection, Action? onIdle = null, TimeSpan timeout = default)
    {
        if (connection.Reader is not StreamPipeReader || connection.Writer is not StreamPipeWriter)
            ThrowHelper.ThrowInvalidOperation("Transport does not support synchronous I/O.");

        Initialize(connection, onIdle);
        var flow = new StartupFlow(async: false, options, timeout == default ? options.ConnectionTimeout : timeout);
        var task = StartAsync(flow, flow.WaitForComplete());
        Debug.Assert(task.IsCompleted);
        task.GetAwaiter().GetResult();
    }

    public async ValueTask StartAsync(PgClientOptions options, TransportConnection connection, Action? onIdle = null, CancellationToken cancellationToken = default)
    {
        Initialize(connection, onIdle);
        var flow = new StartupFlow(async: true, options, options.ConnectionTimeout);
        await StartAsync(flow, flow.WaitForComplete(cancellationToken), cancellationToken).ConfigureAwait(false);
    }

    async ValueTask StartAsync(PgClientFlow? flow, ValueTask flowCompletion, CancellationToken cancellationToken = default)
    {
        _source = PgClientFlowSource.Create(this, _options.ExecutionScheduler);
        _pipeline = Pipeline.Create<PgClientFlow, Policy, PgClientFlowSource, PgClientFlowSource.Enumerator>(new Policy(this), _source);
        if (flow is not null && !TryQueueFlow(flow, ProtocolStatus.Created))
            throw new InvalidOperationException("Could not enqueue starting flow, protocol is not in a valid state to start.");
        try
        {
            if (flowCompletion != default)
                await flowCompletion.ConfigureAwait(false);
            SignalReady();
        }
        catch (Exception ex)
        {
            Abort(ex, waitForIdle: true);
            throw;
        }
    }

    void SignalReady()
    {
        lock (_syncRoot)
        {
            if (_drainingCount > 0)
                _drainingCount--;

            if (_drainingCount is 0 && _status is not ProtocolStatus.Completed)
            {
                _status = ProtocolStatus.Ready;
            }
        }
    }

    bool SignalDraining()
    {
        lock (_syncRoot)
        {
            if (_status is not ProtocolStatus.Completed)
                _status = ProtocolStatus.Draining;
            _drainingCount++;
            return _status is ProtocolStatus.Draining;
        }
    }

    void SignalCompleted()
    {
        lock (_syncRoot)
        {
            _status = ProtocolStatus.Completed;
        }
    }

    Enumerator GetFlows() => new(_pipeline.GetEnumerator());

    public T Queue<T>(T flow) where T : PgClientFlow
    {
        if (!TryQueue(flow))
            ThrowHelper.ThrowInvalidOperation("Protocol is unavailable.");
        return flow;
    }

    public bool TryQueue(PgClientFlow flow, bool mustPipeline = false)
    {
        if (mustPipeline)
        {
            if (!TryQueueFlow(flow, static protocol => protocol.PipelineDepth > 0, this))
                return false;
        }
        else if (!TryQueueFlow(flow, null, (object?)null))
            return false;

        // Bind
        var control = flow.GetExecutionControl(FlowControl);
        control.Bind(AbortToken, _options.FlowActivationTimeout);
        if (!control.IsPipelined)
            Interlocked.Increment(ref _pipelineStalls);

        return true;
    }

    bool TryQueueFlow<TState>(PgClientFlow flow, Func<TState, bool>? predicate = null, TState state = default!)
        => TryQueueFlow(flow, ProtocolStatus.Ready, predicate, state);

    bool TryQueueFlow(PgClientFlow flow, ProtocolStatus requiredStatus) => TryQueueFlow<bool>(flow, requiredStatus);
    bool TryQueueFlow<TState>(PgClientFlow flow, ProtocolStatus requiredStatus, Func<TState, bool>? predicate = null, TState state = default!)
    {
        var isAsync = flow.IsAsyncForEnqueue;
        PgClientFlowSource.EnqueueResult enqueue = default;
        lock (_syncRoot)
        {
            if (_status != requiredStatus)
                return false;

            if (predicate?.Invoke(state) == false)
                return false;

            if (isAsync)
                enqueue = _source.Enqueue(flow);
        }
        if (isAsync)
        {
            enqueue.Execute(runContinuationsAsynchronously: true);
        }
        else
        {
            _source.EnqueueSyncWithHandoff(flow);
        }
        return true;
    }

    // TODO remove and create a drain primitive.
    Exception CreateAbortException(Exception? abortReason)
    {
        Console.WriteLine("Aborting");
        Console.WriteLine(abortReason);
        while (!Debugger.IsAttached)
            Thread.Sleep(1000);
        Debugger.Break();
        return null!;
    }

    public void Abort(Exception? exception = null) => Abort(exception, waitForIdle: true);

    void Abort(Exception? abortReason, bool waitForIdle)
    {
        // Already aborting.
        if (_abortCts.IsCancellationRequested)
            return;

        _abortCts.Cancel();

        SignalDraining();
        try
        {
            _ = _pipeline.CompleteAsync(CreateAbortException(abortReason));
        }
        finally
        {
            SignalCompleted();
        }
    }

    public async ValueTask CompleteAsync(Exception? exception = null)
    {
        // We're already done if this returns false.
        if (!SignalDraining())
            return;
        try
        {
            // Traverse remaining flows ourselves once the token fires so flows don't register/unregister on each execution.
            using var reg = _abortCts.Token.UnsafeRegister(_ =>
            {
                foreach (var flow in GetFlows())
                    ((IProtocolFlow)flow).Abort();
            }, null);

            _abortCts.CancelAfter(_options.CompletionTimeout);
            // Wait for the pipeline to complete and drain remaining items.
            await _pipeline.CompleteAsync(exception).ConfigureAwait(false);
        }
        finally
        {
            _abortCts.CancelAfter(TimeSpan.Zero);
            _heartbeat?.Dispose();
            SignalCompleted();
        }
    }

    internal ValueTask Heartbeat(TimeSpan period)
    {
        var control = FlowControl;
        foreach (var flow in GetFlows())
        {
            try
            {
                flow.GetExecutionControl(control).OnHeartbeat(period);
            }
            catch (Exception)
            {
                // TODO log it
            }
        }
        return new();
    }


    public struct Enumerator
    {
        Pipeline<PgClientFlow, Policy, PgClientFlowSource, PgClientFlowSource.Enumerator>.Enumerator _inner;

        internal Enumerator(Pipeline<PgClientFlow, Policy, PgClientFlowSource, PgClientFlowSource.Enumerator>.Enumerator inner) => _inner = inner;

        public PgClientFlow Current => _inner.Current;
        public Enumerator GetEnumerator() => this;
        public bool MoveNext() => _inner.MoveNext();
    }

    internal readonly struct Policy : IPipelinePolicy<PgClientFlow>
    {
        readonly CancellationToken _abortToken;
        readonly PgClientProtocol _protocol;
        readonly ValueTaskSourcePromise<PipelineItemResult> _promise;
        readonly ActivationWorkItem _activationWorkItem;

        public Policy(PgClientProtocol protocol)
        {
            _protocol = protocol;
            _abortToken = protocol._abortToken;
            _promise = new();
            _activationWorkItem = new(protocol.FlowControl);
            ExecutionScheduler = protocol._options.ExecutionScheduler;
            ActivationScheduler = protocol._options.ActivationScheduler;
        }

        public PipelineScheduler? ExecutionScheduler { get; }
        PipelineScheduler? ActivationScheduler { get; }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CompleteItem(PgClientFlow item, int remainingDepth, Exception? exception)
        {
            ((IProtocolFlow)item).Complete(exception);
            _protocol.FlowControl.OnCompleted(item, remainingDepth);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<PipelineItemResult> ExecuteItemAsync(PgClientFlow item, CancellationToken cancellationToken)
        {
            ((IProtocolFlow)item).Start();
            PromiseAsyncValueTaskMethodBuilder<PipelineItemResult>.Promise = _promise;
            try
            {
                return ExecuteCore(_protocol.FlowControl, item, cancellationToken);
            }
            finally
            {
                PromiseAsyncValueTaskMethodBuilder<PipelineItemResult>.Promise = null;
            }

            [AsyncMethodBuilder(typeof(PromiseAsyncValueTaskMethodBuilder<>))]
            static async ValueTask<PipelineItemResult> ExecuteCore(
                Control flowControl, PgClientFlow item, CancellationToken cancellationToken)
            {
                var tasks = await flowControl.Execute(item, cancellationToken).ConfigureAwait(false);
                return new PipelineItemResult(tasks.TrailingExecutionTask, tasks.PipelineTask);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ActivateHeadItem(PgClientFlow item, bool preferAsync = true)
        {
            // Inline-activate when the framework allows it (preferAsync=false) OR when the flow
            // is sync. Sync flows park their caller via Task.AsTask().GetAwaiter().GetResult(),
            // so the activation continuation is just a kernel wait-handle signal, bounded cost,
            // safe to run under the advancer latch. Async flows can attach arbitrary continuation
            // work via await, so they MUST go through TP to keep the latch hold bounded.
            if (preferAsync && item.IsAsyncAtBind)
            {
                _activationWorkItem.Initialize(item);
                if (ActivationScheduler is { } scheduler)
                    scheduler.SubmitDetached(ActivationWorkItemAction, _activationWorkItem, preferLocal: true);
                else
                    ThreadPool.UnsafeQueueUserWorkItem(_activationWorkItem, preferLocal: true);
            }
            else
                _protocol.FlowControl.Activate(item);
        }

        static readonly Action<object?> ActivationWorkItemAction = static state => ((IThreadPoolWorkItem)state!).Execute();

        public bool TryRecoverItemFailure(PipelineItemFailureContext context, PgClientFlow failedItem, CancellationToken cancellationToken, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out PgClientFlow? recoveryItem)
        {
            recoveryItem = null;
            return false;
        }

        sealed class ActivationWorkItem(Control flowControl) : IThreadPoolWorkItem
        {
            PgClientFlow _item = null!;
            public void Initialize(PgClientFlow item) => _item = item;
            void IThreadPoolWorkItem.Execute() => flowControl.Activate(_item);
        }
    }

    internal sealed class Control(PgClientProtocol protocol) : IProtocolStatic<CommandFlow.ReadState>
    {
        public PgClientFlow? ExecutorFlow { get; private set; }
        public PgClientFlow? ActivatedFlow { get; private set; }

        public PgProtocolDataWriter Writer => protocol._protocolDataWriter;

        public void OnParameterStatus(BackendMessage message)
        {
            message.DebugEnsureExpected(PgTypes.BackendType.ParameterStatus);
            message.DebugEnsureBuffered();

            var reader = message.BodyReader;
            _ = reader.TryPeek(out var nameStart);
            switch ((char)nameStart)
            {
            case 'c':
                // If Postgres supported ASCII incompatible encodings there would be a catch-22
                // reporting the new encoding value encoded in the new encoding.
                // As it doesn't support e.g. utf16 we can always rely on the ascii bytes,
                // which is enough to transmit encoding names.
                if (reader.IsNext("client_encoding\0"u8, advancePast: true))
                {
                    _ = reader.TryReadTo(out ReadOnlySequence<byte> value, [(byte)'\0']);
                    var newEncoding = protocol._protocolDataWriter.ClientEncoding.GetString(value);
                    // Map from PG names to ICU/IANA names https://www.iana.org/assignments/character-sets/character-sets.xhtml.
                    // https://github.com/postgres/postgres/blob/713d9a847e6409a2a722aed90975eef6d75dc701/src/common/encnames.c#L414
                    // Server reports a new client_encoding (typically from SET CLIENT_ENCODING). Map the PG name
                    // to a .NET / IANA name (per src/common/encnames.c) and refresh.
                    // SQL_ASCII is special, it explicitly means "no encoding conversion on the wire," so the .NET
                    // side keeps whatever DefaultClientEncoding the caller chose to interpret the raw bytes.
                    // Other PG names without a .NET equivalent (MULE_INTERNAL, EUC_JIS_2004, LATIN10, WIN874) are
                    // real encodings .NET can't decode, let Encoding.GetEncoding throw and break the connection.
                    newEncoding = newEncoding switch
                    {
                        "SQL_ASCII" => newEncoding,
                        "EUC_JP" => "EUC-JP",
                        "EUC_CN" => "EUC-CN",
                        "EUC_KR" => "EUC-KR",
                        "EUC_TW" => "EUC-TW",
                        "EUC_JIS_2004" => newEncoding,
                        "UTF8" => "UTF-8",
                        "MULE_INTERNAL" => newEncoding,
                        "LATIN1" => "ISO-8859-1",
                        "LATIN2" => "ISO-8859-2",
                        "LATIN3" => "ISO-8859-3",
                        "LATIN4" => "ISO-8859-4",
                        "LATIN5" => "ISO-8859-9",
                        "LATIN6" => "ISO-8859-10",
                        "LATIN7" => "ISO-8859-13",
                        "LATIN8" => "ISO-8859-14",
                        "LATIN9" => "ISO-8859-15",
                        "LATIN10" => newEncoding,
                        "WIN1256" => "CP1256",
                        "WIN1258" => "CP1258",
                        "WIN866" => "CP866",
                        "WIN874" => newEncoding,
                        "KOI8R" => "KOI8-R",
                        "WIN1251" => "CP1251",
                        "WIN1252" => "CP1252",
                        "ISO_8859_5" => "ISO-8859-5",
                        "ISO_8859_6" => "ISO-8859-6",
                        "ISO_8859_7" => "ISO-8859-7",
                        "ISO_8859_8" => "ISO-8859-8",
                        "WIN1250" => "CP1250",
                        "WIN1253" => "CP1253",
                        "WIN1254" => "CP1254",
                        "WIN1255" => "CP1255",
                        "WIN1257" => "CP1257",
                        "KOI8U" => "KOI8-U",
                        _ => newEncoding
                    };

                    try
                    {
                        if (newEncoding == "SQL_ASCII")
                            protocol._protocolDataWriter.ClientEncoding = protocol._options.DefaultClientEncoding;

                        protocol._protocolDataWriter.ClientEncoding = Encoding.GetEncoding(newEncoding);
                    }
                    catch (ArgumentException ex)
                    {
                        protocol.Abort(ex);
                        throw;
                    }
                }
                break;
            case 's':
                if (reader.IsNext("search_path\0"u8, advancePast: true))
                {
                    _ = reader.TryReadTo(out ReadOnlySequence<byte> value, [(byte)'\0']);
                    var newSearchPath = protocol._protocolDataWriter.ClientEncoding.GetString(value);
                    protocol.CurrentSearchPath = newSearchPath;
                }
                break;
            default:
                // TODO log ignored parameter status.
                break;
            }
        }

        // Allows the protocol to do any bookkeeping of transaction state and any cleanup.
        public void OnFlowRfq(BackendMessage message)
        {
            var rfq = ReadyForQueryMessage.Create(message);
        }

        [AsyncMethodBuilder(typeof(NonContextRestoringPoolingValueTaskMethodBuilder<>))]
        internal async ValueTask<FlowTasks> Execute(PgClientFlow flow, CancellationToken abortToken)
        {
            ExecutorFlow = flow;
            var writer = protocol._protocolDataWriter;
            if (writer.UnflushedBytes > 1000)
                await writer.FlushAsync(abortToken).ConfigureAwait(false);
            var flowTasks = await flow.GetExecutionControl(this).ExecuteAuto().ConfigureAwait(false);
            // TODO only null after flowTasks.TrailingExecutionTask is completed.
            ExecutorFlow = null;
            return new(flowTasks.TrailingExecutionTask, flowTasks.PipelineTask);
        }

        internal void Activate(PgClientFlow flow)
        {
            ActivatedFlow = flow;
            var control = flow.GetExecutionControl(this);
            if (!control.IsPipelined)
                Interlocked.Decrement(ref protocol._pipelineStalls);
            var decoder = protocol._pgDecoder;
            decoder.Initialize(this);
            control.Activate(decoder);
        }

        internal void OnCompleted(PgClientFlow flow, int remainingDepth)
        {
            ActivatedFlow = null;
            if (remainingDepth is 0)
            {
                // Get rid of any rooted buffers, refs and so on.
                _commandFlowReadState = new();
                protocol._poolConnectionIdleSignal?.Invoke();
            }
        }

        CommandFlow.ReadState _commandFlowReadState = new();
        ref readonly CommandFlow.ReadState IProtocolStatic<CommandFlow.ReadState>.Value
            => ref _commandFlowReadState;
    }
}

