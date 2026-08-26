using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Immutable;
using System.Diagnostics;
using Slon.Runtime;
using System.Runtime.CompilerServices;
using System.Security.Authentication;
using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Slon.Pipelines;
using Slon.Runtime.CompilerServices;
using Slon.Threading;
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

[Flags]
enum FlowEnqueueOptions : byte
{
    None = 0,
    RequireExistingPipeline = 1,
    BlockAdmission = 2,
    AllowMigration = 4
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
        WriteTimeout = options.WriteTimeout;
        HeartbeatInterval = options.HeartbeatInterval;
        TimeProvider = options.TimeProvider;
        CancellationTimeout = options.CancellationTimeout;
        CancellationRetryInterval = options.CancellationRetryInterval;
        FlowActivationTimeout = options.ConnectionTimeout;
        SessionReset = options.SessionReset.Snapshot();
        DataRowStreamingThreshold = options.DataRowStreamingThreshold;
        MaxInFlightFlowsPerWire = options.MaxInFlightFlowsPerWire;
        ExecutionScheduler = options.ExecutionScheduler;
        LoggerFactory = options.LoggerFactory;
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
    public TimeSpan HeartbeatInterval { get; set; } = Heartbeat.DefaultInterval;
    public TimeSpan ReadTimeout { get; set; } = PgClientOptions.DefaultReadTimeout;
    public TimeSpan WriteTimeout { get; set; } = TimeSpan.FromSeconds(10);
    // Allocation-free grace before starting a backend CancelRequest. Heartbeat supplies the clock.
    public TimeSpan CancelRequestDelay { get; set; }
    public TimeSpan CancellationTimeout { get; set; } = TimeSpan.FromSeconds(10);
    public TimeSpan CancellationRetryInterval { get; set; } = TimeSpan.FromSeconds(1);
    internal PgSessionResetOptions SessionReset { get; set; } = new();
    public int DataRowStreamingThreshold { get; set; } = BackendMessageBatch.Segmenter.DefaultDataRowStreamingThreshold;
    internal int MaxInFlightFlowsPerWire { get; set; }
    public ILoggerFactory LoggerFactory { get; set; } = NullLoggerFactory.Instance;
    // Datasource bootstrap supplies this. A standalone raw protocol can omit backend identity and
    // operate with the explicit lower-level compatibility capabilities instead.
    public PgBackendInfoProvider? BackendProvider { get; set; }
    public PgBackendInfo? ExpectedBackendInfo { get; set; }
    public PgBackendCapabilities BackendCapabilities { get; set; }
        = PgBackendCapabilities.PostgreSqlCompatibility;

    /// Sends a side-channel CancelRequest and classifies whether request bytes may have reached
    /// PostgreSQL. The attempt must have ended before it returns. Null disables server cancellation.
    public Func<int, int, CancellationToken, ValueTask<CancelRequestState>>? CancelSender { get; set; }
    // Temporary certification seam. Remove once the cancellation read-timeout witnesses settle.
    internal Action? ReadTimeoutArmed { get; set; }
}

sealed partial class PgClientProtocol : IDisposable, IAsyncDisposable
{
    internal abstract class LoadObserver
    {
        internal abstract void OnFlowQueued(bool stallsPipeline);
        internal abstract void OnFlowActivated();
        internal abstract void OnFlowReleased(bool stallsPipeline);
    }

    internal readonly struct Hosting
    {
        Hosting(bool drivesHeartbeat, Action? admissionAvailable, LoadObserver? loadObserver)
        {
            DrivesHeartbeat = drivesHeartbeat;
            AdmissionAvailable = admissionAvailable;
            LoadObserver = loadObserver;
        }

        public bool DrivesHeartbeat { get; }
        public Action? AdmissionAvailable { get; }
        public LoadObserver? LoadObserver { get; }

        public static Hosting Connection { get; } = new(
            drivesHeartbeat: true, admissionAvailable: null, loadObserver: null);

        public static Hosting Pooled(Action admissionAvailable, LoadObserver loadObserver)
        {
            ArgumentNullException.ThrowIfNull(admissionAvailable);
            ArgumentNullException.ThrowIfNull(loadObserver);
            return new(drivesHeartbeat: true, admissionAvailable, loadObserver);
        }
    }

    internal readonly struct Startup
    {
        readonly PgClientProtocol _protocol;
        readonly TransportConnection _transport;
        readonly StartupFlow _flow;
        readonly Action _onStarted;

        internal Startup(PgClientProtocol protocol, TransportConnection transport, StartupFlow flow,
            Action onStarted)
        {
            _protocol = protocol;
            _transport = transport;
            _flow = flow;
            _onStarted = onStarted;
        }

        internal PgClientProtocol Protocol => _protocol;
        internal PgClientOptions Options => _flow.Options;

        internal void Start(Hosting hosting = default)
        {
            _protocol.Start(_transport, _flow, hosting);
            _onStarted();
        }

        internal async ValueTask StartAsync(Hosting hosting = default,
            CancellationToken cancellationToken = default)
        {
            await _protocol.StartAsync(_transport, _flow, hosting, cancellationToken)
                .ConfigureAwait(false);
            _onStarted();
        }
    }

    readonly PgClientProtocolOptions _options;
    readonly PgSessionResetOptions _sessionReset;
    readonly ILogger _logger;
    TransportConnection _connection = null!;
    IOutputWriter _pipeWriter = null!;
    ProtocolDataWriter _protocolDataWriter = null!;
    PipeSegmentEnumerator<BackendMessageBatch.Segmenter, BackendMessageBatch> _pipeSegmentEnumerator = null!;
    PgDecoder _pgDecoder = null!;

    LoadObserver? _loadObserver;
    Heartbeat? _heartbeat;
    Action? _admissionAvailable;
    PipelineScheduler _executionScheduler = null!;
    PipelineScheduler _activationScheduler = null!;

    // BackendKeyData used for diagnostics and side-channel cancellation.
    int _backendProcessId;
    int _backendSecretKey;
    readonly PgServerParameterState _serverParameterState = new();
    PgBackendInfo? _backendInfo;
    PgBackendCapabilities _backendCapabilities;
    string? _sessionResetCommand;
    // Last transaction status observed at RFQ, shared by outer and nested flows on this wire.
    TransactionStatus _transactionStatus;

    // Stopping requests an orderly drain; abort terminates wire I/O. CloseSignal publishes the reason
    // before either token fires, so token-driven observers always see the matching close state.
    readonly CloseSignal _close;
    Pipeline<PgClientFlow, Policy, PgClientFlowSource, PgClientFlowSource.Enumerator> _pipeline = null!;
    PgClientFlowSource _source;
    PgClientFlowBindingContext? _flowBindingContext;
    Func<FlowMigration, bool>? _flowMigration;
    // Reused exclusive-scope state. The outer pipeline admits only one such scope at a time.
    ExclusiveScopeState? _exclusiveScope;
    readonly Lock _syncRoot = new();
    ProtocolStatus _status = ProtocolStatus.Created;
    int _admissionBlocked;
    int _queryProtocolEstablished;
    // Track draining count so overlapping recovery starts/ends don't signal ready too early.
    // Any concurrent CompleteAsync (which also transitions to draining) is respected the same way.
    int _drainingCount;

    PgClientProtocol(PgClientProtocolOptions options)
    {
        ArgumentNullException.ThrowIfNull(options.LoggerFactory);
        _options = options;
        _sessionReset = options.SessionReset.Snapshot();
        _logger = options.LoggerFactory.CreateLogger("Slon.Pg.Protocol");
        _backendCapabilities = options.BackendCapabilities;
        _close = CloseSignal.CreateRoot();
        FlowControl = new Control(this, poolFacing: true);
    }

    public string CurrentSearchPath { get; set; } = "public";

    internal Control FlowControl { get; }
    CancellationToken AbortToken => _close.AbortToken;
    CancellationToken StoppingToken => _close.StoppingToken;
    public int PipelineDepth => _pipeline.Depth;
    // Undispatched source work. Together with depth, this is the protocol's outstanding load.
    public int Backlog => _source.Backlog;
    public int Outstanding => _pipeline.Depth + _source.Backlog;

    ProtocolStatus Status
        => (ProtocolStatus)Volatile.Read(ref Unsafe.As<ProtocolStatus, int>(ref _status));
    void SetStatus(ProtocolStatus status)
        => Volatile.Write(ref Unsafe.As<ProtocolStatus, int>(ref _status), (int)status);

    // Used by the source before parking; pre-initialization has no unflushed bytes.
    internal long UnflushedBytes => _protocolDataWriter?.UnflushedBytes ?? 0;
    internal ValueTask FlushAsync(CancellationToken cancellationToken) => _protocolDataWriter.FlushAsync(cancellationToken);

    internal bool IsCompleted => Status is ProtocolStatus.Completed;
    // Draining immediately vetoes new pool placement, before terminal completion.
    internal bool IsDraining => Status is ProtocolStatus.Draining or ProtocolStatus.Completed;
    internal bool IsSchedulable
        => Status is ProtocolStatus.Ready && Volatile.Read(ref _admissionBlocked) == 0 && _source.HasCapacity;

    internal void SetFlowBindingContext(PgClientFlowBindingContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        if (Interlocked.CompareExchange(ref _flowBindingContext, context, null) is not null)
            ThrowHelper.ThrowInvalidOperation("The protocol flow binding context was already configured.");
    }

    internal void SetFlowMigration(Func<FlowMigration, bool> migration)
    {
        ArgumentNullException.ThrowIfNull(migration);
        if (Interlocked.CompareExchange(ref _flowMigration, migration, null) is not null)
            ThrowHelper.ThrowInvalidOperation("The protocol inert-flow migration handler was already configured.");
    }

    internal bool TryBeginPruning()
    {
        lock (_syncRoot)
        {
            if (_status is not ProtocolStatus.Ready || _admissionBlocked != 0 || Outstanding != 0)
                return false;

            SetStatus(ProtocolStatus.Draining);
            return true;
        }
    }

    // Published before close tokens fire.
    internal PgClientClosedException? CloseReason => _close.Reason;
    // Used by state queries, recovery, and pool steering.
    internal TransactionStatus TransactionStatus => _transactionStatus;
    // Raw shutdown cause, or null after clean completion.
    internal Exception? CompletionException => _close.Reason?.InnerException;
    internal string? SessionResetCommand => _sessionResetCommand;
    public static PgClientProtocol Create(PgClientProtocolOptions protocolOptions)
        => new(protocolOptions);

    void Initialize(TransportConnection connection, Hosting hosting)
    {
        _executionScheduler = _options.ExecutionScheduler
            ?? PipelineScheduler.ThreadPool;
        _activationScheduler = _options.ActivationScheduler
            ?? PipelineScheduler.ThreadPool;

        _connection = connection;
        _pipeWriter = connection.Writer as IOutputWriter ?? new PipeOutputWriter(connection.Writer);
        _protocolDataWriter = new(_pipeWriter, PgClientOptions.PreStartupEncoding,
            connection.WaitUntilWritable, AbortToken, FlowControl, _options.WriteTimeout);
        _pipeSegmentEnumerator = new(connection.Reader,
            new(_options.DataRowStreamingThreshold), ownsReader: true);
        _pgDecoder = new(_pipeSegmentEnumerator, AbortToken, _options.ReadTimeout, _options.ReadTimeoutArmed);
        _admissionAvailable = hosting.AdmissionAvailable;
        _loadObserver = hosting.LoadObserver;

        if (!hosting.DrivesHeartbeat)
        {
            _heartbeat = new(_options.HeartbeatInterval, _options.TimeProvider, _logger);
            _heartbeat.Register(period => Heartbeat(period));
        }
    }

    public void Start(PgClientOptions options, TransportConnection connection,
        TimeSpan timeout = default)
        => StartCore(options, connection, default, timeout);

    internal void Start(PgClientOptions options, TransportConnection connection,
        Hosting hosting, TimeSpan timeout = default)
        => StartCore(options, connection, hosting, timeout);

    void StartCore(PgClientOptions options, TransportConnection connection,
        Hosting hosting, TimeSpan timeout)
    {
        var deadline = new Deadline(timeout == default ? options.ConnectionTimeout : timeout);
        Start(connection, new StartupFlow(async: false, options, null, deadline.GetRemaining()),
            hosting);
    }

    internal void Start(TransportConnection connection, StartupFlow flow,
        Hosting hosting = default)
    {
        try
        {
            if (connection.Reader is not StreamPipeReader || connection.Writer is not StreamPipeWriter)
                ThrowHelper.ThrowInvalidOperation("Transport does not support synchronous I/O.");

            Initialize(connection, hosting);
            var task = StartAsync(flow, flow.WaitForComplete());
            Debug.Assert(task.IsCompleted);
            task.AsTask().GetAwaiter().GetResult();
        }
        catch (Exception ex) when (Status is ProtocolStatus.Created)
        {
            ReleaseTransportOnStartFailure(connection, ex);
            throw;
        }
    }

    public ValueTask StartAsync(PgClientOptions options, TransportConnection connection,
        CancellationToken cancellationToken = default)
        => StartAsyncCore(options, connection, default, cancellationToken);

    internal ValueTask StartAsync(PgClientOptions options, TransportConnection connection,
        Hosting hosting, CancellationToken cancellationToken = default)
        => StartAsyncCore(options, connection, hosting, cancellationToken);

    ValueTask StartAsyncCore(PgClientOptions options, TransportConnection connection,
        Hosting hosting, CancellationToken cancellationToken)
        => StartAsync(connection,
            new StartupFlow(async: true, options, null, options.ConnectionTimeout),
            hosting, cancellationToken);

    internal async ValueTask StartAsync(TransportConnection connection, StartupFlow flow,
        Hosting hosting = default, CancellationToken cancellationToken = default)
    {
        try
        {
            Initialize(connection, hosting);
            await StartAsync(flow, flow.WaitForComplete(cancellationToken), cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (Status is ProtocolStatus.Created)
        {
            ReleaseTransportOnStartFailure(connection, ex);
            throw;
        }
    }

    /// <summary>Performs PostgreSQL SSLRequest negotiation.</summary>
    /// <returns><see langword="true"/> when the caller must upgrade the transport before performing further I/O; otherwise <see langword="false"/>.</returns>
    public static bool NegotiateSsl(TransportConnection connection, PostgreSqlSslMode mode, TimeSpan timeout = default)
    {
        ArgumentNullException.ThrowIfNull(connection);

        var deadline = new Deadline(timeout);
        Span<byte> request = stackalloc byte[8];
        BinaryPrimitives.WriteInt32BigEndian(request, 8);
        BinaryPrimitives.WriteInt32BigEndian(request[4..], 80877103);
        ((StreamPipeWriter)connection.Writer).Write(request, deadline.GetRemaining());
        var read = ((StreamPipeReader)connection.Reader).ReadAtLeast(1, deadline.GetRemaining());
        if (read.Buffer.IsEmpty)
            throw new EndOfStreamException("PostgreSQL closed the connection before answering the SSL request.");
        var response = read.Buffer.FirstSpan[0];
        var responseLength = read.Buffer.Length;
        connection.Reader.AdvanceTo(read.Buffer.GetPosition(1));
        EnsureNoAdditionalSslResponseData(responseLength);
        return ShouldUpgradeTransport(response, mode);
    }

    /// <summary>Performs PostgreSQL SSLRequest negotiation.</summary>
    /// <returns><see langword="true"/> when the caller must upgrade the transport before performing further I/O; otherwise <see langword="false"/>.</returns>
    public static async ValueTask<bool> NegotiateSslAsync(TransportConnection connection,
        PostgreSqlSslMode mode, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);

        var request = new byte[8];
        BinaryPrimitives.WriteInt32BigEndian(request, 8);
        BinaryPrimitives.WriteInt32BigEndian(request.AsSpan(4), 80877103);
        var write = await connection.Writer.WriteAsync(request, cancellationToken).ConfigureAwait(false);
        if (write.IsCanceled)
        {
            cancellationToken.ThrowIfCancellationRequested();
            throw new OperationCanceledException();
        }
        if (write.IsCompleted)
            throw new EndOfStreamException("The transport completed while sending the PostgreSQL SSL request.");

        var read = await connection.Reader.ReadAtLeastAsync(1, cancellationToken).ConfigureAwait(false);
        if (read.IsCanceled)
        {
            cancellationToken.ThrowIfCancellationRequested();
            throw new OperationCanceledException();
        }
        if (read.Buffer.IsEmpty)
            throw new EndOfStreamException("PostgreSQL closed the connection before answering the SSL request.");
        var response = read.Buffer.FirstSpan[0];
        var responseLength = read.Buffer.Length;
        connection.Reader.AdvanceTo(read.Buffer.GetPosition(1));
        EnsureNoAdditionalSslResponseData(responseLength);
        return ShouldUpgradeTransport(response, mode);
    }

    static bool ShouldUpgradeTransport(byte response, PostgreSqlSslMode mode)
    {
        if (response is (byte)'S')
            return true;
        if (response is (byte)'N')
        {
            if (mode is PostgreSqlSslMode.Prefer)
                return false;
            throw new PgClientException(new AuthenticationException(
                "PostgreSQL rejected the required TLS connection."));
        }
        throw new PgClientException(new PgProtocolException(
            $"PostgreSQL returned an invalid SSL response byte: 0x{response:X2}."));
    }

    static void EnsureNoAdditionalSslResponseData(long responseLength)
    {
        if (responseLength != 1)
            throw new PgClientException(new PgProtocolException(
                "PostgreSQL sent additional unencrypted data with the SSL response."));
    }

    // Startup failed before the protocol could take over teardown - the sync-capability check,
    // Initialize, pipeline construction, or queueing the startup flow, all before FailProtocol can run
    // (it needs the pipeline). Release the just-connected transport so the socket doesn't leak. The
    // callers' Status==Created filter skips failures the startup flow itself raised: those go through
    // FailProtocol -> Shutdown, which transitions past Created and owns the teardown.
    static void ReleaseTransportOnStartFailure(TransportConnection connection, Exception reason)
    {
        connection.Abort();
        connection.Writer.Complete(reason);
        connection.Reader.Complete(reason);
    }

    async ValueTask StartAsync(StartupFlow flow, ValueTask<PgClientFlow> flowCompletion, CancellationToken cancellationToken = default)
    {
        _source = PgClientFlowSource.Create(
            this, FlowControl, _executionScheduler, _options.MaxInFlightFlowsPerWire);
        _pipeline = Pipeline.Create<PgClientFlow, Policy, PgClientFlowSource, PgClientFlowSource.Enumerator>(new Policy(this, FlowControl), _source);
        FlowControl.BindSource(_source);
        FlowControl.BindPipeline(_pipeline);
        // Seed the wire's transaction status to Idle before the startup flow is queued. A fresh
        // connection holds no transaction, and StartupFlow's terminating RFQ doesn't route through
        // OnFlowRfq (it never arms _rfqCount - see CopyStartupBuffer), so without this seed the
        // startup flow's own CompleteItem would hit GuardWireIdleOnHandoff with the Unknown default
        // and fail a healthy connection. Set before TryQueueFlow so it precedes that flow's completion.
        _transactionStatus = TransactionStatus.Idle;
        if (!TryQueueFlow(flow, ProtocolStatus.Created))
            throw new InvalidOperationException("Could not enqueue starting flow, protocol is not in a valid state to start.");
        try
        {
            if (flowCompletion != default)
                await flowCompletion.ConfigureAwait(false);
            // Pull the BackendKeyData values once startup has settled. The flow's task chain is
            // the happens-before edge, so the values are visible here. After this single write the
            // fields are effectively readonly.
            _backendProcessId = flow.BackendProcessId;
            _backendSecretKey = flow.BackendSecretKey;
            var startupParameters = _serverParameterState.CompleteStartup();
            if (_options.BackendProvider is { } backendProvider)
            {
                _backendInfo = backendProvider.CreateBackendInfo(startupParameters);
                if (_options.ExpectedBackendInfo is { } expectedBackendInfo)
                    backendProvider.ValidateConnectionCompatibility(expectedBackendInfo, _backendInfo);
                _backendCapabilities = _backendInfo.Capabilities;
                _sessionResetCommand = backendProvider.ResolveSessionResetCommand(_sessionReset, _backendInfo);
            }
            else
            {
                _sessionResetCommand = _sessionReset.ResolveCommand(_backendCapabilities);
            }
            Volatile.Write(ref _queryProtocolEstablished, 1);
            SignalReady();
        }
        catch (Exception ex)
        {
            FailProtocol(ex);
            throw;
        }
    }

    void SignalReady()
    {
        var becameReady = false;
        lock (_syncRoot)
        {
            if (_drainingCount > 0)
                _drainingCount--;

            if (_drainingCount is 0 && _status is not ProtocolStatus.Completed)
            {
                becameReady = _status is not ProtocolStatus.Ready;
                SetStatus(ProtocolStatus.Ready);
            }
        }

        if (becameReady && IsSchedulable)
            NotifyAdmissionAvailable();
    }

    void SignalDraining()
    {
        lock (_syncRoot)
        {
            if (_status is ProtocolStatus.Completed)
                return;
            _drainingCount++;
            SetStatus(ProtocolStatus.Draining);
        }
    }

    internal void ReleaseAdmissionBarrier()
    {
        var signal = false;
        lock (_syncRoot)
        {
            Debug.Assert(_admissionBlocked == 1);
            _admissionBlocked = 0;
            signal = IsSchedulable;
        }

        if (signal)
            NotifyAdmissionAvailable();
    }

    void ReleaseWireCapacity()
    {
        if (!_source.ReleaseCapacity() || !IsSchedulable)
            return;

        NotifyAdmissionAvailable();
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    void NotifyAdmissionAvailable()
    {
        try { _admissionAvailable?.Invoke(); }
        catch (Exception ex)
        {
            SlonLogMessages.UnobservedCallbackException(
                _logger, ex, "the admission-availability callback");
            FailProtocol(ex);
        }
    }

    void SignalCompleted()
    {
        lock (_syncRoot)
        {
            SetStatus(ProtocolStatus.Completed);
        }
    }

    Enumerator GetFlows() => new(this);

    public T Queue<T>(T flow, CancellationToken cancellationToken = default) where T : PgClientFlow
    {
        if (!TryQueue(flow, cancellationToken: cancellationToken))
            ThrowHelper.ThrowInvalidOperation("Protocol is unavailable.");
        return flow;
    }

    public bool TryQueue(PgClientFlow flow, FlowEnqueueOptions options = FlowEnqueueOptions.None,
        CancellationToken cancellationToken = default)
    {
        // Bind the caller token before enqueue so the eager write reads it (published with the flow
        // by the enqueue). Only when cancelable - the common no-token submit pays no field write.
        if (cancellationToken.CanBeCanceled)
            flow.BindCallerToken(cancellationToken);

        if ((options & FlowEnqueueOptions.RequireExistingPipeline) != 0)
        {
            if (!TryQueueFlow(flow, options, static protocol => protocol.PipelineDepth > 0, this))
                return false;
        }
        else if (!TryQueueFlow(flow, options, null, (object?)null))
            return false;

        try
        {
            var control = flow.GetExecutionControl(FlowControl);
            if (flow.NeedsSyncHandoff && flow.DefersSyncHandoff)
                _source.SignalExecutor();
            _loadObserver?.OnFlowQueued(control.StallsPipeline);
        }
        catch (Exception ex)
        {
            // TryQueueFlow already crossed the ownership boundary. Keep the committed result and
            // terminate the protocol instead of exposing this as a failed placement.
            FailProtocol(ex);
        }

        return true;
    }

    // Begin an exclusive-access scope: the user-driven sibling of the startup handshake. Builds (or
    // reuses) a nested pipeline (poolFacing:false, so no pool-unit signaling). A recoverable inner
    // failure resyncs the shared wire but terminates this private pipeline instead of restoring admission.
    // and queues the cached ExclusiveAccessFlow on the outer pipeline. A per-acquisition lease is returned:
    // await HandoffReady to acquire, submit subflows, CompleteScopeAsync to release. One scope at a time
    // per connection.
    //
    // An ADO connection IS an exclusive scope (the protocol underneath is pooled and outlives the
    // connection), so connection-dispose is scope teardown, not protocol teardown. A linked CloseSignal
    // gives the per-scope decoder/writer shells stable tokens while allowing root protocol termination to
    // cascade through every scope using the shared physical pipes. The shells are created once here
    // alongside the flyweight and reused across scopes.
    // Low-level queue half used by handoff/race tests. Ordinary callers use the acquired-scope
    // methods below so the queue/ownership split does not leak through their control flow.
    internal ExclusiveScopeLease QueueExclusiveScope(bool async, bool longRunning = false)
    {
        var options = longRunning ? FlowEnqueueOptions.BlockAdmission : FlowEnqueueOptions.None;
        if (!TryQueueExclusiveScope(async, options, out var flow))
            ThrowHelper.ThrowInvalidOperation("Protocol is unavailable.");
        return flow;
    }

    internal bool TryQueueExclusiveScope(bool async, FlowEnqueueOptions options,
        [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out ExclusiveScopeLease? scope)
    {
        ExclusiveAccessFlow? flow;
        PgClientFlowSource.EnqueueResult enqueue = default;
        bool handoff;
        bool capacityOwned;
        lock (_syncRoot)
        {
            if (_status is not ProtocolStatus.Ready || _admissionBlocked != 0 ||
                ((options & FlowEnqueueOptions.RequireExistingPipeline) != 0 && _pipeline.Depth == 0) ||
                !_source.TryAcquireCapacity(out capacityOwned))
            {
                scope = null;
                return false;
            }

            try
            {
                // Creation and enqueue share the shutdown admission lock: a rejected begin never touches
                // the close signal, while an admitted flow is owned by the pipeline drain.
                _exclusiveScope ??= ExclusiveScopeState.Create(this);
                flow = _exclusiveScope.RentFlow();
                flow.PrepareScope(async, _options.FlowActivationTimeout);
                flow.SetEnqueueOptions(options);
                if (capacityOwned)
                    flow.MarkWireCapacityOwned();
                scope = flow.CreateLease();
            }
            catch
            {
                _source.ReleaseUnboundCapacity();
                throw;
            }

            if ((options & FlowEnqueueOptions.BlockAdmission) != 0)
            {
                Debug.Assert(_admissionBlocked == 0);
                _admissionBlocked = 1;
            }

            handoff = flow.NeedsSyncHandoff;
            if (!handoff)
                enqueue = _source.Enqueue(flow, activationTimeout: _options.FlowActivationTimeout);
            else
                _source.EnqueueSyncWaiter(flow, _options.FlowActivationTimeout);
        }

        try
        {
            if (!handoff)
                enqueue.Execute(runContinuationsAsynchronously: true);

            var control = flow.GetExecutionControl(FlowControl);
            _loadObserver?.OnFlowQueued(control.StallsPipeline);
        }
        catch (Exception ex)
        {
            // The source owns the flow from the enqueue above. In particular, do not let a pool
            // scheduler mistake a synchronous driver/callback failure for candidate rejection.
            // Termination faults the committed scope and retires the candidate through that flow.
            FailProtocol(ex);
        }
        return true;
    }

    internal ExclusiveScopeLease BeginExclusiveScope(bool longRunning = false)
    {
        var scope = QueueExclusiveScope(async: false, longRunning);
        scope.WaitForHandoffSynchronously();
        return scope;
    }

    internal void DriveSyncHandoff(PgClientFlow flow) => _source.WaitForExecutor(flow);

    internal async ValueTask<ExclusiveScopeLease> BeginExclusiveScopeAsync(
        CancellationToken cancellationToken = default, bool longRunning = false)
    {
        var scope = QueueExclusiveScope(async: true, longRunning);
        await scope.WaitForHandoffAsync(cancellationToken).ConfigureAwait(false);
        return scope;
    }

    bool TryQueueFlow<TState>(PgClientFlow flow, FlowEnqueueOptions options,
        Func<TState, bool>? predicate = null, TState state = default!)
        => TryQueueFlow(flow, ProtocolStatus.Ready, options, predicate, state);

    bool TryQueueFlow(PgClientFlow flow, ProtocolStatus requiredStatus)
        => TryQueueFlow<bool>(flow, requiredStatus, FlowEnqueueOptions.None);
    bool TryQueueFlow<TState>(PgClientFlow flow, ProtocolStatus requiredStatus,
        FlowEnqueueOptions options, Func<TState, bool>? predicate = null, TState state = default!)
    {
        // A handoff-capable sync flow is held at its FIFO turn. Consumer-driven flows defer the
        // handoff; self-driven flows take it as part of admission.
        var handoff = flow.NeedsSyncHandoff;
        PgClientFlowSource.EnqueueResult enqueue = default;
        var capacityOwned = false;
        lock (_syncRoot)
        {
            if (_status != requiredStatus ||
                (requiredStatus is ProtocolStatus.Ready && _admissionBlocked != 0))
                return false;

            if (predicate?.Invoke(state) == false)
                return false;

            if (requiredStatus is ProtocolStatus.Ready &&
                !_source.TryAcquireCapacity(out capacityOwned))
                return false;

            try
            {
                flow.SetEnqueueOptions(options);
                if (capacityOwned)
                    flow.MarkWireCapacityOwned();
            }
            catch
            {
                if (capacityOwned)
                    _source.ReleaseUnboundCapacity();
                throw;
            }

            // Both modes write the SPSC storage, so the enqueue must serialize with concurrent
            // same-protocol producers (single-producer contract). The sync flow goes in at its real FIFO
            // position; any blocking handoff happens outside the lock. Depth is counted at dispatch
            // (executor-single-writer), so producers do not update it.
            if (!handoff)
                enqueue = _source.Enqueue(flow, inlineEligible: _pipeline.Depth == 0 && _source.Backlog == 0,
                    activationTimeout: _options.FlowActivationTimeout);
            else
                _source.EnqueueSyncWaiter(flow, _options.FlowActivationTimeout);
        }
        try
        {
            if (!handoff)
                enqueue.Execute(runContinuationsAsynchronously: false);
            else if (!flow.DefersSyncHandoff)
                _source.WaitForExecutor(flow);
        }
        catch (Exception ex)
        {
            // Enqueue is the ownership boundary. Once crossed, report success to placement callers
            // and let protocol termination fault and retire the committed flow.
            FailProtocol(ex);
        }
        return true;
    }

    /// Awaitable teardown, keyed on <paramref name="closeReason"/> the way Pipe/Channel Complete is: a
    /// NULL reason is a GRACEFUL close (drain in-flight up to CompletionTimeout, then escalate to RST); a
    /// NON-NULL reason is a FORCEFUL abort (RST immediately, the reason being the in-flight flows' fault).
    /// Either way returns only once the pipeline has fully drained - the awaitable counterpart to the
    /// fire-and-forget <see cref="DisposeAsync"/> / <see cref="Dispose"/> / <see cref="FailProtocol"/>.
    /// So forceful-and-await is just CompleteAsync(reason); graceful-and-await is CompleteAsync().
    public Task CompleteAsync(Exception? closeReason = null)
        => Shutdown(closeReason, forceful: closeReason is not null, collateral: false);

    /// Async forceful tear-down. Fires AbortToken immediately, fails activations for pipelined
    /// flows. The pipeline drain unwinds in the background; this method does NOT await it (the
    /// returned task is the entry-point handle, not the drain). Callers that need to observe
    /// drain completion should call <see cref="CompleteAsync"/> first.
    public ValueTask DisposeAsync()
    {
        try
        {
            FireAndForget(DisposeAsyncCore(closeReason: null, collateral: false));
            return ValueTask.CompletedTask;
        }
        catch (Exception ex)
        {
            return ValueTask.FromException(ex);
        }
    }

    /// Synchronous tear-down for callers that can't go async (the canonical case is
    /// <see cref="IDisposable.Dispose"/>'s sync contract bubbling down to
    /// connection/protocol cleanup). Same fire-and-forget semantics as <see cref="DisposeAsync"/>:
    /// AbortToken fires immediately, pipeline drain happens in the background. Idempotent.
    public void Dispose()
        => FireAndForget(DisposeAsyncCore(closeReason: null, collateral: false));

    /// Internal emergency self-evict for the two framework-internal "we cannot continue" sites
    /// (startup catch, OnParameterStatus encoding failure). Fire-and-forget shape so it can run
    /// from the message-processing thread. Pool eviction picks up via the status flag.
    void FailProtocol(Exception? reason)
        => FireAndForget(DisposeAsyncCore(reason, collateral: true));

    void FailBackendTermination(PgError error)
        => FailProtocol(PgErrorException.Create(error));

    internal void ReportUnobservedCallback(Exception exception, string callback)
        => SlonLogMessages.UnobservedCallbackException(_logger, exception, callback);

    /// Shared core for the Dispose paths: a forceful Shutdown, nothing more. Completion is the
    /// protocol's single terminal stage - Shutdown's completion finally releases EVERY resource
    /// (transport, heartbeat, scope signal, close signal), so a bare <see cref="CompleteAsync"/>
    /// is fully terminal and the Dispose verbs are aliases differing only in forcefulness and
    /// fire-and-forget shape. Idempotent via <c>_disposed</c>.
    bool _disposed;
    async ValueTask DisposeAsyncCore(Exception? closeReason, bool collateral)
    {
        // Pure alias over the terminal drain: forceful shutdown, all resource release happens in
        // Shutdown's completion finally (the single terminal stage). This gate only makes the
        // dispose VERB idempotent; it owns no resources of its own.
        if (Interlocked.Exchange(ref _disposed, true))
            return;
        await Shutdown(closeReason, forceful: true, collateral).ConfigureAwait(false);
    }

    /// async void (not a discard) so the background drain's exceptions are observed and reported here.
    async void FireAndForget(ValueTask task)
    {
        try
        {
            await task.ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            SlonLogMessages.BackgroundProtocolOperationFailed(_logger, ex);
        }
    }

    // Passive completion observation, matching Pipeline.Completion: this task never initiates
    // shutdown, and CompleteAsync returns the same task after signalling it. The TCS is available
    // from construction so wrappers can attach regardless of who eventually drives completion.
    readonly TaskCompletionSource _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Completes after terminal drain and resource teardown. Observing this task does not
    /// initiate shutdown; <see cref="CompleteAsync"/> does.</summary>
    public Task Completion => _completion.Task;

    // Single-winner drain. The first Shutdown caller claims this and runs the body; concurrent and later
    // callers await Completion. Separate from the completion TCS because that task must be observable
    // before shutdown begins.
    bool _shutdownStarted;
    PgCollateralException? _collateralException;
    TaskCompletionSource? _executorStopped;
    Task Shutdown(Exception? closeReason, bool forceful, bool collateral)
    {
        bool owner;
        CloseSignal.Lease closeLease = default;
        TaskCompletionSource completion;
        lock (_syncRoot)
        {
            owner = !_shutdownStarted;
            if (owner)
            {
                // Materialize the canonical closed exception BEFORE any cancellation can fire (the forceful
                // escalation below, or the body's graceful cancel). A sync read/flow faulting on the
                // abortive close or AbortToken translates to it (PgDecoder reads _close.Reason); if it's
                // still null when the wire breaks, the raw ObjectDisposedException leaks instead. The owner
                // sets it once; losers read the same instance. Wraps closeReason as inner. CloseSignal also
                // re-materializes on every trip, so the invariant is structural, not just this ordering.
                // Publish the per-flow verdict BEFORE ClosedException becomes observable. Some flow
                // paths poll IsProtocolClosed rather than waking through the abort token; publishing
                // the verdict first prevents such a successor from briefly selecting the canonical root cause
                // instead of its collateral verdict. ClosedException is the publication flag here.
                if (collateral)
                    Volatile.Write(ref _collateralException,
                        PgCollateralException.ForProtocolFailure(closeReason));
                _close.MaterializeReason(closeReason);
                _executorStopped = new(TaskCreationOptions.RunContinuationsAsynchronously);
                _source.SetDrainSignal(_executorStopped);
                _shutdownStarted = true;
                if (_status is not ProtocolStatus.Completed)
                    SetStatus(ProtocolStatus.Draining);
                _drainingCount++;
            }
            completion = _completion;

            if (forceful)
                TerminateCancellationLocked();

            // The drain owner retains the close signal through RunShutdownAsync. A forceful loser
            // leases it only for the synchronous escalation below; terminal release waits for leases.
            if (forceful && !owner)
                closeLease = _close.TryAcquire();
        }

        // Forceful escalation: idempotent, applied by ANY forceful caller - including one that loses the
        // drain claim to a concurrent graceful CompleteAsync - so a forceful Dispose can break a graceful
        // drain that would otherwise hang on a wedged peer. AbortToken + the abortive close (RST, never
        // blocks) fault parked sync I/O into the translation path; async I/O already unblocks off
        // AbortToken. Runs AFTER the claim so _closedException is set: a loser's lock acquisition
        // happens-after the owner's materialize. _abortCts is live: DisposeAsyncCore (the only forceful
        // caller, gated by _disposed so it runs once) fires this before its await and disposes the CTSes
        // only afterwards.
        if (forceful && (owner || closeLease.IsAcquired))
        {
            using (closeLease)
            {
                _close.Abort();
                _connection?.Abort();
                // A forceful close must release non-I/O flows immediately. In particular, an acquired
                // exclusive flow may be parked solely on its inner pipeline and therefore has no socket
                // operation for the transport abort to wake. Periodic propagation repeats this pass for
                // a flow crossing an enumeration visibility window.
                PropagateFlowTermination();
            }
        }

        if (owner)
            _ = DriveShutdownAsync(forceful, completion);
        return completion.Task;
    }

    // Owns the single drain body and publishes its outcome to every awaiting caller. Run outside
    // _syncRoot (the gate only claims under the lock) so the body's awaits / cancellation callbacks
    // never execute while the lock is held.
    async ValueTask DriveShutdownAsync(bool forceful, TaskCompletionSource completion)
    {
        try
        {
            await RunShutdownAsync(forceful).ConfigureAwait(false);
            completion.SetResult();
        }
        catch (Exception ex)
        {
            completion.SetException(ex);
        }
    }

    async Task RunShutdownAsync(bool forceful)
    {
        // Set by the Shutdown gate under _syncRoot before any cancellation fired.
        var closedException = _close.Reason!;
        ITimer? abortTimer = null;

        // Graceful: bound the drain with CompletionTimeout and fire StoppingToken so the body drains
        // to a clean RFQ. Timeout expiry must perform the same physical escalation as a forceful caller:
        // AbortToken alone cannot break synchronous socket I/O. Route the timer back through Shutdown so
        // cancellation delivery finishes before the transport is aborted, exactly as for an external
        // forceful caller. Parked-flow propagation is heartbeat-driven either way
        // (ExecutionControl.OnHeartbeat fails the activation source within HeartbeatInterval; forceful
        // disposal accepts that latency too).
        if (!forceful)
        {
            abortTimer = _options.TimeProvider.CreateTimer(
                static state => _ = ((PgClientProtocol)state!).Shutdown(
                    closeReason: null, forceful: true, collateral: false),
                this, _options.CompletionTimeout, Timeout.InfiniteTimeSpan);
            await _close.StopAsync().ConfigureAwait(false);
        }

        // The shutdown winner armed this before firing either close token. It resolves when the
        // executor's source pull returns completed, allowing inert items to migrate without waiting
        // for already-dispatched flows to drain.
        var executorStopped = _executorStopped!;
        var cleanShutdown = false;

        // AsTask once: consumed by both the source-drain gate and the final await.
        var completeTask = _pipeline.CompleteAsync(closedException).AsTask();
        try
        {
            await Task.WhenAny(executorStopped.Task, completeTask).ConfigureAwait(false);
            Exception flowTermination = Volatile.Read(ref _collateralException) is { } collateral
                ? collateral
                : closedException;
            _source.DrainInertItems(flow => DisposeInertFlow(flow, flowTermination));
            await completeTask.ConfigureAwait(false);
            cleanShutdown = !FlowControl.AbortToken.IsCancellationRequested;
        }
        catch (PgClientClosedException)
        {
            // Expected forced-close outcome, not a fault to surface. When the wire is torn down mid-drain
            // (a forceful Abort, or a forceful sibling racing this graceful drain), the executor's pre-park
            // flush faults with the closed exception. ExecuteSource's sanctioned-shutdown catch swallows
            // only a token-matched OCE, but the writer translates the abort to PgClientClosedException, so
            // it escapes into completeTask. The pipeline still ran its own teardown (DrainOnCompletionAsync
            // + enumerator dispose) in its finally, so the residual is drained - only the exception bubbles
            // here. Swallow it so CompleteAsync/DisposeAsync complete normally. Catch the type, not a single
            // instance: a concurrent graceful+forceful pair each materialize their own closed exception and
            // either may win _closedException, so both are equally expected here.
        }
        finally
        {
            // Disarm and join the graceful escalation callback before releasing its close-signal lease.
            if (abortTimer is not null)
                await abortTimer.DisposeAsync().ConfigureAwait(false);
            try
            {
                // Release the transport once the drain has completed. Single-winner gating runs this body
                // exactly once, so the wire is closed exactly once at completion - NOT gated on Dispose. The
                // transport is not observable protocol state: unlike the cancellation sources (whose tokens a
                // caller may still read after a graceful CompleteAsync), a completed protocol's wire can't be
                // reached by anyone. Without this, a CompleteAsync never followed by Dispose leaked its socket
                // (max_connections).
                if (_connection is not null)
                {
                    // Release the transport through the endpoints the protocol holds - the connection owns
                    // no teardown. A clean drain first flushes its best-effort Terminate; error-completing
                    // the writer then discards anything left by a failed Terminate (forceful shutdown already
                    // Abort'd the socket). Complete the reader through the enumerator that owns it. The
                    // completed pipeline has already joined every read owner and retired its borrowed buffer
                    // before reader completion can return pooled segments. Both endpoints dispose the shared
                    // stream idempotently.
                    if (cleanShutdown)
                    {
                        try
                        {
                            // The completed pipeline has retired every writer owner. Terminate is a
                            // best-effort PostgreSQL courtesy at this sole-owner boundary; failure to
                            // send it cannot make an otherwise-complete local shutdown fail.
                            _protocolDataWriter.WriteTerminate();
                            await _protocolDataWriter.FlushAsync(default).ConfigureAwait(false);
                        }
                        catch (Exception ex)
                        {
                            SlonLogMessages.TerminateWriteFailed(_logger, ex);
                        }
                    }
                    await _connection.Writer.CompleteAsync(closedException).ConfigureAwait(false);
                    await ((IAsyncDisposable)_pgDecoder).DisposeAsync().ConfigureAwait(false);
                }
            }
            finally
            {
                try
                {
                    _heartbeat?.Dispose();
                    _cancellation?.Dispose();
                    _exclusiveScope?.Dispose();
                    await _close.DisposeAsync().ConfigureAwait(false);
                }
                finally
                {
                    // Pool eviction may replace this backend as soon as Completed becomes visible.
                    SignalCompleted();
                }
            }
        }
    }

    void DisposeInertFlow(PgClientFlow flow, Exception flowTermination)
    {
        var control = flow.GetExecutionControl(FlowControl);
        if (flow.AllowsMigration &&
            (!control.IsDecoderSettled || control.AbortToken.IsCancellationRequested) &&
            _flowMigration is { } migrate)
        {
            var migration = new FlowMigration(flow, control, _options.TimeProvider);
            control.DetachForMigration(_source);
            try
            {
                if (migrate(migration))
                    return;
            }
            catch (Exception ex)
            {
                migration.Fail(ex);
                return;
            }
        }
        control.FailUnstarted(flowTermination);
    }

    internal ValueTask Heartbeat(TimeSpan period)
    {
        OnCancellationHeartbeat(period);
        PropagateFlowHeartbeat(period);
        return new();
    }

    void PropagateFlowHeartbeat(TimeSpan period)
    {
        var control = FlowControl;
        try
        {
            _source.OnActivationHeartbeat(period);
        }
        catch (Exception ex)
        {
            SlonLogMessages.UnobservedCallbackException(
                _logger, ex, "the source heartbeat callback");
        }

        foreach (var flow in GetFlows())
        {
            try
            {
                flow.GetExecutionControl(control).OnHeartbeat(period);
            }
            catch (Exception ex)
            {
                SlonLogMessages.UnobservedCallbackException(
                    _logger, ex, "a flow heartbeat callback");
            }
        }
    }

    void PropagateFlowTermination()
    {
        var control = FlowControl;
        foreach (var flow in GetFlows())
        {
            try
            {
                flow.GetExecutionControl(control).PropagateTermination();
            }
            catch (Exception ex)
            {
                SlonLogMessages.UnobservedCallbackException(
                    _logger, ex, "a flow termination callback");
            }
        }
    }

    public struct Enumerator
    {
        Pipeline<PgClientFlow, Policy, PgClientFlowSource, PgClientFlowSource.Enumerator>.Enumerator _inner;

        internal Enumerator(PgClientProtocol protocol) => _inner = protocol._pipeline.GetEnumerator();

        public PgClientFlow Current => _inner.Current;
        public Enumerator GetEnumerator() => this;
        public bool MoveNext() => _inner.MoveNext();
    }

    readonly struct Policy : IPipelinePolicy<PgClientFlow>
    {
        readonly Control _control;
        readonly ValueTaskSourcePromise<PipelineItemResult> _promise;
        readonly ExclusiveScopeState? _localPipeline;

        // Parameterized by Control (not the protocol) so the same policy drives both the protocol's
        // outer pipeline (FlowControl) and an exclusive flow's inner pipeline (its own Control reading
        // the inner pipeline's slots). The optional local owner changes only recovery disposition:
        // resync still runs, but that private pipeline closes admission afterwards.
        public Policy(PgClientProtocol protocol, Control control, ExclusiveScopeState? localPipeline = null)
        {
            _control = control;
            _promise = new();
            _localPipeline = localPipeline;
            ActivationScheduler = protocol._activationScheduler;
        }

        PipelineScheduler ActivationScheduler { get; }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CompleteItem(PgClientFlow item, int remainingDepth, Exception? exception)
        {
            if (exception is PgClientClosedException && _control.ClosedException is not null)
                exception = _control.FlowTerminationException;
            // OnReleasing (protocol bookkeeping: ActivatedFlow release, read-state recycle) must run
            // BEFORE Release (user-visible terminal): Release fires the flow's completed observer,
            // which may Reset() and re-enqueue the SAME instance. If that next tenure's Activate lands
            // before OnReleasing's depth-0 CAS, the comparand matches the new activation (ABA) and
            // severs a live binding. Ordering OnReleasing first closes this by causality. Recovery
            // items take the hardened path (capture + try/finally) out-of-line to keep this inlineable.
            if (item is ResyncRecoveryFlow { FailedFlow: { } failedFlow } recovery)
            {
                CompleteRecoveryItem(recovery, failedFlow, remainingDepth, exception);
                return;
            }

            _control.OnReleasing(item, remainingDepth);
            if (exception is PgProtocolException)
                exception = new PgClientException(exception);
            item.GetExecutionControl(_control).Release(exception);
            // No recovery in play here (recovered flows take the branch above), so the wire state is final:
            // an outer flow that left a transaction open is unscoped poison. Inner-scope / failed flows are
            // exempt (handled in GuardWireIdleOnHandoff).
            _control.GuardWireIdleOnHandoff(exception);
            _control.OnReleased(remainingDepth);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        void CompleteRecoveryItem(ResyncRecoveryFlow resyncRecovery, PgClientFlow failedFlow, int remainingDepth, Exception? exception)
        {
            // Capture the binding BEFORE Release fires the resyncRecovery's completed observer:
            // completion is the reuse gate, and a Reset on reuse clears the binding (same
            // causality as the OnReleasing-before-Release ordering below).
            var failureException = resyncRecovery.FailureException!;

            _control.OnReleasing(resyncRecovery, remainingDepth);
            try
            {
                resyncRecovery.GetExecutionControl(_control).Release(exception);
            }
            finally
            {
                if (exception is not null)
                    _control.FailProtocol(exception);

                // Releasing a resyncRecovery ends its supplanted flow's extended lifetime: the wire is
                // resynced (or dead) and nothing references the failed tenure. The supplanted flow
                // completes on EVERY exit (including the resyncRecovery's own fault), or its caller strands.
                // A resyncRecovery that also died attaches its fault behind the original failure as inner -
                // but ONLY when both are independent bugs. THE canonical shutdown close (Close.Reason) on
                // EITHER side is the one shutdown, not a distinct fault: the failed flow may already carry it
                // (we started shut down), and/or recovery's own resync drain may have been torn by a
                // graceful->abort escalation and died with it (we got another one). Surfacing an
                // AggregateException of that one redundant close only confuses the consumer, so fold it.
                // Keyed by IDENTITY, not type: only the canonical Close.Reason instance folds - any OTHER
                // PgClientClosedException (e.g. the never-started dispatch fallback) is a genuine independent
                // fault and still aggregates. shutdownClose is null outside a shutdown, so a normal mid-op
                // recovery always aggregates two real faults.
                // Single-level by construction: TryRecoverItemFailure refuses ResyncRecoveryFlow items.
                var shutdownClose = _control.ClosedException;
                Exception combined = exception is null
                    || (shutdownClose is not null && (ReferenceEquals(exception, shutdownClose) || ReferenceEquals(failureException, shutdownClose)))
                    ? failureException
                    : new PgClientException(new AggregateException(
                        failureException is PgClientException { InnerException: { } failureCause } ? failureCause : failureException,
                        exception is PgClientException { InnerException: { } recoveryCause } ? recoveryCause : exception));
                if (combined is PgProtocolException)
                    combined = new PgClientException(combined);

                // Recovery retirement made the wire schedulable. Publish that state before completing
                // the supplanted flow: its completion continuation may immediately submit new work.
                if (resyncRecovery.BlocksAdmission)
                    _control.RecoveryCompleted();

                failedFlow.GetExecutionControl(_control).Release(combined);
                _control.OnReleased(remainingDepth);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<PipelineItemResult> ExecuteItemAsync(PgClientFlow item, bool pipelineTaskRecovery, CancellationToken cancellationToken)
        {
            var itemControl = item.GetExecutionControl(_control);
            try
            {
                // The source owns an inert, portable flow until this dispatch. Bind only after dequeue:
                // shutdown may move anything still in the source to another wire, where connection-local
                // command state must be recomputed against that wire's context. Bind publishes execution
                // ownership only after that fallible preparation succeeds.
                itemControl.Bind(_control.FlowBindingContext);
            }
            catch (Exception ex)
            {
                // Binding has not touched the wire. Deliver the local failure to the caller, and leave
                // the item unstarted so recovery does not inject protocol work for it.
                itemControl.FailBeforeStart(ex);
                throw;
            }
            // The pooled execute promise is SINGLE-PUMPED: the executor pump serializes its dispatches,
            // so one ExecuteCore releases the promise before the next Starts, and reusing one instance is
            // safe. Pipeline-task recovery can run
            // alongside an in-flight executor dispatch; routing it through the pooled promise would let
            // the two TryStart the one promise at once -> "already executing". The framework tells us
            // which side issued this, so we read it off pipelineTaskRecovery, not off item type: most recovery
            // dispatches run INLINE on the executor thread (serialized) and DO use the pooled promise;
            // only pipeline-task recovery overlaps. It takes the stock async builder so it
            // never touches the shared promise - the two sides become independent by construction. Free:
            // that path's ExecuteAuto (recovery's) completes synchronously, so the stock builder never
            // suspends and never boxes a state machine.
            if (pipelineTaskRecovery)
                return ExecutePipelineTaskRecovery(_control, item, cancellationToken);

            PromiseAsyncValueTaskMethodBuilder<PipelineItemResult>.Promise = _promise;
            try
            {
                return ExecuteCore(_control, item, cancellationToken);
            }
            finally
            {
                PromiseAsyncValueTaskMethodBuilder<PipelineItemResult>.Promise = null;
            }

            [AsyncMethodBuilder(typeof(PromiseAsyncValueTaskMethodBuilder<>))]
            static async ValueTask<PipelineItemResult> ExecuteCore(
                Control control, PgClientFlow item, CancellationToken cancellationToken)
            {
                await control.WaitForCancellationAttempt().ConfigureAwait(false);

                // A flow may defer this flush only when its first phase cannot wait for decoder input.
                if (!item.SupportsDeferredFlush && control.UnflushedBytes != 0)
                    await control.FlushAsync(cancellationToken).ConfigureAwait(false);

                var tasks = await control.Execute(item).ConfigureAwait(false);
                return new PipelineItemResult(tasks.TrailingExecutionTask, tasks.PipelineTask);
            }

            // Stock builder (no shared promise) for pipeline-task recovery. Body identical to ExecuteCore.
            static async ValueTask<PipelineItemResult> ExecutePipelineTaskRecovery(
                Control control, PgClientFlow item, CancellationToken cancellationToken)
            {
                await control.WaitForCancellationAttempt().ConfigureAwait(false);
                var tasks = await control.Execute(item).ConfigureAwait(false);
                return new PipelineItemResult(tasks.TrailingExecutionTask, tasks.PipelineTask);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ActivateHeadItem(PgClientFlow item, bool preferAsync = true)
        {
            // Bind the decoder synchronously now: the pipeline has just published item to the
            // ActivatedItem slot on this thread, so the bind reads the slot when it agrees with item.
            // Only the body wake is deferred below.
            _control.BindDecoder(item);

            // Inline-activate when the framework allows it (preferAsync=false) or the flow is sync:
            // sync flows park on a kernel wait-handle signal, bounded cost, safe under the advancer
            // latch. Async flows can attach arbitrary await continuations, so they go through TP.
            if (preferAsync && item.IsAsyncAtDispatch)
            {
                // The flow itself is the work item: an immutable (flow, control) pairing per queued
                // activation, zero-alloc. A single cached mutable work item was a lost-update box -
                // two activations in flight let the second Initialize overwrite the first, so both
                // ran the later flow and the earlier never activated. One pending activation per flow
                // tenure makes the per-flow field safe.
                item.PrepareActivationDispatch(_control);
                // SubmitDetached must not throw (the PipeScheduler.Schedule-style dispatch contract); a
                // caller handing us a fallible scheduler owns the resulting connection breakage. No guard.
                ActivationScheduler.SubmitDetached(ActivationWorkItemAction, item, preferLocal: true);
            }
            else
                _control.Activate(item);
        }

        static readonly Action<object?> ActivationWorkItemAction = static state => ((IThreadPoolWorkItem)state!).Execute();

        public bool TryRecoverItemFailure(in PipelineItemFailureContext context, PgClientFlow failedItem, CancellationToken cancellationToken, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out PgClientFlow? recoveryItem)
        {
            // Recovery-on-recovery does not exist (the framework guarantees it: a committed
            // recovery's late fault travels as a marker exception and completes directly,
            // never consulted here).
            Debug.Assert(failedItem is not ResyncRecoveryFlow,
                "Recovery item routed back into TryRecoverItemFailure - recovery-on-recovery must not exist.");

            // Connection-local binding occurs at dispatch but before Start. A bind failure has not
            // touched the wire and therefore needs caller fault delivery, not protocol recovery.
            if (!failedItem.IsStarted)
            {
                recoveryItem = null;
                return false;
            }

            // Startup has not established a reusable query-protocol wire. Authentication may still own
            // the server state machine, where injecting Sync is neither a valid nor useful recovery.
            if (!_control.QueryProtocolEstablished)
            {
                recoveryItem = null;
                return false;
            }

            // Recovery assumes the decoder still owns a trustworthy message boundary. A framing
            // failure says precisely that it may not: reading toward an RFQ can reinterpret body
            // bytes and condemn successors with misleading secondary failures. Publish the cause and
            // abort the connection instead; the failed flow keeps its specific exception while queued
            // successors receive the collateral flow-termination verdict.
            if (context.Exception is PgFramingException
                || (context.Exception is not PgProtocolException and not PgClientException
                    && _control.IsConnectionLost(context.Exception)))
            {
                _control.FailProtocol(context.Exception);
                recoveryItem = null;
                return false;
            }

            // Pipeline is ABORTING: skip recovery and let the framework propagate the failure. Gate on the
            // ABORT token specifically, NOT ClosedException - which a GRACEFUL close also sets. A forceful
            // abort is teardown over a torn wire: recovering it drives a resync drain over a dead/RST'd
            // socket, racing the close-torn buffer into a negative-bufferedBytes assertion. A GRACEFUL close
            // is NOT teardown - the wire stays live (the close waits for the drain), so recovery MUST run: it
            // resyncs the failed flow's leftover to RFQ, keeping the wire clean for the next pipelined flow,
            // which must read ITS OWN bytes rather than the leftover (the pipelined-shutdown desync). The
            // abort token fires together with the abortive close reason (_close.Abort), so it has no lag
            // here. The graceful StoppingToken/close does NOT fire it.
            if (_control.AbortToken.IsCancellationRequested)
            {
                recoveryItem = null;
                return false;
            }

            var failedItemControl = failedItem.GetExecutionControl(_control);

            // Substitute-write gate. Both must hold for recovery to inject a terminating Sync:
            //   - The failure kind hasn't closed the failed flow's write window (PipelineTask
            //     is the closed-window case, identity already released from the writer).
            //   - The wire isn't already RFQ-terminated. If the last write was Query/Sync the server
            //     emits the inherited RFQs and recovery is pure read-drain; if it ended mid extended-
            //     query, recovery's Sync brings the wire back to a defined state.
            // canWrite: the failure didn't close the write window (PipelineTask = closed-window,
            // identity already released from the writer). Recovery writes a ROLLBACK whenever it can,
            // to close any transaction the failed flow left open (including an exclusive scope's, on
            // abort-to-root). canWriteSync additionally injects a Sync to realign the wire when the last
            // write was mid extended-query (no RFQ induced); a Query/Sync last message realigns itself.
            var canWrite = context.Kind is not PipelineItemFailureKind.PipelineTask;
            var canWriteSync = canWrite && !failedItemControl.LastMessageInducesRfq;

            // The outstanding phase task to sequence against, by failure kind:
            //   - PipelineTask: the failed flow's in-flight WRITE (trailing). Recovery's TrailingPhase
            //     awaits it before WriteSync so it doesn't collide on the single-producer writer.
            //   - TrailingExecutionTask: the failed flow's in-flight READ. It continues on its own
            //     control via the decoder permit; recovery's DrainPhase awaits it so it never resolves
            //     the read-turn out from under it. Without forwarding, the robbed read decodes the
            //     wrong message and its late fault re-enters nonexistent recovery-of-recovery.
            var outstandingIsRead = context.Kind is PipelineItemFailureKind.TrailingExecutionTask;
            var outstandingPhase =
                outstandingIsRead || context.Kind is PipelineItemFailureKind.ExecutionPipelineTask
                    ? context.OutstandingPhaseTask
                    : default;

            // The framework will NOT complete a supplanted item - that's this policy's job
            // (CompleteItem fires when the recovery completes). The failed item's lifetime extends as
            // far as the recovery, so its dispatch state, RFQ bookkeeping, and registrations release
            // before the instance can be reused.
            var recovery = ResyncRecoveryFlow.Create(
                _control, failedItem, context.Exception, outstandingPhase, outstandingIsRead,
                failedItemControl.RfqCount, canWriteSync, canWrite);
            recoveryItem = recovery;
            if (recovery.BlocksAdmission)
                _control.RecoveryStarted();
            _control.OnFlowSubstituted(failedItem, recoveryItem);
            _localPipeline?.Terminate(context.Exception);
            return true;
        }

    }

    // Erases the pipeline's policy and source types so controls at any nesting depth can read its
    // execution slots through one closed internal shape.
    abstract class FlowSlots
    {
        FlowSlots() { }

        public abstract PgClientFlow? Executing { get; }
        public abstract PgClientFlow? Activated { get; }

        public static FlowSlots Create<TPolicy, TSource, TEnumerator>(
            Pipeline<PgClientFlow, TPolicy, TSource, TEnumerator> pipeline)
            where TPolicy : IPipelinePolicy<PgClientFlow>
            where TSource : IPipelineSource<PgClientFlow, TEnumerator>
            where TEnumerator : struct, IPipelineEnumerator<PgClientFlow>
            => new PipelineSlots<TPolicy, TSource, TEnumerator>(pipeline);

        sealed class PipelineSlots<TPolicy, TSource, TEnumerator>(
            Pipeline<PgClientFlow, TPolicy, TSource, TEnumerator> pipeline) : FlowSlots
            where TPolicy : IPipelinePolicy<PgClientFlow>
            where TSource : IPipelineSource<PgClientFlow, TEnumerator>
            where TEnumerator : struct, IPipelineEnumerator<PgClientFlow>
        {
            public override PgClientFlow? Executing => pipeline.ExecutingItem;
            public override PgClientFlow? Activated => pipeline.ActivatedItem;
        }
    }

    internal sealed class Control(PgClientProtocol protocol, bool poolFacing) : IProtocolStatic<CommandFlow.ReadState>
    {
        // The pipeline whose slots this Control reads, bound right after that pipeline is created. The
        // outer (pool-facing) Control reads the protocol's own pipeline; an exclusive flow's inner
        // Control reads its inner pipeline - both through the same IPipelineSlots handle, so any
        // nesting depth composes. ExecutingFlow / ActivatedFlow are the single source of truth (the
        // single-pump invariant + in-order Activate-before-Complete): ExecutingFlow is the write-phase
        // identity (ThrowIfCannotWrite); ActivatedFlow is the read pipe's current-reader handle
        // (PgDecoder routes messages to it).
        FlowSlots _slots = null!;
        public void BindPipeline<TPolicy, TSource, TEnumerator>(
            Pipeline<PgClientFlow, TPolicy, TSource, TEnumerator> pipeline)
            where TPolicy : IPipelinePolicy<PgClientFlow>
            where TSource : IPipelineSource<PgClientFlow, TEnumerator>
            where TEnumerator : struct, IPipelineEnumerator<PgClientFlow>
            => _slots = FlowSlots.Create(pipeline);

        PgClientFlowSource _source;
        public void BindSource(PgClientFlowSource source) => _source = source;
        internal PgClientFlowBindingContext? FlowBindingContext => protocol._flowBindingContext;
        public bool HasQueuedFlow => _source.Backlog != 0;
        public bool IsInlineDrive => _source.IsInlineDrive;
        public long UnflushedBytes => protocol.UnflushedBytes;
        public ValueTask FlushAsync(CancellationToken cancellationToken) => protocol.FlushAsync(cancellationToken);
        PgClientFlow? _cancellationActivatedFlow;
        internal (PgClientFlow? Owner, int Window) CancellationActivation
        {
            get
            {
                var owner = Volatile.Read(ref _cancellationActivatedFlow);
                return owner is { IsCompleted: false }
                    ? (owner, owner.CancellationWindow)
                    : (null, 0);
            }
        }
        internal void PublishCancellationActivation(PgClientFlow flow)
            => Volatile.Write(ref protocol.FlowControl._cancellationActivatedFlow, flow);
        void SubstituteCancellationActivation(PgClientFlow from, PgClientFlow to)
            => Interlocked.CompareExchange(
                ref protocol.FlowControl._cancellationActivatedFlow, to, from);
        void ClearCancellationActivation(PgClientFlow flow)
        {
            var outerOwner = protocol.FlowControl.ActivatedFlow;
            if (ReferenceEquals(outerOwner, flow))
                outerOwner = null;
            Interlocked.CompareExchange(
                ref protocol.FlowControl._cancellationActivatedFlow, outerOwner, flow);
        }
        public ValueTask WaitForCancellationAttempt()
            => protocol.WaitForCancellationAttempt();
        public void RequestServerCancellation(PgClientFlow instigator, int window,
            BackendCancellationTiming timing, TaskCompletionSource? delivery,
            object episodeKey, int scope, BackendCancellationTiming subsequentTiming)
            => protocol.RequestServerCancellation(instigator, window, timing, delivery,
                episodeKey, scope, subsequentTiming);
        public bool OnBackendCancellationObserved(PgClientFlow instigator, int window)
            => protocol.OnBackendCancellationObserved(instigator, window);
        public bool IsAtCancellationReadFrontier(PgClientFlow flow, int window)
            => protocol.FlowControl.Decoder.IsAtCancellationReadFrontier(flow, window);
        internal int ClearCancellationReadFrontier(PgClientFlow flow)
            => protocol.FlowControl.Decoder.ClearCancellationReadFrontier(flow);
        public void EnterCancellationReadFrontier(PgClientFlow flow, int window)
        {
            protocol.FlowControl.Decoder.SetCancellationReadFrontier(flow, window);
            if (protocol.HasCancellationIntents)
                protocol.OnCancellationReadFrontier();
        }
        public void LeaveCancellationReadFrontier(PgClientFlow flow)
            => protocol.LeaveCancellationReadFrontier(flow);
        public bool HasPriorCancellationExposure(PgClientFlow flow, int window)
            => protocol.HasPriorCancellationExposure(flow, window);
        public void OnFlowSubstituted(PgClientFlow from, PgClientFlow to)
        {
            SubstituteCancellationActivation(from, to);
            protocol.OnFlowSubstituted(from, to);
        }
        internal string? SessionResetCommand => protocol.SessionResetCommand;

        // The scope's linked close signal, set once for an exclusive-scope inner Control; null for the
        // pool-facing FlowControl (which reads the protocol's _close directly). Inner flows read the
        // scope signal's tokens so a protocol stop/abort cascades through the link, while a scope-only
        // trip stays off the protocol token.
        CloseSignal? _scopeClose;
        public void BindScopeClose(CloseSignal scopeClose) => _scopeClose = scopeClose;

        // Per-Control decoder/writer shells over the protocol's shared read/write pipes. The inner
        // (exclusive-scope) Control binds scope shells carrying the scope token; the outer Control
        // leaves these null and resolves to the protocol's base shells (themselves bound to this
        // Control). The single-pump invariant keeps only one shell per direction active at a time, so
        // both share the one physical pipe safely.
        PgDecoder? _decoder;
        ProtocolDataWriter? _writer;
        public void BindShells(PgDecoder decoder, ProtocolDataWriter writer)
        {
            _decoder = decoder;
            _writer = writer;
        }

        PgDecoder Decoder => _decoder ?? protocol._pgDecoder;

        public PgClientFlow? ExecutingFlow => _slots.Executing;
        public PgClientFlow? ActivatedFlow => _slots.Activated;

        public ProtocolDataWriter Writer => _writer ?? protocol._protocolDataWriter;

        // Backend identity from BackendKeyData (pulled from StartupFlow after startup completes).
        // Process id is the diagnostic-safe value (logs, "which backend"); secret key is restricted
        // to the CancelRequest payload. 0 = not yet received.
        public int BackendProcessId => protocol._backendProcessId;
        public int BackendSecretKey => protocol._backendSecretKey;

        // The wire's last-seen transaction status. Connection-wide (single field on the protocol); the
        // inner-scope Control reads the same one. Idle / Transaction / Error, or Unknown pre-first-RFQ.
        public TransactionStatus TransactionStatus => protocol._transactionStatus;
        public bool QueryProtocolEstablished => Volatile.Read(ref protocol._queryProtocolEstablished) is not 0;

        public bool IsConnectionLost(Exception exception) => protocol._connection.IsConnectionLost(exception);

        public void FailBackendTermination(PgError error) => protocol.FailBackendTermination(error);
        public IImmutableDictionary<string, string> StartupParameters
            => protocol._serverParameterState.BaseSnapshot;
        public IImmutableDictionary<string, string> SessionParameters
            => protocol._serverParameterState.CurrentSnapshot;
        public int SessionParametersRevision => protocol._serverParameterState.Revision;
        public PgBackendInfo BackendInfo
            => protocol._backendInfo ?? throw new InvalidOperationException(
                "Backend identity is unavailable because startup has not completed or no backend provider was configured.");
        public PgBackendCapabilities BackendCapabilities => protocol._backendCapabilities;
        public Encoding ClientEncoding => protocol._protocolDataWriter.ClientEncoding;

        // Tokens come from the scope signal for an inner Control (so the scope cascade reaches inner
        // flows), else the protocol's _close. Both are stable across a flow's tenure. Surfaced through
        // Control so ExecutionControl and the body read them without per-flow storage.
        CloseSignal Close => _scopeClose ?? protocol._close;
        public CancellationToken AbortToken => Close.AbortToken;
        public CancellationToken StoppingToken => Close.StoppingToken;

        /// The canonical PgClientClosedException once Shutdown has entered, null otherwise. Single
        /// instance per lifetime, materialized before any cancellation fires so an observer waking on
        /// AbortToken/StoppingToken sees a non-null value. For an inner Control a scope-only trip resolves
        /// the scope reason; a protocol trip chains through the link to the protocol reason.
        public PgClientClosedException? ClosedException => Close.Reason;
        public Exception FlowTerminationException
            => Volatile.Read(ref protocol._collateralException) is { } collateral
                ? collateral
                : ClosedException!;

        /// Throws PgClientClosedException if closed, no-op otherwise. For the OCE catch path inside
        /// existing async I/O frames, converting our abort-token OCE to the typed exception without an
        /// extra wrapping frame.
        public void ThrowIfClosed()
        {
            if (Close.Reason is { } ex)
                throw ex;
        }

        public void OnParameterStatus(BackendMessage message)
        {
            message.DebugEnsureExpected(PgTypes.BackendType.ParameterStatus);
            message.DebugEnsureBuffered();

            var reader = message.BodyReader;
            if (!reader.TryReadTo(out ReadOnlySequence<byte> nameBytes, (byte)0,
                    advancePastDelimiter: true)
                || !reader.TryReadTo(out ReadOnlySequence<byte> valueBytes, (byte)0,
                    advancePastDelimiter: true))
                throw PgProtocolException.NotEnoughData("ParameterStatus");
            if (reader.Remaining is not 0)
                throw new PgProtocolException("ParameterStatus contains trailing data.");

            var name = Encoding.UTF8.GetString(nameBytes);
            var value = protocol._protocolDataWriter.ClientEncoding.GetString(valueBytes);
            protocol._serverParameterState.Set(name, value);

            switch (name)
            {
            case "client_encoding":
                // If Postgres supported ASCII incompatible encodings there would be a catch-22
                // reporting the new encoding value encoded in the new encoding.
                // As it doesn't support e.g. utf16 we can always rely on the ascii bytes,
                // which is enough to transmit encoding names.
                {
                    var newEncoding = value;
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
                        protocol._protocolDataWriter.ClientEncoding = ResolveClientEncoding(
                            newEncoding, protocol._options.DefaultClientEncoding);
                    }
                    catch (ArgumentException ex)
                    {
                        protocol.FailProtocol(ex);
                        throw;
                    }
                }
                break;
            case "search_path":
                protocol.CurrentSearchPath = value;
                break;
            default:
                SlonLogMessages.IgnoredParameterStatus(protocol._logger, name);
                break;
            }
        }

        internal static Encoding ResolveClientEncoding(string encodingName, Encoding defaultEncoding)
            => encodingName == "SQL_ASCII" ? defaultEncoding : Encoding.GetEncoding(encodingName);

        // Connection-wide transaction-state bookkeeping. Routes to the single protocol field (NOT a
        // per-Control copy) so inner-scope and outer flows keep one consistent view of the one wire.
        public void OnFlowRfq(PgClientFlow flow, BackendMessage message,
            int completedWindow, int remainingWindowCount)
        {
            protocol._transactionStatus = ReadyForQueryMessage.Create(message).TransactionStatus;
            protocol.OnCancellationWindowCompleted(flow, completedWindow, remainingWindowCount);
        }

        // Wire-handoff guard, called from Policy.CompleteItem when a flow retires. The OUTER multiplexed
        // pipeline (poolFacing) hands the wire between INDEPENDENT flows, so a flow must leave it Idle -
        // a left-open transaction would run the next interleaved flow inside it (corruption). The inner-
        // scope Control holds a transaction across its OWN subflows and is exempt (poolFacing=false). And
        // we only guard a CLEAN completion: a failed flow is recovery's domain (resync -> status-gated
        // ROLLBACK -> Idle), and a recovered flow takes the ResyncRecoveryFlow branch anyway, so by the
        // time the normal branch runs there is no recovery in play. (An autocommit error rolls back to
        // Idle on its own, so this trips only on a genuinely unscoped transaction left open by a success.)
        public void GuardWireIdleOnHandoff(Exception? completionException)
        {
            // A cleanly-completed outer flow must leave the wire at Idle; anything else means it left a
            // transaction open. StartupFlow's terminating RFQ doesn't route through OnFlowRfq (it never
            // arms _rfqCount - see CopyStartupBuffer), so the wire status is seeded to Idle before that
            // flow is queued (StartAsync); every other flow's own RFQ is read by ExecutePipelined before
            // its CompleteItem, so the status here is always this flow's own final state.
            if (poolFacing && completionException is null && protocol._transactionStatus is not TransactionStatus.Idle)
            {
                var exception = new InvalidOperationException(
                    $"A multiplexed flow completed leaving the connection in transaction status '{protocol._transactionStatus}'. " +
                    "Transactions must run inside an exclusive scope; failing the connection to avoid corrupting subsequent flows.");
                ReportProtocolInvariantViolation(exception);
                protocol.FailProtocol(exception);
            }
        }

        internal ValueTask<FlowTasks> Execute(PgClientFlow flow)
        {
            return flow.GetExecutionControl(this).ExecuteAuto();
        }

        // Bind the shared decoder to the flow being activated. Runs synchronously inside the policy's
        // ActivateHeadItem, where the pipeline has just published this flow to the ActivatedItem slot
        // on the same thread, so Initialize reads the slot when it provably agrees with the flow.
        // Deferring the bind into the TP wake let a dispatch outlive the flow's retirement and bind
        // against a depth-0-cleared slot.
        internal void BindDecoder(PgClientFlow flow)
        {
            PublishCancellationActivation(flow);
            protocol._loadObserver?.OnFlowActivated();
            Decoder.Initialize(this);
            protocol.OnFlowActivated(flow);
        }

        // Wake the flow's body with the bound decoder. Resumes the body inline, so async flows run this
        // off the executor via the TP dispatch. Safe to lag the flow's retirement: TrySetResult no-ops
        // on a flow the abort already faulted.
        internal void Activate(PgClientFlow flow)
            => flow.GetExecutionControl(this).Activate(Decoder);

        // Self-evict route for the flow layer's release-callback seam (see ExecutionControl.Release).
        internal void FailProtocol(Exception? reason) => protocol.FailProtocol(reason);
        internal void FailProtocolFromCallback(Exception exception, string callback)
        {
            protocol.ReportUnobservedCallback(exception, callback);
            protocol.FailProtocol(exception);
        }
        internal void ReportProtocolInvariantViolation(Exception exception)
            => SlonLogMessages.ProtocolInvariantViolation(protocol._logger, exception);

        internal void RecoveryStarted() => protocol.SignalDraining();
        internal void RecoveryCompleted() => protocol.SignalReady();
        internal void AssignCancellationBoundary(PgClientFlow flow, int window)
            => protocol.AssignCancellationBoundary(flow, window);
        internal void ReleaseAdmissionBarrier() => protocol.ReleaseAdmissionBarrier();
        internal void ReleaseWireCapacity() => protocol.ReleaseWireCapacity();

        internal void OnReleasing(PgClientFlow flow, int remainingDepth)
        {
            protocol._serverParameterState.CommitFlow();
            ClearCancellationActivation(flow);
            protocol.OnFlowReleased(flow, poolFacing && remainingDepth is 0);
            // Inner exclusive-scope subflows are not pool load units; only the outer pipeline reports
            // admission-to-retirement lifetimes to its host.
            if (poolFacing)
                protocol._loadObserver?.OnFlowReleased(
                    flow.GetExecutionControl(this).StallsPipeline);

            // At pipeline park release the rooted read-state buffers. Pool idle is published only
            // after Release fires the reusable flow's terminal observer; depth zero here is not yet
            // a pool-placement boundary.
            if (remainingDepth is 0)
                _commandFlowReadState = new();
        }

        internal void OnReleased(int remainingDepth)
        {
            if (!poolFacing || remainingDepth is not 0 || !protocol.IsSchedulable)
                return;

            if (protocol.Outstanding is not 0)
                return;

            protocol.NotifyAdmissionAvailable();
        }

        CommandFlow.ReadState _commandFlowReadState = new();
        ref readonly CommandFlow.ReadState IProtocolStatic<CommandFlow.ReadState>.Value
            => ref _commandFlowReadState;
    }
}
