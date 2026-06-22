using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using Draghi.Pipelining;
using Slon.Pools;

namespace Slon.Benchmark;

public readonly struct Sequential;
public readonly struct Pipelined;
public readonly struct PipelinedUserCompleted;
public readonly struct PooledUserCompleted;

enum ProtocolStatus { Created, Ready, Draining, Completed }

readonly struct FlowTasks(ValueTask trailingExecutionTask, ValueTask pipelineTask)
{
    public ValueTask TrailingExecutionTask { get; } = trailingExecutionTask;
    public ValueTask PipelineTask { get; } = pipelineTask;
    public FlowTasks(ValueTask pipelineTask) : this(default, pipelineTask) { }
    public static implicit operator FlowTasks(ValueTask pipelineTask) => new(pipelineTask);
}

// Lifecycle contract for benchmark flows. Explicit-impl on EmptyFlow forces any direct
// caller (the benchmark itself) to cast intentionally.
internal interface IProtocolFlow
{
    void Start();
    void Complete(Exception? exception = null);
    void Abort();
}

sealed class EmptyProtocolOptions
{
    public bool RunEnqueueAsynchronously { get; set; } = true;
    public TimeProvider TimeProvider { get; set; } = TimeProvider.System;
    public TimeSpan CompletionTimeout { get; set; } = TimeSpan.FromSeconds(10);
}

#pragma warning disable CS9113 // Parameter is unread.
readonly struct EmptyFlowContext<TMode>(EmptyFlow<TMode> flow, EmptyProtocol<TMode>.Control control) where TMode : struct;
#pragma warning restore CS9113 // Parameter is unread.

readonly struct Trigger;

#pragma warning disable CS9113 // Parameter is unread.
sealed class EmptyFlow<TMode>(int debugIndex) : IProtocolFlow, IValueTaskSource<bool>, IValueTaskSource<Trigger>, IValueTaskSource<string>, IValueTaskSource
#pragma warning restore CS9113 // Parameter is unread.
    where TMode : struct
{
    TimeSpan _startTimeout;
    TimeSpan _remainingTimeout;

    // Minimal flow lifecycle. Benchmarks measure the cost of running through this shape.
    // The previous spin-state machinery was incidental overhead.
    bool _started;
    bool _completed;
    TaskCompletionSource _completionTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

    Action? CompleteDelegate { get; set; }
    Action? ActivatedDelegate { get; set; }

    ManualResetValueTaskSourceCore<bool> _activateFlowSource;
    ValueTask<bool> ActivateFlowTask => new(this, _activateFlowSource.Version);
    internal void ActivateFlow() => _activateFlowSource.SetResult(true);

    ManualResetValueTaskSourceCore<string> _activateUserSource;
    public ValueTask<string> ActivateUserTask => new(this, _activateUserSource.Version);
    void ActivateUser() => _activateUserSource.SetResult("some reader over data");

    ManualResetValueTaskSourceCore<Trigger> _userCompletedSource;
    ValueTask<Trigger> UserCompletedTask => new(this, _userCompletedSource.Version);
    public void UserCompleted() => _userCompletedSource.SetResult(new());

    ManualResetValueTaskSourceCore<bool> _smExecutionCompletedSource;
    ConfiguredValueTaskAwaitable<bool>.ConfiguredValueTaskAwaiter _smFlowActivationAwaiter;
    ConfiguredValueTaskAwaitable<Trigger>.ConfiguredValueTaskAwaiter _smUserTriggerAwaiter;

    internal ValueTask<FlowTasks> Execute(EmptyFlowContext<TMode> context, CancellationToken cancellationToken)
    {
        if (typeof(TMode) == typeof(Pipelined) || typeof(TMode) == typeof(PipelinedUserCompleted) || typeof(TMode) == typeof(PooledUserCompleted))
        {
            _smFlowActivationAwaiter = ActivateFlowTask.ConfigureAwait(false).GetAwaiter();
            if (_smFlowActivationAwaiter.IsCompleted)
            {
                if (typeof(TMode) == typeof(Pipelined))
                {
                    _smFlowActivationAwaiter.GetResult();
                    return new(ValueTask.CompletedTask);
                }

                FlowActivated();
            }
            else
                _smFlowActivationAwaiter.UnsafeOnCompleted(ActivatedDelegate ??= FlowActivated);
            return new(result: new ValueTask(this, _smExecutionCompletedSource.Version));
        }

        return new(ValueTask.CompletedTask);

        void FlowActivated()
        {
            _smFlowActivationAwaiter.GetResult();
            ActivateUser();
            _smUserTriggerAwaiter = UserCompletedTask.ConfigureAwait(false).GetAwaiter();
            if (_smUserTriggerAwaiter.IsCompleted)
                Complete();
            else
                _smUserTriggerAwaiter.UnsafeOnCompleted(CompleteDelegate ??= Complete);

            void Complete()
            {
                _smUserTriggerAwaiter.GetResult();
                _smExecutionCompletedSource.SetResult(true);
            }
        }
    }

    public void Initialize(TimeSpan startTimeout)
    {
        _remainingTimeout = _startTimeout = startTimeout;
    }

    internal bool Heartbeat(TimeSpan interval)
    {
        // Unregister once we're started or completed.
        if (IsStarted || IsCompleted)
            return false;

        if ((_remainingTimeout -= interval) <= TimeSpan.Zero)
        {
            Console.WriteLine(_remainingTimeout);
            throw new TimeoutException("Operation timed out.");
        }

        return true;
    }

    public bool IsCompleted => _completed;
    public bool IsStarted => _started && !_completed;

    void IProtocolFlow.Start() => _started = true;

    void IProtocolFlow.Complete(Exception? exception)
    {
        if (_completed)
            return;
        _completed = true;
        if (exception is not null)
            _completionTcs.TrySetException(exception);
        else
            _completionTcs.TrySetResult();
    }

    void IProtocolFlow.Abort() { /* benchmarks don't exercise abort */ }

    public ValueTask WaitForComplete() => new(_completionTcs.Task);

    public void Reset()
    {
        _started = false;
        _completed = false;
        _completionTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _activateFlowSource.Reset();
        _remainingTimeout = _startTimeout;

        if (typeof(TMode) == typeof(PipelinedUserCompleted) || typeof(TMode) == typeof(PooledUserCompleted))
        {
            _smExecutionCompletedSource.Reset();
            _activateUserSource.Reset();
            _userCompletedSource.Reset();
        }
    }

    bool IValueTaskSource<bool>.GetResult(short token) => _activateFlowSource.GetResult(token);
    ValueTaskSourceStatus IValueTaskSource<bool>.GetStatus(short token) => _activateFlowSource.GetStatus(token);
    void IValueTaskSource<bool>.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => _activateFlowSource.OnCompleted(continuation, state, token, flags);

    ValueTaskSourceStatus IValueTaskSource.GetStatus(short token) => _smExecutionCompletedSource.GetStatus(token);
    void IValueTaskSource.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => _smExecutionCompletedSource.OnCompleted(continuation, state, token, flags);
    void IValueTaskSource.GetResult(short token) => _smExecutionCompletedSource.GetResult(token);

    Trigger IValueTaskSource<Trigger>.GetResult(short token) => _userCompletedSource.GetResult(token);
    ValueTaskSourceStatus IValueTaskSource<Trigger>.GetStatus(short token) => _userCompletedSource.GetStatus(token);
    void IValueTaskSource<Trigger>.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => _userCompletedSource.OnCompleted(continuation, state, token, flags);

    string IValueTaskSource<string>.GetResult(short token) => _activateUserSource.GetResult(token);
    ValueTaskSourceStatus IValueTaskSource<string>.GetStatus(short token) => _activateUserSource.GetStatus(token);
    void IValueTaskSource<string>.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => _activateUserSource.OnCompleted(continuation, state, token, flags);
}

sealed class EmptyProtocol<TMode> : IPoolConnection<EmptyProtocol<TMode>>
    where TMode : struct
{
    readonly EmptyProtocolOptions _options;
    readonly Action? _poolConnectionIdleSignal;
    readonly CancellationTokenSource _abortCts;
    QueuedPipeline<EmptyFlow<TMode>, Policy> _pipeline = null!;
    readonly Lock _syncRoot = new();
    ProtocolStatus _status = ProtocolStatus.Created;
    int _drainingCount;

    public EmptyProtocol(EmptyProtocolOptions? options, ConnectionPoolContext<EmptyProtocol<TMode>>? poolContext)
    {
        _options = options ?? new();
        _abortCts = new(Timeout.InfiniteTimeSpan, _options.TimeProvider);
        FlowControl = new Control(this);
        _pipeline = Pipeline.Create<EmptyFlow<TMode>, Policy>(new Policy(this));
        // No startup flow for the benchmark protocol, immediately ready.
        lock (_syncRoot)
            _status = ProtocolStatus.Ready;
        _poolConnectionIdleSignal = poolContext?.CreateConnectionIdleSignal(this);
    }

    Control FlowControl { get; }
    public int PipelineDepth => _pipeline.Depth;

    public bool TryQueue(EmptyFlow<TMode> flow, bool mustPipeline = false)
        => TryQueueFlow(flow,
            mustPipeline ? protocol => protocol.PipelineDepth > 0 : null,
            mustPipeline ? this : default!);

    bool TryQueueFlow<TState>(EmptyFlow<TMode> flow, Func<TState, bool>? predicate, TState state)
    {
        UnboundedQueueSource<EmptyFlow<TMode>>.EnqueueResult enqueue;
        lock (_syncRoot)
        {
            if (_status != ProtocolStatus.Ready)
                return false;

            if (predicate?.Invoke(state) == false)
                return false;

            enqueue = _pipeline.Enqueue(flow);
        }
        enqueue.Execute();
        return true;
    }

    async ValueTask IPoolConnection<EmptyProtocol<TMode>>.CompleteAsync(Exception? exception)
    {
        lock (_syncRoot)
        {
            if (_status is ProtocolStatus.Completed)
                return;
            _status = ProtocolStatus.Draining;
            _drainingCount++;
        }
        try
        {
            _abortCts.CancelAfter(_options.CompletionTimeout);
            await _pipeline.CompleteAsync(exception).ConfigureAwait(false);
        }
        finally
        {
            _abortCts.CancelAfter(TimeSpan.Zero);
            lock (_syncRoot) _status = ProtocolStatus.Completed;
        }
    }

    bool IPoolConnection<EmptyProtocol<TMode>>.IsIdle => PipelineDepth is 0;
    bool IPoolConnection<EmptyProtocol<TMode>>.IsCompleted => _status is ProtocolStatus.Completed;

    // The benchmark protocol has no startup flow (it constructs immediately Ready), so there is
    // no suppressed idle publication for the pool's post-lease Start to unblock.
    void IPoolConnection<EmptyProtocol<TMode>>.Start() { }

    int IPoolConnection<EmptyProtocol<TMode>>.CompareTo(EmptyProtocol<TMode>? other)
    {
        // null instances are always better, they represent empty connection slots.
        if (other is null)
            return 1;

        var score = PipelineDepth;
        var otherScore = other.PipelineDepth;

        return score < otherScore ? -1 : score == otherScore ? 0 : 1;
    }

    internal readonly struct Policy : IPipelinePolicy<EmptyFlow<TMode>>
    {
        readonly EmptyProtocol<TMode> _protocol;
        readonly ActivationWorkItem _activationWorkItem;

        public Policy(EmptyProtocol<TMode> protocol)
        {
            _protocol = protocol;
            _activationWorkItem = new(protocol.FlowControl);
            RunEnqueueAsynchronously = protocol._options.RunEnqueueAsynchronously;
        }

        public bool RunEnqueueAsynchronously { get; }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CompleteItem(EmptyFlow<TMode> item, int remainingDepth, Exception? exception)
        {
            ((IProtocolFlow)item).Complete(exception);
            _protocol.FlowControl.OnCompleted(item, remainingDepth);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<PipelineItemResult> ExecuteItemAsync(EmptyFlow<TMode> item, bool waiterExecution, CancellationToken cancellationToken)
        {
            ((IProtocolFlow)item).Start();
            return ExecuteCore(_protocol.FlowControl, item, cancellationToken);

            static async ValueTask<PipelineItemResult> ExecuteCore(
                Control flowControl, EmptyFlow<TMode> item, CancellationToken cancellationToken)
            {
                var tasks = await flowControl.Execute(item, cancellationToken).ConfigureAwait(false);
                return new PipelineItemResult(tasks.TrailingExecutionTask, tasks.PipelineTask);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ActivateHeadItem(EmptyFlow<TMode> item, bool preferAsync = true)
        {
            if (preferAsync)
            {
                _activationWorkItem.Initialize(item);
                ThreadPool.UnsafeQueueUserWorkItem(_activationWorkItem, preferLocal: true);
            }
            else
                _protocol.FlowControl.Activate(item);
        }

        public bool TryRecoverItemFailure(PipelineItemFailureContext context, EmptyFlow<TMode> failedItem, CancellationToken cancellationToken, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out EmptyFlow<TMode>? recoveryItem)
        {
            recoveryItem = null;
            return false;
        }

        sealed class ActivationWorkItem(Control flowControl) : IThreadPoolWorkItem
        {
            EmptyFlow<TMode> _item = null!;
            public void Initialize(EmptyFlow<TMode> item) => _item = item;
            void IThreadPoolWorkItem.Execute() => flowControl.Activate(_item);
        }
    }

    internal sealed class Control(EmptyProtocol<TMode> protocol)
    {
        internal ValueTask<FlowTasks> Execute(EmptyFlow<TMode> flow, CancellationToken completionToken)
            => flow.Execute(new EmptyFlowContext<TMode>(flow, this), completionToken);

        internal void Activate(EmptyFlow<TMode> flow)
        {
            if (typeof(TMode) == typeof(Pipelined) || typeof(TMode) == typeof(PipelinedUserCompleted) || typeof(TMode) == typeof(PooledUserCompleted))
            {
                flow.ActivateFlow();
            }
        }

        internal void OnCompleted(EmptyFlow<TMode> flow, int remainingDepth)
        {
            if (typeof(TMode) == typeof(PooledUserCompleted))
            {
                if (remainingDepth is 0)
                    protocol._poolConnectionIdleSignal?.Invoke();
            }
        }
    }
}
