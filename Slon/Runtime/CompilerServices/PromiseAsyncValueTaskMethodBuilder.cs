using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks.Sources;

namespace Slon.Runtime.CompilerServices;

struct PromiseAsyncValueTaskMethodBuilder<TResult>
{
    // As we cannot pass arguments to builders today we use a thread-static to pass the promise instance.
    [field: ThreadStatic]
    public static ValueTaskSourcePromise<TResult>? Promise { get; set; }

    /// Sets Promise to `promise` and returns a disposable that clears it on exit. Intended for
    /// `using (PromiseAsyncValueTaskMethodBuilder{T}.BeginCallScope(_readPromise)) { return LocalFn(); }`
    /// so the builder's Create() picks up the promise during LocalFn's state machine construction.
    public static CallScope BeginCallScope(ValueTaskSourcePromise<TResult> promise)
    {
        Promise = promise;
        return default;
    }

    public ref struct CallScope
    {
        public void Dispose() => Promise = null;
    }

    readonly ValueTaskSourcePromise<TResult> _promise;
    ValueTask<TResult> _task;
    bool _promiseTask;

    PromiseAsyncValueTaskMethodBuilder(ValueTaskSourcePromise<TResult> promise)
    {
        _promise = promise;
    }

    public static PromiseAsyncValueTaskMethodBuilder<TResult> Create(ValueTaskSourcePromise<TResult> promise)
    {
        ArgumentNullException.ThrowIfNull(promise);
        return new(promise);
    }

    public static PromiseAsyncValueTaskMethodBuilder<TResult> Create()
    {
        var promise = Promise;
        if (promise is null)
            ThrowNoPromise();

        return new(promise);

        [DoesNotReturn]
        static void ThrowNoPromise()
            => throw new InvalidOperationException("Provide a promise instance through the thread static on this builder type.");
    }

    public void Start<TStateMachine>(ref TStateMachine stateMachine) where TStateMachine : IAsyncStateMachine
    {
        if (!_promise.TryStart())
            ThrowAlreadyStarted();

        // Use the standard capture/restore start logic.
        AsyncValueTaskMethodBuilder<TResult>.Create().Start(ref stateMachine);

        static void ThrowAlreadyStarted()
            => throw new InvalidOperationException("The async method is already executing, multiple callers are not supported.");
    }

    public void SetStateMachine(IAsyncStateMachine stateMachine)
    {
        ArgumentNullException.ThrowIfNull(stateMachine);
    }

    public void SetResult(TResult result)
    {
        if (_promiseTask)
        {
            _promise.SetResult(result);
        }
        else
        {
            _task = new ValueTask<TResult>(result);
        }
    }

    public void SetException(Exception exception)
    {
        if (_promiseTask)
        {
            _promise.SetException(exception);
        }
        else
        {
            _task = new(System.Threading.Tasks.Task.FromException<TResult>(exception));
        }
    }

    public void AwaitOnCompleted<TAwaiter, TStateMachine>(ref TAwaiter awaiter, ref TStateMachine stateMachine)
        where TAwaiter : INotifyCompletion
        where TStateMachine : IAsyncStateMachine
    {
        _promiseTask = true;
        _promise.AwaitOnCompleted(ref awaiter, ref stateMachine);
    }

    public void AwaitUnsafeOnCompleted<TAwaiter, TStateMachine>(ref TAwaiter awaiter, ref TStateMachine stateMachine)
        where TAwaiter : ICriticalNotifyCompletion
        where TStateMachine : IAsyncStateMachine
    {
        _promiseTask = true;
        _promise.AwaitUnsafeOnCompleted(ref awaiter, ref stateMachine);
    }

    public ValueTask<TResult> Task => _promise.TryGetTask(out var task) ? task : _task;
}

struct PromiseAsyncValueTaskMethodBuilder
{
    // As we cannot pass arguments to builders today we use a thread-static to pass the promise instance.
    [field: ThreadStatic]
    public static ValueTaskSourcePromise<bool>? Promise { get; set; }

    /// Sets Promise to `promise` and returns a disposable that clears it on exit. Intended for
    /// `using (PromiseAsyncValueTaskMethodBuilder.BeginCallScope(_readPromise)) { return LocalFn(); }`
    /// so the builder's Create() picks up the promise during LocalFn's state machine construction.
    public static CallScope BeginCallScope(ValueTaskSourcePromise<bool> promise)
    {
        Promise = promise;
        return default;
    }

    public ref struct CallScope
    {
        public void Dispose() => Promise = null;
    }

    readonly ValueTaskSourcePromise<bool> _promise;
    ValueTask _task;
    bool _promiseTask;

    PromiseAsyncValueTaskMethodBuilder(ValueTaskSourcePromise<bool> promise)
    {
        ArgumentNullException.ThrowIfNull(promise);
        _promise = promise;
    }

    public static PromiseAsyncValueTaskMethodBuilder Create(ValueTaskSourcePromise<bool> promise)
        => new(promise);

    public static PromiseAsyncValueTaskMethodBuilder Create()
    {
        var promise = Promise;
        if (promise is null)
            ThrowNoPromise();

        return new(promise);

        [DoesNotReturn]
        static void ThrowNoPromise()
            => throw new InvalidOperationException("Provide a promise instance through the thread static on this builder type.");
    }

    public void Start<TStateMachine>(ref TStateMachine stateMachine) where TStateMachine : IAsyncStateMachine
    {
        if (!_promise.TryStart())
            ThrowAlreadyStarted();

        // Use the standard capture/restore start logic.
        AsyncValueTaskMethodBuilder.Create().Start(ref stateMachine);

        static void ThrowAlreadyStarted()
            => throw new InvalidOperationException("The async method is already executing, multiple callers are not supported.");
    }

    public void SetStateMachine(IAsyncStateMachine stateMachine)
    {
        ArgumentNullException.ThrowIfNull(stateMachine);
    }

    public void SetResult()
    {
        if (_promiseTask)
        {
            _promise.SetResult(true);
        }
        else
        {
            _task = new ValueTask();
        }
    }

    public void SetException(Exception exception)
    {
        if (_promiseTask)
        {
            _promise.SetException(exception);
        }
        else
        {
            _task = new(System.Threading.Tasks.Task.FromException(exception));
        }
    }

    public void AwaitOnCompleted<TAwaiter, TStateMachine>(ref TAwaiter awaiter, ref TStateMachine stateMachine)
        where TAwaiter : INotifyCompletion
        where TStateMachine : IAsyncStateMachine
    {
        _promiseTask = true;
        _promise.AwaitOnCompleted(ref awaiter, ref stateMachine);
    }

    public void AwaitUnsafeOnCompleted<TAwaiter, TStateMachine>(ref TAwaiter awaiter, ref TStateMachine stateMachine)
        where TAwaiter : ICriticalNotifyCompletion
        where TStateMachine : IAsyncStateMachine
    {
        _promiseTask = true;
        _promise.AwaitUnsafeOnCompleted(ref awaiter, ref stateMachine);
    }

    public ValueTask Task => _promise.TryGetVoidTask(out var task) ? task : _task;
}

sealed class ValueTaskSourcePromise<TResult> : IValueTaskSource<TResult>, IValueTaskSource
{
    StateMachineBox? _stateMachineBox;
    Slon.Threading.Tasks.Sources.ManualResetValueTaskSourceCore<TResult> _core;
    bool _started;
    bool _taskSourceRequired;

    public short Token => _core.Version;
    public bool IsStarted => Volatile.Read(ref _started);

    public void AwaitOnCompleted<TAwaiter, TStateMachine>(
        ref TAwaiter awaiter,
        ref TStateMachine stateMachine)
        where TAwaiter : INotifyCompletion
        where TStateMachine : IAsyncStateMachine
    {
        var continuation = GetContinuation(ref stateMachine);
        awaiter.OnCompleted(continuation);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void AwaitUnsafeOnCompleted<TAwaiter, TStateMachine>(
        ref TAwaiter awaiter,
        ref TStateMachine stateMachine)
        where TAwaiter : ICriticalNotifyCompletion
        where TStateMachine : IAsyncStateMachine
    {
        var continuation = GetContinuation(ref stateMachine);
        try
        {
            awaiter.UnsafeOnCompleted(continuation);
        }
        catch (Exception ex)
        {
            ThreadPool.QueueUserWorkItem(state => ((ExceptionDispatchInfo)state!).Throw(), ExceptionDispatchInfo.Capture(ex));
        }
    }

    Action GetContinuation<TStateMachine>(ref TStateMachine stateMachine) where TStateMachine : IAsyncStateMachine
    {
        if (!_started)
            ThrowInvalidOperation();

        Action continuation;
        var executionContext = ExecutionContext.Capture();
        if (_taskSourceRequired)
        {
            // We should already have the right state machine.
            Debug.Assert(_stateMachineBox is StateMachineBox<TStateMachine>);
            continuation = _stateMachineBox.MoveNextAction;
            if (!ReferenceEquals(_stateMachineBox.ExecutionContext, executionContext))
                _stateMachineBox.ExecutionContext = executionContext;
        }
        else
        {
            continuation = Core(ref stateMachine);
            _stateMachineBox.ExecutionContext = executionContext;
        }

        _taskSourceRequired = true;
        return continuation;

        [MethodImpl(MethodImplOptions.NoInlining)]
        [MemberNotNull(nameof(_stateMachineBox))]
        Action Core(ref TStateMachine stateMachine)
        {
            if (_stateMachineBox is StateMachineBox<TStateMachine> box)
            {
                box.Initialize(stateMachine);
            }
            else
            {
                box = new StateMachineBox<TStateMachine>();
                box.Initialize(stateMachine);
                _stateMachineBox = box;
            }

            return _stateMachineBox.MoveNextAction;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryStart() => !Interlocked.CompareExchange(ref _started, true, false);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryGetTask(out ValueTask<TResult> task)
    {
        if (!_started)
            ThrowInvalidOperation();

        if (!_taskSourceRequired)
        {
            task = default;
            Volatile.Write(ref _started, false);
            return false;
        }

        task = new(this, _core.Version);
        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryGetVoidTask(out ValueTask task)
    {
        if (!_started)
            ThrowInvalidOperation();

        if (!_taskSourceRequired)
        {
            task = default;
            Volatile.Write(ref _started, false);
            return false;
        }

        task = new(this, _core.Version);
        return true;
    }

    public void SetResult(TResult result) => _core.SetResult(result);
    public void SetException(Exception exception) => _core.SetException(exception);

    TResult IValueTaskSource<TResult>.GetResult(short token)
    {
        Debug.Assert(_taskSourceRequired);
        // Fused consume-and-reset: a stale/mismatched token throws from the core's own token check
        // with NOTHING consumed or reset - resetting on that path would wipe whatever tenure is
        // ACTUALLY live (nulling a pending registration nobody will invoke, and reopening _started
        // while that tenure's body may still be running). A genuine consume - successful or
        // faulted alike - recycles the core, so the wrapper's own tenure state retires with it
        // before the payload rethrow.
        var result = _core.GetResultAndReset(token, out var error);
        _stateMachineBox?.Reset();
        _taskSourceRequired = false;
        Volatile.Write(ref _started, false);
        error?.Throw();
        return result;
    }

    ValueTaskSourceStatus IValueTaskSource.GetStatus(short token)
        => _core.GetStatus(token);

    void IValueTaskSource.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => _core.OnCompleted(continuation, state, token, flags);

    void IValueTaskSource.GetResult(short token)
        => ((IValueTaskSource<TResult>)this).GetResult(token);

    ValueTaskSourceStatus IValueTaskSource<TResult>.GetStatus(short token)
        => _core.GetStatus(token);

    void IValueTaskSource<TResult>.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => _core.OnCompleted(continuation, state, token, flags);

    static void ThrowInvalidOperation()
        => throw new InvalidOperationException();

    abstract class StateMachineBox
    {
        protected Action _moveNextAction = null!;
        public ExecutionContext? ExecutionContext { get; set; }

        public Action MoveNextAction => _moveNextAction;
        public abstract void Reset();
    }

    sealed class StateMachineBox<TStateMachine> : StateMachineBox where TStateMachine : IAsyncStateMachine
    {
        TStateMachine _stateMachine = default!;

        public StateMachineBox()
        {
            _moveNextAction = MoveNext;
        }

        public void Initialize(in TStateMachine stateMachine)
        {
            _stateMachine = stateMachine;
        }

        public void MoveNext()
        {
            ExecutionContext? existingContext = null;
            try
            {
                if (ExecutionContext is { } context)
                {
                    existingContext = ExecutionContext.Capture();
                    ExecutionContext.Restore(context);
                }
                _stateMachine.MoveNext();
            }
            finally
            {
                if (existingContext is not null)
                    ExecutionContext.Restore(existingContext);
            }
        }

        public override void Reset()
        {
            ExecutionContext = null;
            _stateMachine = default!;
        }
    }
}
