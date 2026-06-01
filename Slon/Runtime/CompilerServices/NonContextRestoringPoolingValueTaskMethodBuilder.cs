using System.Runtime.CompilerServices;

namespace Slon.Runtime.CompilerServices;

struct NonContextRestoringPoolingValueTaskMethodBuilder()
{
    PoolingAsyncValueTaskMethodBuilder _mb = PoolingAsyncValueTaskMethodBuilder.Create();
    public static NonContextRestoringPoolingValueTaskMethodBuilder Create() => new();

    public ValueTask Task => _mb.Task;
    public void SetException(Exception e) => _mb.SetException(e);
    public void SetResult() =>  _mb.SetResult();

    public void AwaitOnCompleted<TAwaiter, TStateMachine>(ref TAwaiter awaiter, ref TStateMachine stateMachine)
        where TAwaiter : INotifyCompletion
        where TStateMachine : IAsyncStateMachine
        => _mb.AwaitOnCompleted(ref awaiter, ref stateMachine);

    public void AwaitUnsafeOnCompleted<TAwaiter, TStateMachine>(ref TAwaiter awaiter, ref TStateMachine stateMachine)
        where TAwaiter : ICriticalNotifyCompletion
        where TStateMachine : IAsyncStateMachine
        => _mb.AwaitUnsafeOnCompleted(ref awaiter, ref stateMachine);

    // Start is just MoveNext, no capture and restore of execution context or sync context.
    public void Start<TStateMachine>(ref TStateMachine stateMachine) where TStateMachine : IAsyncStateMachine
        => stateMachine.MoveNext();

    public void SetStateMachine(IAsyncStateMachine stateMachine) => _mb.SetStateMachine(stateMachine);
}

struct NonContextRestoringPoolingValueTaskMethodBuilder<T>()
{
    PoolingAsyncValueTaskMethodBuilder<T> _mb = PoolingAsyncValueTaskMethodBuilder<T>.Create();
    public static NonContextRestoringPoolingValueTaskMethodBuilder<T> Create() => new();

    public ValueTask<T> Task => _mb.Task;
    public void SetException(Exception e) => _mb.SetException(e);
    public void SetResult(T result) =>  _mb.SetResult(result);

    public void AwaitOnCompleted<TAwaiter, TStateMachine>(ref TAwaiter awaiter, ref TStateMachine stateMachine)
        where TAwaiter : INotifyCompletion
        where TStateMachine : IAsyncStateMachine
        => _mb.AwaitOnCompleted(ref awaiter, ref stateMachine);

    public void AwaitUnsafeOnCompleted<TAwaiter, TStateMachine>(ref TAwaiter awaiter, ref TStateMachine stateMachine)
        where TAwaiter : ICriticalNotifyCompletion
        where TStateMachine : IAsyncStateMachine
        => _mb.AwaitUnsafeOnCompleted(ref awaiter, ref stateMachine);

    // Start is just MoveNext, no capture and restore of execution context or sync context.
    public void Start<TStateMachine>(ref TStateMachine stateMachine) where TStateMachine : IAsyncStateMachine
        => stateMachine.MoveNext();

    public void SetStateMachine(IAsyncStateMachine stateMachine) => _mb.SetStateMachine(stateMachine);
}
