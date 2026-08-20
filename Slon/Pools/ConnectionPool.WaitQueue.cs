using System.Diagnostics;
using System.Threading.Tasks.Sources;

namespace Slon.Pools;

static partial class ConnectionPool
{
    // At most one caller drives placement attempts. NotifyAvailability requires capacity to be
    // visible first; PublishAvailability brackets identity-bearing capacity and its generation under
    // the queue lock. An active pass therefore observes either the capacity or an obligation to rescan.
    internal sealed class WaitQueue<TResult> : IDisposable
    {
        const long StateMask = 3;
        const long GenerationStep = 4;
        const long GenerationMask = ~StateMask;

        enum DriverState : long
        {
            Idle,
            Armed,
            Active
        }

        readonly Lock _lock = new();
        Waiter? _head;
        Waiter? _tail;
        Waiter? _cursor;
        Waiter? _passTail;
        long _word;
        long _pass;
        long _totalExamined;
        long _totalPlacements;
        long _totalGenerationRestarts;
        long _maxExaminedPerDrive;
        long _maxPlacementsPerDrive;
        long _maxInlineTicks;
        int _count;
        bool _joinPending;
        bool _disposed;

        internal bool HasDemand => GetState(Volatile.Read(ref _word)) is not DriverState.Idle;

        internal int Count
        {
            get
            {
                lock (_lock)
                    return _count;
            }
        }

        internal DriverMetrics Metrics => new(
            Volatile.Read(ref _totalExamined),
            Volatile.Read(ref _totalPlacements),
            Volatile.Read(ref _totalGenerationRestarts),
            Volatile.Read(ref _maxExaminedPerDrive),
            Volatile.Read(ref _maxPlacementsPerDrive),
            TimeSpan.FromTicks(Volatile.Read(ref _maxInlineTicks)));

        internal Waiter<TState> CreateWaiter<TState>(
            Func<TState, long, PlacementAttempt<TResult>> tryPlace, TState state, bool synchronous = false)
            => new(tryPlace, state, synchronous);

        internal UnversionedWaiter<TState> CreateWaiter<TState>(
            Func<TState, PlacementAttempt<TResult>> tryPlace, TState state, bool synchronous = false)
            => new(tryPlace, state, synchronous);

        // Returns the cancellation registration owned by the acquisition wrapper. It must be disposed
        // after the waiter result is consumed so the callback cannot outlive its waiter state.
        internal CancellationTokenRegistration Enqueue(Waiter waiter, CancellationToken cancellationToken = default)
        {
            bool drive;
            bool completeImmediately;
            lock (_lock)
            {
                ObjectDisposedException.ThrowIf(_disposed, this);
                if (cancellationToken.IsCancellationRequested)
                {
                    waiter.State = WaiterState.Terminal;
                    waiter.Completion = Completion<TResult>.Failed(waiter.CreateCancellationException(cancellationToken));
                    drive = false;
                    completeImmediately = true;
                }
                else
                {
                    Link(waiter);
                    drive = ClaimDriverForNewWaiter();
                    completeImmediately = false;
                }
            }

            waiter.Owner = this;
            waiter.CancellationToken = cancellationToken;
            var registration = cancellationToken.CanBeCanceled
                ? cancellationToken.UnsafeRegister(static state =>
                {
                    var request = (Waiter)state!;
                    request.Owner!.Cancel(request, request.CancellationToken);
                }, waiter)
                : default;

            if (completeImmediately)
                waiter.Complete();
            else if (drive)
                Drive();
            return registration;
        }

        internal void NotifyAvailability()
        {
            while (true)
            {
                var word = Interlocked.CompareExchange(ref _word, 0, 0);
                var state = GetState(word);
                var next = NextGeneration(word) | (long)(state is DriverState.Armed ? DriverState.Active : state);
                if (Interlocked.CompareExchange(ref _word, next, word) != word)
                    continue;

                if (state is DriverState.Armed)
                    Drive();
                return;
            }
        }

        // Identity-bearing capacity becomes visible in the same queue-lock section that advances the
        // generation. A driver pass therefore cannot observe the capacity before its restart obligation.
        // The callback runs under the coordinator lock and must remain cheap and must not re-enter it.
        internal void PublishAvailability<TState>(Action<TState, long> publish, TState state,
            bool publishWhenDisposed = false)
        {
            bool drive;
            lock (_lock)
            {
                if (_disposed)
                {
                    if (publishWhenDisposed)
                        publish(state, GetGeneration(Volatile.Read(ref _word)));
                    return;
                }
                while (true)
                {
                    var word = Interlocked.CompareExchange(ref _word, 0, 0);
                    var driverState = GetState(word);
                    var generation = NextGeneration(word);
                    var next = generation |
                        (long)(driverState is DriverState.Armed ? DriverState.Active : driverState);
                    if (Interlocked.CompareExchange(ref _word, next, word) != word)
                        continue;
                    publish(state, generation);
                    drive = driverState is DriverState.Armed;
                    break;
                }
            }

            if (drive)
                Drive();
        }

        internal void Cancel(Waiter waiter, CancellationToken cancellationToken)
        {
            Completion<TResult> completion = default;
            var complete = false;
            lock (_lock)
            {
                if (waiter.State is WaiterState.Terminal)
                    return;

                var exception = waiter.CreateCancellationException(cancellationToken);
                if (waiter.State is WaiterState.Trying)
                {
                    waiter.Cancellation = exception;
                    return;
                }

                Debug.Assert(waiter.State is WaiterState.Queued);
                Unlink(waiter);
                waiter.State = WaiterState.Terminal;
                completion = waiter.Completion = Completion<TResult>.Failed(exception);
                complete = true;
                DisarmIfEmpty();
            }

            if (complete)
                waiter.Complete(completion);
        }

        public void Dispose()
        {
            List<(Waiter Waiter, Completion<TResult> Completion)>? completions = null;
            lock (_lock)
            {
                if (_disposed)
                    return;
                _disposed = true;

                for (var waiter = _head; waiter is not null;)
                {
                    var next = waiter.Next;
                    if (waiter.State is WaiterState.Trying)
                    {
                        waiter.DisposeRequested = true;
                    }
                    else
                    {
                        Unlink(waiter);
                        waiter.State = WaiterState.Terminal;
                        var completion = waiter.Completion = Completion<TResult>.Failed(
                            new ObjectDisposedException(nameof(WaitQueue<TResult>)));
                        (completions ??= []).Add((waiter, completion));
                    }
                    waiter = next;
                }
                DisarmIfEmpty();
            }

            if (completions is not null)
            {
                foreach (var item in completions)
                    item.Waiter.Complete(item.Completion);
            }
        }

        bool ClaimDriverForNewWaiter()
        {
            Debug.Assert(_lock.IsHeldByCurrentThread);
            while (true)
            {
                var word = Interlocked.CompareExchange(ref _word, 0, 0);
                if (GetState(word) is DriverState.Active)
                {
                    // The pass tail is a snapshot. A newcomer joins the next pass instead of resetting
                    // current progress; otherwise a steady arrival stream could starve the old tail.
                    _joinPending = true;
                    return false;
                }
                var next = GetGeneration(word) | (long)DriverState.Active;
                if (Interlocked.CompareExchange(ref _word, next, word) == word)
                    return true;
            }
        }

        void Drive()
        {
            var started = Stopwatch.GetTimestamp();
            long examined = 0;
            long placements = 0;
            long generationRestarts = 0;
            var pass = 0L;
            var generation = 0L;
            while (true)
            {
                Waiter? waiter;
                lock (_lock)
                {
                    var word = Volatile.Read(ref _word);
                    Debug.Assert(GetState(word) is DriverState.Active);
                    if (pass == 0)
                    {
                        pass = ++_pass;
                        generation = GetGeneration(word);
                        _cursor = _head;
                        _passTail = _tail;
                        _joinPending = false;
                    }
                    waiter = FindUntried(pass);
                    if (waiter is not null)
                    {
                        waiter.TriedPass = pass;
                        waiter.State = WaiterState.Trying;
                    }
                    else if (!_joinPending && TryReleaseDriver(generation, out _))
                    {
                        RecordDrive(examined, placements, generationRestarts, started);
                        return;
                    }
                    else
                    {
                        generationRestarts++;
                        pass = ++_pass;
                        generation = GetGeneration(Volatile.Read(ref _word));
                        _cursor = _head;
                        _passTail = _tail;
                        _joinPending = false;
                        continue;
                    }
                }

                PlacementAttempt<TResult> attempt;
                try
                {
                    examined++;
                    attempt = waiter.TryPlace(generation);
                }
                catch (Exception ex)
                {
                    attempt = PlacementAttempt<TResult>.Faulted(ex);
                }

                Completion<TResult> completion = default;
                var complete = false;
                lock (_lock)
                {
                    Debug.Assert(waiter.State is WaiterState.Trying);
                    if (attempt.HasResult)
                    {
                        placements++;
                        Unlink(waiter);
                        waiter.State = WaiterState.Terminal;
                        Exception? termination = waiter.DisposeRequested
                            ? new ObjectDisposedException(nameof(WaitQueue<TResult>))
                            : waiter.Cancellation;
                        completion = waiter.Completion = Completion<TResult>.Placed(attempt.Result, termination);
                        complete = true;
                    }
                    else if (waiter.DisposeRequested || waiter.Cancellation is not null || attempt.Exception is not null)
                    {
                        Unlink(waiter);
                        waiter.State = WaiterState.Terminal;
                        var exception = waiter.DisposeRequested
                            ? new ObjectDisposedException(nameof(WaitQueue<TResult>))
                            : waiter.Cancellation ?? attempt.Exception!;
                        completion = waiter.Completion = Completion<TResult>.Failed(exception);
                        complete = true;
                    }
                    else
                    {
                        waiter.State = WaiterState.Queued;
                    }
                }

                if (complete)
                    waiter.Complete(completion);
            }
        }

        void RecordDrive(long examined, long placements, long generationRestarts, long started)
        {
            Interlocked.Add(ref _totalExamined, examined);
            Interlocked.Add(ref _totalPlacements, placements);
            Interlocked.Add(ref _totalGenerationRestarts, generationRestarts);
            RecordMaximum(ref _maxExaminedPerDrive, examined);
            RecordMaximum(ref _maxPlacementsPerDrive, placements);
            RecordMaximum(ref _maxInlineTicks, Stopwatch.GetElapsedTime(started).Ticks);

            static void RecordMaximum(ref long target, long value)
            {
                var observed = Volatile.Read(ref target);
                while (value > observed)
                {
                    var prior = Interlocked.CompareExchange(ref target, value, observed);
                    if (prior == observed)
                        return;
                    observed = prior;
                }
            }
        }

        bool TryReleaseDriver(long generation, out long retryGeneration)
        {
            Debug.Assert(_lock.IsHeldByCurrentThread);
            while (true)
            {
                var word = Interlocked.CompareExchange(ref _word, 0, 0);
                retryGeneration = GetGeneration(word);
                if (retryGeneration != generation)
                    return false;

                var state = _head is null ? DriverState.Idle : DriverState.Armed;
                var next = retryGeneration | (long)state;
                if (Interlocked.CompareExchange(ref _word, next, word) == word)
                    return true;
            }
        }

        void DisarmIfEmpty()
        {
            Debug.Assert(_lock.IsHeldByCurrentThread);
            if (_head is not null)
                return;

            while (true)
            {
                var word = Interlocked.CompareExchange(ref _word, 0, 0);
                if (GetState(word) is not DriverState.Armed)
                    return;
                var next = GetGeneration(word) | (long)DriverState.Idle;
                if (Interlocked.CompareExchange(ref _word, next, word) == word)
                    return;
            }
        }

        Waiter? FindUntried(long pass)
        {
            Debug.Assert(_lock.IsHeldByCurrentThread);
            while (_cursor is { } waiter)
            {
                _cursor = ReferenceEquals(waiter, _passTail) ? null : waiter.Next;
                if (waiter.State is WaiterState.Queued && waiter.TriedPass != pass)
                    return waiter;
            }
            return null;
        }

        void Link(Waiter waiter)
        {
            Debug.Assert(_lock.IsHeldByCurrentThread);
            waiter.Previous = _tail;
            if (_tail is null)
                _head = waiter;
            else
                _tail.Next = waiter;
            _tail = waiter;
            waiter.State = WaiterState.Queued;
            _count++;
        }

        void Unlink(Waiter waiter)
        {
            Debug.Assert(_lock.IsHeldByCurrentThread);
            if (ReferenceEquals(_passTail, waiter))
            {
                _passTail = waiter.Previous;
                if (ReferenceEquals(_cursor, waiter))
                    _cursor = null;
            }
            else if (ReferenceEquals(_cursor, waiter))
                _cursor = waiter.Next;
            if (waiter.Previous is null)
                _head = waiter.Next;
            else
                waiter.Previous.Next = waiter.Next;
            if (waiter.Next is null)
                _tail = waiter.Previous;
            else
                waiter.Next.Previous = waiter.Previous;
            waiter.Next = waiter.Previous = null;
            _count--;
        }

        static DriverState GetState(long word) => (DriverState)(word & StateMask);
        static long GetGeneration(long word) => word & GenerationMask;
        static long NextGeneration(long word) => unchecked((GetGeneration(word) + GenerationStep) & GenerationMask);

        internal enum WaiterState : byte
        {
            Out,
            Queued,
            Trying,
            Terminal
        }

        internal abstract class Waiter : IValueTaskSource<Completion<TResult>>
        {
            ManualResetValueTaskSourceCore<Completion<TResult>> _asyncCompletion;
            readonly ManualResetEventSlim? _syncCompletion;

            protected Waiter(bool synchronous)
            {
                _asyncCompletion.RunContinuationsAsynchronously = true;
                if (synchronous)
                    _syncCompletion = new();
            }

            internal Waiter? Previous;
            internal Waiter? Next;
            internal WaitQueue<TResult>? Owner;
            internal CancellationToken CancellationToken;
            internal WaiterState State;
            internal long TriedPass;
            internal OperationCanceledException? Cancellation;
            internal bool DisposeRequested;
            internal Completion<TResult> Completion;

            internal abstract PlacementAttempt<TResult> TryPlace(long generation);

            internal OperationCanceledException CreateCancellationException(CancellationToken cancellationToken)
                => _syncCompletion is null
                    ? new TaskCanceledException(null, null, cancellationToken)
                    : new OperationCanceledException(cancellationToken);

            internal ValueTask<Completion<TResult>> AsValueTask()
                => _syncCompletion is null
                    ? new(this, _asyncCompletion.Version)
                    : throw new InvalidOperationException("The synchronous waiter has no ValueTask.");

            internal Completion<TResult> Wait()
            {
                var completionSource = _syncCompletion!;
                completionSource.Wait();
                // Wait may observe the signal before Set has finished touching its lazily-created
                // kernel event. Taking the same lock closes that tail before disposal.
                lock (completionSource)
                {
                    var completion = Completion;
                    completionSource.Dispose();
                    return completion;
                }
            }

            internal void Complete() => Complete(Completion);

            internal void Complete(Completion<TResult> completion)
            {
                if (_syncCompletion is not null)
                {
                    lock (_syncCompletion)
                    {
                        Completion = completion;
                        _syncCompletion.Set();
                    }
                }
                else
                    _asyncCompletion.SetResult(completion);
            }

            public Completion<TResult> GetResult(short token) => _asyncCompletion.GetResult(token);
            public ValueTaskSourceStatus GetStatus(short token) => _asyncCompletion.GetStatus(token);
            public void OnCompleted(Action<object?> continuation, object? state, short token,
                ValueTaskSourceOnCompletedFlags flags)
                => _asyncCompletion.OnCompleted(continuation, state, token, flags);
        }

        internal sealed class Waiter<TState> : Waiter
        {
            readonly Func<TState, long, PlacementAttempt<TResult>> _tryPlace;
            readonly TState _state;

            internal Waiter(Func<TState, long, PlacementAttempt<TResult>> tryPlace, TState state, bool synchronous)
                : base(synchronous)
                => (_tryPlace, _state) = (tryPlace, state);

            internal override PlacementAttempt<TResult> TryPlace(long generation) => _tryPlace(_state, generation);
        }

        internal sealed class UnversionedWaiter<TState> : Waiter
        {
            readonly Func<TState, PlacementAttempt<TResult>> _tryPlace;
            readonly TState _state;

            internal UnversionedWaiter(Func<TState, PlacementAttempt<TResult>> tryPlace, TState state, bool synchronous)
                : base(synchronous)
                => (_tryPlace, _state) = (tryPlace, state);

            internal override PlacementAttempt<TResult> TryPlace(long generation) => _tryPlace(_state);
        }

        internal readonly record struct DriverMetrics(
            long TotalExamined,
            long TotalPlacements,
            long TotalGenerationRestarts,
            long MaxExaminedPerDrive,
            long MaxPlacementsPerDrive,
            TimeSpan MaxInlineDuration);
    }

    internal readonly record struct PlacementAttempt<TResult>
    {
        PlacementAttempt(bool hasResult, TResult result, Exception? exception)
            => (HasResult, Result, Exception) = (hasResult, result, exception);

        internal bool HasResult { get; }
        internal TResult Result { get; }
        internal Exception? Exception { get; }

        internal static PlacementAttempt<TResult> Placed(TResult result) => new(true, result, null);
        internal static PlacementAttempt<TResult> Unavailable => default;
        internal static PlacementAttempt<TResult> Faulted(Exception exception) => new(false, default!, exception);
    }

    internal readonly record struct Completion<TResult>
    {
        Completion(bool hasResult, TResult result, Exception? exception)
            => (HasResult, Result, Exception) = (hasResult, result, exception);

        internal bool HasResult { get; }
        internal TResult Result { get; }
        internal Exception? Exception { get; }

        internal static Completion<TResult> Placed(TResult result, Exception? termination = null)
            => new(true, result, termination);
        internal static Completion<TResult> Failed(Exception exception) => new(false, default!, exception);
    }
}
