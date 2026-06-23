using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Slon.Threading;

// A minimal non-reentrant test-and-set spinlock for nanosecond-scale, almost-always-uncontended
// critical sections on a hot path. Deliberately SHADOWS System.Threading.SpinLock within this
// namespace - the BCL one carries owner-thread tracking and a richer backoff state machine that
// costs more on the uncontended take than this bare CAS; Monitor/lock is reentrancy-safe, which we
// never need and pay for in object + fast-path cost.
//
// NOT reentrancy-safe: the holder must never re-acquire, directly or transitively, or it spins
// forever on its own held lock. The non-obvious case is a synchronous IValueTaskSource completion
// run inline under the lock, whose continuation re-enters - mutate state under the lock, release,
// THEN dispatch.
//
// Holder must not block or await while held: the section is straight-line state mutation only, so a
// waiter spins for nanoseconds against a runnable holder, never against a descheduled one.
struct SpinLock
{
    // 0 = free, 1 = held. Field (not s_-prefixed; no Hungarian per house style).
    int _state;

    // Acquire by CAS, spinning with backoff until free. SpinWait yields after a bounded busy spin so a
    // rare contended take never burns a core, while the common uncontended take is a single CAS.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Enter()
    {
        if (Interlocked.CompareExchange(ref _state, 1, 0) is 0)
            return;
        EnterSpin();
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    void EnterSpin()
    {
        var spin = new SpinWait();
        do
        {
            spin.SpinOnce();
        }
        while (Interlocked.CompareExchange(ref _state, 1, 0) is not 0);
    }

    // Release. Volatile.Write publishes the prior in-section stores before the lock reads free. A plain
    // store (not Interlocked) is sufficient: the holder is the sole writer of 1->0.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Exit() => Volatile.Write(ref _state, 0);

    // Scoped acquire for `using` sites. [UnscopedRef] lets the struct member hand out a ref to its own
    // storage; safe because the lock lives in a long-lived field, never a temporary. Do NOT call on a
    // by-value copy of the lock - the Scope would release a different instance than the section guards.
    [UnscopedRef]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Scope EnterScope()
    {
        Enter();
        return new Scope(ref this);
    }

    public readonly ref struct Scope
    {
        readonly ref SpinLock _lock;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Scope(ref SpinLock @lock)
        {
            _lock = ref @lock;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => _lock.Exit();
    }
}
