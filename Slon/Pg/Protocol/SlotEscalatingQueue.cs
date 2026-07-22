using System.Diagnostics.CodeAnalysis;
using Draghi.Pipelining.Internal;

// Composes Draghi.Pipelining.Internal SPSC primitives. The Experimental tag is deliberate.
namespace Slon.Pg.Protocol;

/// Single-producer, single-consumer storage with an allocation-free slot that permanently escalates
/// to an SPSC queue on the first overlap. The existing head remains in the slot; only later items enter
/// the queue.
///
/// Producer and consumer latch escalation independently. The consumer may stop checking the slot only
/// after observing the queue and then finding the slot empty: observing the queue acquires the earlier
/// slot publication, closing the race where both are published between its first slot and queue reads.
///
/// Exactly one thread may produce and one may consume. DrainInert requires the producer to have stopped.
/// This mutable struct must be held and accessed by reference.
struct SlotEscalatingQueue<T> where T : class
{
    T? _slot;
    SingleProducerSingleConsumerQueue<T>? _queue;
    bool _producerEscalated;
    bool _consumerEscalated;

    /// True once escalated to the queue tier. Stable after first true.
    public bool IsEscalated => Volatile.Read(ref _queue) is not null;

    /// Best-effort gauge: items held (slot occupancy + escalated-queue length). Lock-free, may be
    /// stale - a telemetry/backlog read, not a synchronization primitive.
    public int Count
    {
        get
        {
            var n = Volatile.Read(ref _slot) is null ? 0 : 1;
            var queue = Volatile.Read(ref _queue);
            return queue is null ? n : n + queue.Count;
        }
    }

    public Enumerator GetEnumerator() => new(ref this);

    public struct Enumerator
    {
        readonly T? _slot;
        SingleProducerSingleConsumerQueue<T>.Enumerator _queue;
        bool _yieldSlot;

        internal Enumerator(ref SlotEscalatingQueue<T> storage)
        {
            // Acquiring the queue also observes the earlier slot publication. Missing a later
            // escalation is a valid snapshot.
            var queue = Volatile.Read(ref storage._queue);
            _slot = Volatile.Read(ref storage._slot);
            _queue = queue is null ? default : queue.GetEnumerator();
            _yieldSlot = _slot is not null;
        }

        public T Current { get; private set; } = default!;

        public bool MoveNext()
        {
            if (_yieldSlot)
            {
                _yieldSlot = false;
                Current = _slot!;
                return true;
            }
            if (_queue.MoveNext())
            {
                Current = _queue.Current;
                return true;
            }
            return false;
        }
    }

    /// Enqueues from the sole producer, escalating when the slot is occupied.
    public void Enqueue(T item)
    {
        if (_producerEscalated)
        {
            _queue!.Enqueue(item);
            return;
        }
        if (Volatile.Read(ref _slot) is null)
        {
            Volatile.Write(ref _slot, item);
            return;
        }
        var queue = new SingleProducerSingleConsumerQueue<T>();
        queue.Enqueue(item);
        Volatile.Write(ref _queue, queue);
        _producerEscalated = true;
    }

    /// Takes the next item from the sole consumer.
    public bool TryDequeue([MaybeNullWhen(false)] out T item)
    {
        if (!_consumerEscalated)
        {
            var slot = Volatile.Read(ref _slot);
            if (slot is null)
            {
                if (Volatile.Read(ref _queue) is null)
                {
                    item = null;
                    return false;
                }
                // The queue acquire observes the earlier head publication. An empty re-read therefore
                // proves that the slot has drained and permits queue-only consumption.
                slot = Volatile.Read(ref _slot);
                if (slot is null)
                    _consumerEscalated = true;
            }
            if (slot is not null)
            {
                Volatile.Write(ref _slot, null);
                item = slot;
                return true;
            }
        }
        return _queue!.TryDequeue(out item);
    }

    /// Peeks from the sole consumer without changing its escalation latch.
    public bool TryPeek([MaybeNullWhen(false)] out T item)
    {
        if (_consumerEscalated)
            return _queue!.TryPeek(out item);
        var slot = Volatile.Read(ref _slot);
        if (slot is null)
        {
            var queue = Volatile.Read(ref _queue);
            if (queue is null)
            {
                item = null;
                return false;
            }
            // Preserve FIFO when the head and queue were published across the first slot read.
            slot = Volatile.Read(ref _slot);
            if (slot is null)
                return queue.TryPeek(out item);
        }
        item = slot;
        return true;
    }

    /// Drains the slot and then the queue after the producer has stopped.
    public void DrainInert(Action<T> onInert)
    {
        var slot = Volatile.Read(ref _slot);
        if (slot is not null)
        {
            Volatile.Write(ref _slot, null);
            onInert(slot);
        }
        var queue = Volatile.Read(ref _queue);
        if (queue is not null)
            while (queue.TryDequeue(out var item))
                onInert(item);
    }
}
