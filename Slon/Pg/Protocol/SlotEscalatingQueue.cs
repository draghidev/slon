using System.Diagnostics.CodeAnalysis;
using Draghi.Pipelining.Internal;

// Composes Draghi.Pipelining.Internal SPSC primitives. The Experimental tag is deliberate.
#pragma warning disable DRAGHI001

namespace Slon.Pg.Protocol;

/// Single-producer / single-consumer storage with a slot fast path and lazy one-way SPSC escalation.
/// One inline reference slot absorbs the sequential common case with no queue allocation; on the
/// FIRST overlap (a second item enqueued while the slot is still occupied) it escalates to an SPSC
/// queue, ONE-WAY - every subsequent item goes through the queue.
///
/// Escalation is binary FROM EACH SIDE'S VIEW via a side-local latch (each side is single-threaded
/// over its own ops): once a side observes escalation it goes queue-only and never reads the slot
/// again, so neither hot path perpetually re-checks the slot. The head is NOT moved at escalation -
/// it stays in the slot and the consumer drains it on its next take. The consumer latches only after
/// confirming an empty slot ORDERED AFTER it has acquired a live queue: a bare empty-slot read does
/// NOT prove the head is drained, because the producer can fill the slot (head) and escalate between
/// the consumer's slot read and its queue read, leaving the first read stale-null. The head-fill
/// release-stores the slot before escalation release-stores the queue, so a slot re-read that follows
/// the queue-acquire is guaranteed to observe a still-present head (see TryDequeue). Leaving the head
/// is what keeps the hot path Interlocked-free: the producer never claims the slot, so the consumer's
/// plain Volatile.Read + Volatile.Write take races nothing. The slot reference IS its own state
/// (null/non-null); a single atomic ref write fills it (data + publish in one).
///
/// Contract: exactly one producer thread calls Enqueue; exactly one consumer thread calls TryDequeue
/// / TryPeek; DrainInert is the consumer once the producer has stopped. As a mutable struct it must
/// live as a non-readonly field and never be copied - mutations are by ref this.
struct SlotEscalatingQueue<T> where T : class
{
    T? _slot;
    SingleProducerSingleConsumerQueue<T>? _queue;
    bool _producerEscalated;   // producer-local latch (single-threaded with Enqueue)
    bool _consumerEscalated;   // consumer-local latch (single-threaded with TryDequeue/TryPeek)

    /// True once escalated to the queue tier. Stable after first true.
    public bool IsEscalated => Volatile.Read(ref _queue) is not null;

    /// Producer (single thread). Latched: straight to the queue. Else fill the empty slot, or
    /// escalate on overlap - leaving the head in the slot (no move, no claim race), queuing only the
    /// overflow, publishing (release) so the consumer's acquire sees it, then latching.
    public void Enqueue(T item)
    {
        if (_producerEscalated)
        {
            _queue!.Enqueue(item);
            return;
        }
        if (Volatile.Read(ref _slot) is null)
        {
            Volatile.Write(ref _slot, item);   // single atomic ref write = data + publish
            return;
        }
        var queue = new SingleProducerSingleConsumerQueue<T>();
        queue.Enqueue(item);                   // overflow only; the head stays in the slot
        Volatile.Write(ref _queue, queue);
        _producerEscalated = true;
    }

    /// Consumer (single thread). Latched: queue-only, the slot is never touched again. Else the slot
    /// first (its take races nothing - the producer never claims it). A null slot does NOT prove the
    /// head is drained: the producer can fill the slot (head) and escalate BETWEEN the consumer's slot
    /// read and its queue read, so a first null read can be stale. Only latch after re-reading the slot
    /// ORDERED AFTER the queue-acquire: the head-fill release-stores _slot before escalation
    /// release-stores _queue, so a slot load that follows the queue-acquire observes the head if present.
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
                // Ordered re-read: the head may have raced the escalation into the slot between the
                // first read and now. A null here (after the queue-acquire) genuinely proves the head
                // is drained, so it is safe to latch and go queue-only.
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

    /// Consumer peek (single thread). Non-consuming, so it does not latch; the next TryDequeue does.
    /// Same stale-slot window as TryDequeue: peeking the queue while the head is still in the slot would
    /// return the wrong (out-of-FIFO) head, so re-read the slot ordered after the queue-acquire first.
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
            // Ordered re-read (see TryDequeue): the head may have raced the escalation into the slot.
            slot = Volatile.Read(ref _slot);
            if (slot is null)
                return queue.TryPeek(out item);   // head confirmed drained, peek the queue head
        }
        item = slot;
        return true;
    }

    /// Sole consumer once the producer has stopped: the head may still be in the slot (escalation
    /// leaves it), so drain slot then queue, in order. No producer races here.
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
