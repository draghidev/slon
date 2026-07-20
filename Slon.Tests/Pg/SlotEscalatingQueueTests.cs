using Slon.Pg.Protocol;

namespace Slon.Tests.Pg;

// Direct tests for the source's slot-with-lazy-SPSC-escalation storage, with no protocol, source
// shell, or wire - just the data structure. The deterministic cases pin FIFO + the footprint
// guarantee (sequential traffic never allocates the queue) + the escalation transition; the stress
// case is a barrier-synced single-producer / single-consumer race that hammers the boundary the
// design hinges on: the producer escalating (publishing the queue, LEAVING the head in the slot)
// while the consumer concurrently takes that head. A lost item surfaces as a consumer timeout; a
// reorder or torn dequeue surfaces as a wrong-identity / out-of-position assert.
[TestClass]
[DoNotParallelize]
public class SlotEscalatingQueueTests
{
    // Pure in-memory data-structure exercise (no protocol / threadpool / waits), genuinely O(1)/iter and
    // linear at any scale, so a high cap. SLON_UNCAPPED=1 lifts it entirely.
    static int StressIterations => StressEnv.Iterations(fallback: 20_000, cap: 200_000);

    [TestMethod]
    public void Empty_DequeueAndPeek_False()
    {
        var q = new SlotEscalatingQueue<string>();
        Assert.IsFalse(q.TryDequeue(out var d));
        Assert.IsNull(d);
        Assert.IsFalse(q.TryPeek(out var p));
        Assert.IsNull(p);
        Assert.IsFalse(q.IsEscalated);
    }

    [TestMethod]
    public void Sequential_NeverEscalates_Fifo()
    {
        var q = new SlotEscalatingQueue<string>();
        for (int i = 0; i < 100; i++)
        {
            var s = i.ToString();
            q.Enqueue(s);
            Assert.IsFalse(q.IsEscalated, $"sequential enqueue/dequeue must reuse the slot, not escalate (i={i})");
            Assert.IsTrue(q.TryDequeue(out var d));
            Assert.AreEqual(s, d);
            Assert.IsFalse(q.TryDequeue(out _));
        }
        Assert.IsFalse(q.IsEscalated);
    }

    [TestMethod]
    public void Overlap_Escalates_AndPreservesFifo()
    {
        var q = new SlotEscalatingQueue<string>();
        q.Enqueue("a");                            // slot
        Assert.IsFalse(q.IsEscalated);
        q.Enqueue("b");                            // overlap -> escalate
        Assert.IsTrue(q.IsEscalated);
        Assert.IsTrue(q.TryDequeue(out var d0));
        Assert.AreEqual("a", d0);                  // head, still from the slot
        Assert.IsTrue(q.TryDequeue(out var d1));
        Assert.AreEqual("b", d1);                  // from the queue (consumer now latched)
        Assert.IsFalse(q.TryDequeue(out _));
    }

    [TestMethod]
    public void Escalation_DrainsHeadThenQueue_Fifo()
    {
        var q = new SlotEscalatingQueue<string>();
        foreach (var s in new[] { "a", "b", "c", "d" })   // a -> slot, b,c,d -> queue
            q.Enqueue(s);
        Assert.IsTrue(q.IsEscalated);
        var outp = new List<string>();
        while (q.TryDequeue(out var d))
            outp.Add(d);
        CollectionAssert.AreEqual(new[] { "a", "b", "c", "d" }, outp);
    }

    [TestMethod]
    public void PostEscalation_ContinuesFifo_AcrossInterleavedEnqueues()
    {
        var q = new SlotEscalatingQueue<string>();
        q.Enqueue("a");
        q.Enqueue("b");                            // escalate
        Assert.IsTrue(q.TryDequeue(out var d0));
        Assert.AreEqual("a", d0);                  // head from slot
        q.Enqueue("c");                            // post-escalation enqueue -> queue
        Assert.IsTrue(q.TryDequeue(out var d1));
        Assert.AreEqual("b", d1);                  // consumer latches here
        q.Enqueue("d");
        Assert.IsTrue(q.TryDequeue(out var d2));
        Assert.AreEqual("c", d2);
        Assert.IsTrue(q.TryDequeue(out var d3));
        Assert.AreEqual("d", d3);
        Assert.IsFalse(q.TryDequeue(out _));
    }

    [TestMethod]
    public void TryPeek_SlotThenQueue_DoesNotConsume()
    {
        var q = new SlotEscalatingQueue<string>();
        q.Enqueue("a");
        q.Enqueue("b");                            // a slot, b queue (escalated)
        Assert.IsTrue(q.TryPeek(out var p0));
        Assert.AreEqual("a", p0);                  // not latched: peeks the slot head
        Assert.IsTrue(q.TryPeek(out var p0b));
        Assert.AreEqual("a", p0b);                 // still there (non-consuming)
        Assert.IsTrue(q.TryDequeue(out var d0));
        Assert.AreEqual("a", d0);
        Assert.IsTrue(q.TryPeek(out var p1));
        Assert.AreEqual("b", p1);                  // slot empty, not yet latched: peeks the queue
        Assert.IsTrue(q.TryDequeue(out var d1));
        Assert.AreEqual("b", d1);                  // latches
        Assert.IsFalse(q.TryPeek(out _));          // latched + empty
    }

    [TestMethod]
    public void Enumerator_WalksSlotThenQueue_WithoutConsuming()
    {
        var q = new SlotEscalatingQueue<string>();
        q.Enqueue("a");
        q.Enqueue("b");
        q.Enqueue("c");

        var got = new List<string>();
        var enumerator = q.GetEnumerator();
        while (enumerator.MoveNext())
            got.Add(enumerator.Current);

        CollectionAssert.AreEqual(new[] { "a", "b", "c" }, got);
        Assert.IsTrue(q.TryDequeue(out var a));
        Assert.AreEqual("a", a);
        Assert.IsTrue(q.TryDequeue(out var b));
        Assert.AreEqual("b", b);
        Assert.IsTrue(q.TryDequeue(out var c));
        Assert.AreEqual("c", c);
    }

    [TestMethod]
    public void DrainInert_SlotOnly()
    {
        var q = new SlotEscalatingQueue<string>();
        q.Enqueue("a");
        var got = new List<string>();
        q.DrainInert(got.Add);
        CollectionAssert.AreEqual(new[] { "a" }, got);
    }

    [TestMethod]
    public void DrainInert_HeadInSlotPlusQueue_Fifo()
    {
        var q = new SlotEscalatingQueue<string>();
        q.Enqueue("a");
        q.Enqueue("b");
        q.Enqueue("c");                            // a slot, b,c queue; head never consumed
        var got = new List<string>();
        q.DrainInert(got.Add);
        CollectionAssert.AreEqual(new[] { "a", "b", "c" }, got);   // slot head then queue, in order
    }

    [TestMethod]
    public void DrainInert_AfterHeadConsumed_QueueResidual()
    {
        var q = new SlotEscalatingQueue<string>();
        q.Enqueue("a");
        q.Enqueue("b");
        q.Enqueue("c");
        Assert.IsTrue(q.TryDequeue(out var d));
        Assert.AreEqual("a", d);                   // consume the head from the slot
        var got = new List<string>();
        q.DrainInert(got.Add);
        CollectionAssert.AreEqual(new[] { "b", "c" }, got);
    }

    sealed class Box
    {
        public SlotEscalatingQueue<object> Q;
    }

    [TestMethod]
    public void Stress_Spsc_ExactlyOnceFifo()
    {
        var iters = StressIterations;
        const int MaxN = 4;
        var items = new object[MaxN];
        var index = new Dictionary<object, int>(ReferenceEqualityComparer.Instance);

        for (int it = 0; it < iters; it++)
        {
            // Cycle N over 1..MaxN: N=1 is pure slot (must never escalate), N=2 is the tightest
            // boundary, N>=3 forces the queue. Fresh struct (slot/queue/latches) each iteration.
            var n = 1 + (it % MaxN);
            var box = new Box();
            index.Clear();
            for (int k = 0; k < n; k++)
            {
                items[k] = new object();
                index[items[k]] = k;
            }

            var collected = new List<object>(n);

            // Consumer on Task.Run, racing the producer at the slot/escalation boundary: it starts
            // first and hot-spins on the empty struct, so it sits at the slot when the producer's first
            // Enqueue lands. The Wait is the hang net - a genuine lost item (a regression of the
            // stranded-head latch race, where the consumer latches queue-only while the head is still in
            // the slot) leaves the consumer unable to collect N, and we dump what is stuck. That race,
            // not pool load, was the earlier intermittent timeout.
            var stop = new bool[1];
            var consumer = Task.Run(() =>
            {
                while (!Volatile.Read(ref stop[0]) && collected.Count < n)
                    if (box.Q.TryDequeue(out var x))
                        collected.Add(x);
            });

            for (int k = 0; k < n; k++)   // producer = this thread
                box.Q.Enqueue(items[k]);

            if (!consumer.Wait(TimeSpan.FromSeconds(10)))
            {
                // Stop the consumer so we are the sole consumer, then inspect what's stuck.
                Volatile.Write(ref stop[0], true);
                consumer.Wait(TimeSpan.FromSeconds(2));
                var got = string.Join(",", collected.Select(o => index.TryGetValue(o, out var p) ? p.ToString() : "?"));
                var residual = new List<object>();
                box.Q.DrainInert(residual.Add);
                var stuck = string.Join(",", residual.Select(o => index.TryGetValue(o, out var p) ? p.ToString() : "?"));
                Assert.Fail($"iter {it} (N={n}): consumer collected {collected.Count}/{n} - LOST item. escalated={box.Q.IsEscalated} collectedIdx=[{got}] residualInQueue=[{stuck}]");
            }
            Assert.AreEqual(n, collected.Count, $"iter {it} (N={n}): wrong count");
            for (int k = 0; k < n; k++)
                Assert.AreEqual(k, index[collected[k]], $"iter {it} (N={n}): position {k} out of FIFO order or wrong identity");
        }
    }
}
