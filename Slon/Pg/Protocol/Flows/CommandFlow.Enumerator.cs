using System.Collections;
using System.Threading.Tasks.Sources;

namespace Slon.Pg.Protocol.Flows;

sealed partial class CommandFlow
{
    Slon.Threading.Tasks.Sources.ManualResetValueTaskSourceCore<bool> _enumeratorMoveNextTaskSource;
    // Serializes move-next rearming against body termination. Otherwise Reset can replace the generation
    // just before terminal completion and strand the consumer (see MoveNextRearm.tla). Never hold it while
    // dispatching the gate, which may run the body inline.
    Slon.Threading.SpinLock _rearmLock;
    CommandResult<ResultMessageEnumerator>? _enumeratorCurrent;
    bool _enumeratorCompleted;
    bool _isResultReady;
    // Consumer-thread-only. The first call uses the initial source generation; later calls rearm it.
    // Body start is not a substitute because an executor-driven body may finish before first consumption.
    bool _consumerAdvanced;

    bool IsEnumerationCompleted => Volatile.Read(ref _enumeratorCompleted);
    void PublishEnumerationCompleted() => Volatile.Write(ref _enumeratorCompleted, true);

    ValueTask<bool> EnumeratorMoveNextTask => new(this, _enumeratorMoveNextTaskSource.Version);

    bool IValueTaskSource<bool>.GetResult(short token) => _enumeratorMoveNextTaskSource.GetResult(token);
    ValueTaskSourceStatus IValueTaskSource<bool>.GetStatus(short token) => _enumeratorMoveNextTaskSource.GetStatus(token);
    void IValueTaskSource<bool>.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => _enumeratorMoveNextTaskSource.OnCompleted(continuation, state, token, flags);

    // Consumer completion must dispatch asynchronously because it may run while the pipeline still owns
    // the current execution frame.
    void CompleteEnumeration()
    {
        // Drain errors outrank cancellation and clean completion. Preserve every error across a batch.
        if (_drainErrors is { Count: > 0 } errors)
        {
            Exception fault = errors.Count == 1 ? errors[0] : new AggregateException(errors);
            _enumeratorMoveNextTaskSource.TrySetException(fault, runContinuationsAsynchronously: true);
        }
        else if (_deliverCancelOce && !_consumerDisposed)
            _enumeratorMoveNextTaskSource.TrySetException(new OperationCanceledException(_cancelDeliverToken), runContinuationsAsynchronously: true);
        else
            _enumeratorMoveNextTaskSource.TrySetResult(false, runContinuationsAsynchronously: true);
        // _enumeratorCompleted was set by the caller (SetResult's completed branch) before this runs.
        SignalPumpProgress();
    }

    // Cancellation stops waiting; it does not stop the autonomous drain or escape from disposal.
    async ValueTask AwaitDrainOnDispose()
    {
        try
        {
            await WaitForComplete(_flowCancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (_flowCancellationToken.IsCancellationRequested)
        {
            // Caller cancelled the wait; unwind. The body drains autonomously in the background.
        }
        catch (PgClientClosedException)
        {
            // The flow completed via a protocol close; that is a clean terminal for a disposing consumer.
            return;
        }
        // Flow completion is independent of errors accumulated while draining.
        if (_drainErrors is { Count: > 0 } errors)
            throw errors.Count == 1 ? errors[0] : new AggregateException(errors);
    }

    void AwaitDrainOnDisposeSynchronously()
    {
        try
        {
            WaitForCompleteSynchronously(_flowCancellationToken);
        }
        catch (OperationCanceledException) when (_flowCancellationToken.IsCancellationRequested)
        {
            // Caller cancelled the wait; the body continues draining autonomously.
        }
        catch (PgClientClosedException)
        {
            return;
        }

        if (_drainErrors is { Count: > 0 } errors)
            throw errors.Count == 1 ? errors[0] : new AggregateException(errors);
    }

    void CompleteEnumerationWithClose(Exception closeException)
    {
        // A result may already own this generation. Preserve the close in the latch, but only publish
        // terminal enumeration state when the close wins the generation; otherwise the consumer must
        // observe the result once, rearm, and self-deliver the latched close on its next move.
        _callerInteractionCore.SetCloseLatch(closeException);
        if (_enumeratorMoveNextTaskSource.TrySetException(closeException, runContinuationsAsynchronously: true))
            PublishEnumerationCompleted();
        SignalPumpProgress();
    }

    // Consumer terminality can precede body terminality under shutdown. Transfer a live body to the
    // autonomous driver without re-entering MoveNext: its gate may already be faulted while its handoff
    // continuation is pending.
    bool TransferLiveBodyToDrain()
    {
        if (IsBodyTerminated)
            return false;
        MarkTerminalConsumerGone();
        _callerInteractionCore.ResumeBody(runContinuationsAsynchronously: true);
        _callerInteractionCore.WakeBody();
        return true;
    }

    // Terminal state outlives task-source generations. Close takes precedence over caller cancellation;
    // absent either, a terminal enumeration completes cleanly.
    void EnsureEnumerationCompleted()
    {
        if (_enumeratorMoveNextTaskSource.GetStatus(_enumeratorMoveNextTaskSource.Version) is not ValueTaskSourceStatus.Pending)
            return;
        if (_callerInteractionCore.CloseException is { } latched)
            _enumeratorMoveNextTaskSource.TrySetException(latched, runContinuationsAsynchronously: true);
        else if (Volatile.Read(ref _cancelRequested) || EffectiveCancellationToken.IsCancellationRequested)
            _enumeratorMoveNextTaskSource.TrySetException(
                new OperationCanceledException(EffectiveCancellationToken.IsCancellationRequested ? EffectiveCancellationToken : _cancelDeliverToken),
                runContinuationsAsynchronously: true);
        // Rearming after a clean terminal still needs to complete the new generation.
        else if (IsEnumerationCompleted)
            _enumeratorMoveNextTaskSource.TrySetResult(false, runContinuationsAsynchronously: true);
    }

    public readonly struct Enumerator(CommandFlow flow) : IEnumerator<CommandResult>, IAsyncEnumerator<CommandResult>
    {
        // Here so we can pass the cancellation token and enumerate without boxing the struct (which WithCancellation must do).
        /// <inheritdoc cref="IAsyncEnumerable{T}.GetAsyncEnumerator" />
        public Enumerator GetAsyncEnumerator() => this;

        // Dispose always calls MoveNext to confirm the enumerator is done without tracking additional state.
        // So this method should be resilient to multiple fetches of *at least* the final result.
        /// <inheritdoc />
        public bool MoveNext()
        {
            if (flow is null)
                return false;

            // Queueing only established FIFO position. This is the first point at which the caller
            // is ready to take the source pump and drive the synchronous body.
            if (!flow._consumerAdvanced && !flow.IsAsyncAtDispatch)
                flow.WaitForSyncHandoff();

            var takeOverAsyncGate = false;

            // Guard-decide-rearm serialized against the body's terminal (see _rearmLock). The using scope
            // ends before the WaitForContinuation drive below (which runs the body inline and would
            // re-enter this non-reentrant lock); try/finally release keeps it lock-safe on any throw.
            using (flow._rearmLock.EnterScope())
            {
                // See MoveNextAsync: terminal enumeration state can outlive a completed source generation.
                // Ensure the current generation carries that terminal before returning it.
                if (flow.IsEnumerationCompleted)
                {
                    flow.EnsureEnumerationCompleted();
                    return flow.EnumeratorMoveNextTask.Result;
                }

                if (flow.IsAsync)
                {
                    if (flow._enumeratorCurrent is null)
                        ThrowHelper.ThrowInvalidOperation("No immediate sync/async mixing is allowed, the first MoveNext{Async} call has to match the async argument passed during initialize.");
                    flow.IsAsync = false;
                    takeOverAsyncGate = true;
                }

                // See MoveNextAsync: rearm only on a non-first call; the first-call source is fresh and the
                // body's first delivery lands on it.
                if (flow._consumerAdvanced)
                    flow._enumeratorMoveNextTaskSource.Reset();
                flow._consumerAdvanced = true;
            }
            // Close-latch self-deliver (sync): under close this call completes the generation it just
            // armed, on its own thread.
            if (flow._callerInteractionCore.CloseException is { } syncClosed)
            {
                flow.CompleteEnumerationWithClose(syncClosed);
                return flow.EnumeratorMoveNextTask.Result;
            }
            // The body may already be parked on the async inter-result gate. Once this caller changes
            // the flow to synchronous driving, open that gate inline so the body can hand its continuation
            // to the rendezvous below. The edge also covers an in-flight body that has not parked yet.
            if (takeOverAsyncGate)
            {
                flow._callerInteractionCore.ResumeBody(runContinuationsAsynchronously: false);
                var delivered = flow.EnumeratorMoveNextTask;
                if (delivered.IsCompleted)
                    return delivered.Result;
            }
            // A progress wake may precede both result completion and publication of the body's next
            // handoff continuation (close faults the gate before a resumed body reaches YieldToCaller).
            // Keep rendezvousing until either the result owns this turn or there is body work to drive.
            while (true)
            {
                var continuation = flow._callerInteractionCore.WaitForContinuation();
                var task = flow.EnumeratorMoveNextTask;
                if (task.IsCompleted)
                {
                    if (continuation is not null)
                        flow._callerInteractionCore.DeferContinuation(continuation);
                    return task.Result;
                }
                continuation ??= flow._callerInteractionCore.TryTakePendingContinuation();
                if (continuation is null)
                    continue;
                continuation.Invoke();
            }
        }

        // DisposeAsync always calls MoveNextAsync to confirm the enumerator is done without tracking additional state.
        // So this method should be resilient to multiple fetches of the final result.
        /// <inheritdoc />
        public ValueTask<bool> MoveNextAsync() => MoveNextAsync(default);

        /// <summary>Advances the enumerator asynchronously to the next element of the collection.</summary>
        /// <param name="cancellationToken">A <see cref="T:System.Threading.CancellationToken" /> that may be used to cancel the asynchronous operation.</param>
        /// <returns>A <see cref="T:System.Threading.Tasks.ValueTask`1" /> that will complete with a result of <see langword="true" /> if the enumerator was successfully advanced to the next element, or <see langword="false" /> if the enumerator has passed the end of the collection.</returns>
        public ValueTask<bool> MoveNextAsync(CancellationToken cancellationToken)
        {
            if (flow is null)
                return new(false);

            if (cancellationToken.IsCancellationRequested)
            {
                flow._callerCancellationToken = cancellationToken;
                if (flow.RequestCancel(cancellationToken, CancellationScope.CurrentWindow))
                {
                    flow._callerInteractionCore.ResumeBody(runContinuationsAsynchronously: false);
                    flow._callerInteractionCore.WakeBody();
                }
                return ValueTask.FromException<bool>(new OperationCanceledException(cancellationToken));
            }

            // The guard-decide-rearm is serialized against the body's terminal (see _rearmLock): the using
            // scope covers the _enumeratorCompleted read through the move-next Reset, and ends before the
            // gate dispatch below (which runs the body inline and would re-enter this non-reentrant lock).
            // try/finally release keeps it lock-safe on any throw.
            using (flow._rearmLock.EnterScope())
            {
                // Publish the per-read token before the terminal repair below: a flow may already have
                // completed before its first consumer call, and EnsureEnumerationCompleted needs this token to
                // distinguish a pre-fired cancellation from a clean end.
                if (cancellationToken.CanBeCanceled)
                {
                    flow._callerCancellationToken = cancellationToken;
                    flow._enumeratorMoveNextTaskSource.CanCompleteConcurrently = true;
                }

                // Terminal enumeration state may outlive its completed source generation. Complete the
                // newly armed generation with the same close, cancellation, or clean-end outcome.
                if (flow.IsEnumerationCompleted)
                {
                    flow.EnsureEnumerationCompleted();
                    return flow.EnumeratorMoveNextTask;
                }

                if (!flow.IsAsync)
                {
                    if (flow._enumeratorCurrent is null)
                        ThrowHelper.ThrowInvalidOperation("No immediate sync/async mixing is allowed, the first MoveNext{Async} call has to match the async argument passed during initialize.");
                    flow.IsAsync = true;
                }

                // The first delivery targets the initial generation. After consumer disposal, keep the
                // current generation for the body's one-shot terminal. Body-initiated drain retains a live
                // consumer and therefore continues rearming.
                if (flow._consumerAdvanced && !Volatile.Read(ref flow._consumerDisposed))
                    flow._enumeratorMoveNextTaskSource.Reset();
                flow._consumerAdvanced = true;
            }
            // Drive the body; teardown may already have faulted the gate.
            flow._callerInteractionCore.ResumeBody(runContinuationsAsynchronously: false);
            // Read the close latch after Reset and complete the generation just armed.
            if (flow._callerInteractionCore.CloseException is { } closed)
                flow.CompleteEnumerationWithClose(closed);
            return flow.EnumeratorMoveNextTask;
        }

        public CommandResult Current => flow?._enumeratorCurrent ?? default!;

        /// <inheritdoc />
        public void Dispose()
        {
            if (flow is null)
                return;

            // Consumer terminality can precede body terminality under shutdown. A terminal consumer must
            // still transfer a live body to autonomous drain, but must not re-enter the ordinary MoveNext
            // pump: its gate may already be faulted while its handoff continuation is pending.
            if (flow.IsEnumerationCompleted)
            {
                if (flow.TransferLiveBodyToDrain() && flow.WaitForDrainOnDispose)
                    flow.AwaitDrainOnDisposeSynchronously();
                return;
            }

            // Synchronous disposal takes over an async body through a two-way rendezvous. A gate-parked
            // body resumes inline; an in-flight body later hands over its continuation. Waiting on this
            // rendezvous rather than MoveNext avoids blocking on the task the body itself must complete.
            if (flow.IsAsync)
            {
                flow.IsAsync = false;
                if (flow.WaitForDrainOnDispose)
                    flow.MarkConsumerWaitForDrain();
                else
                    flow.MarkConsumerGone();
                // Resume a gate-parked body inline; otherwise buffer the edge.
                flow._callerInteractionCore.ResumeBody(runContinuationsAsynchronously: false);
                if (flow.WaitForDrainOnDispose)
                {
                    DriveBodyToTermination(flow);
                    // Drain ran on this thread; surface accumulated drain errors (completes immediately).
                    flow.AwaitDrainOnDisposeSynchronously();
                }
                else
                {
                    // Fast-return: wake the body to drain autonomously in the background, then return.
                    flow._callerInteractionCore.WakeBody();
                }
                return;
            }

            // Cancellation graduates a synchronous body to the dedicated driver. The disposer no
            // longer pumps the same body concurrently; it only waits for tenure release when requested.
            flow.MarkSyncConsumerGone();
            if (flow.WaitForDrainOnDispose)
                flow.AwaitDrainOnDisposeSynchronously();

            static void DriveBodyToTermination(CommandFlow flow)
            {
                while (!flow.IsBodyTerminated)
                {
                    var continuation = flow._callerInteractionCore.WaitForContinuation();
                    if (continuation is null)
                    {
                        if (flow.IsBodyTerminated)
                            break;
                        continuation = flow._callerInteractionCore.TryTakePendingContinuation();
                        if (continuation is null)
                            continue;
                    }
                    continuation.Invoke();
                }
            }
        }

        /// <inheritdoc />
        public ValueTask DisposeAsync()
        {
            if (flow is null)
                return new();

            if (flow.IsEnumerationCompleted)
            {
                // A completed awaited drain may still have errors to surface.
                flow.TransferLiveBodyToDrain();
                return flow.WaitForDrainOnDispose ? flow.AwaitDrainOnDispose() : new();
            }

            // Mark autonomous drain, open the async result gate, and wake any synchronous rendezvous.
            // Pipeline tenure keeps successors behind the body until it reaches RFQ.
            if (flow.WaitForDrainOnDispose) flow.MarkConsumerWaitForDrain(); else flow.MarkConsumerGone();
            flow._callerInteractionCore.ResumeBody(runContinuationsAsynchronously: false);
            flow._callerInteractionCore.WakeBody();
            // Optionally await the body's bounded completion; cancellation stops waiting, not draining.
            if (flow.WaitForDrainOnDispose)
                return flow.AwaitDrainOnDispose();
            return new();
        }

        /// <inheritdoc />
        void IEnumerator.Reset() => throw new NotSupportedException();
        /// <inheritdoc />
        object? IEnumerator.Current => Current;
    }

}
