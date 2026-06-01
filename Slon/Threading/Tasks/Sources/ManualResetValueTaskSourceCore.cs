using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks.Sources;

namespace Slon.Threading.Tasks.Sources;

// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// Tweaks by Nino Floris, all rights reserved.

/// <summary>Provides the core logic for implementing a manual-reset <see cref="IValueTaskSource"/> or <see cref="IValueTaskSource{TResult}"/>.</summary>
/// <typeparam name="TResult">Specifies the type of results of the operation represented by this instance.</typeparam>
[StructLayout(LayoutKind.Auto)]
struct ManualResetValueTaskSourceCore<TResult>
{
    /// <summary>
    /// The callback to invoke when the operation completes if <see cref="OnCompleted"/> was called before the operation completed,
    /// or <see cref="ContinuationDispatchCore.CompletionSentinelAction"/> if the operation completed before a callback was supplied,
    /// or null if a callback hasn't yet been provided and the operation hasn't yet completed.
    /// </summary>
    Action<object?>? _continuation;
    /// <summary>State to pass to <see cref="_continuation"/>.</summary>
    object? _continuationState;
    /// <summary>
    /// Null if no special context was found.
    /// ExecutionContext if one was captured due to needing to be flowed.
    /// A scheduler (TaskScheduler or SynchronizationContext) if one was captured and needs to be used for callback scheduling.
    /// Or a CapturedContext if there's both an ExecutionContext and a scheduler.
    /// The most common and the fast path case to optimize for is null.
    /// </summary>
    object? _capturedContext;
    /// <summary>The exception with which the operation failed, or null if it hasn't yet completed or completed successfully.</summary>
    ExceptionDispatchInfo? _error;
    /// <summary>The result with which the operation succeeded, or the default value if it hasn't yet completed or failed.</summary>
    TResult? _result;
    /// <summary>The current version of this value, used to help prevent misuse.</summary>
    short _version;
    /// <summary>Whether the current operation has completed (this can mean it's still completing, which is why _continuation is also checked).</summary>
    bool _completed;
    /// <summary>Whether concurrent completions are handled correctly.</summary>
    bool _canCompleteConcurrently;

    /// <summary>Gets or sets whether concurrent completions are handled correctly.</summary>
    /// <remarks>Enabling this allows the use of TrySet methods and makes the Set methods thread safe.</remarks>
    public bool CanCompleteConcurrently
    {
        get => _canCompleteConcurrently;
        set => _canCompleteConcurrently = value;
    }

    /// <summary>Resets to prepare for the next operation.</summary>
    public void Reset()
    {
        // Reset/update state for the next use/await of this instance.
        _version++;
        _continuation = null;
        _continuationState = null;
        _capturedContext = null;
        _error = null;
        _result = default;
        _completed = default;
    }

    /// <summary>Completes with a successful result.</summary>
    /// <param name="result">The result.</param>
    public void SetResult(TResult result)
    {
        var canCompleteConcurrently = _canCompleteConcurrently;
        if (_completed || (canCompleteConcurrently && Interlocked.CompareExchange(ref _completed, true, false) is not false))
            ThrowInvalidOperationException();
        if (!canCompleteConcurrently)
            _completed = true;
        _result = result;
        new ContinuationDispatcher(ref  _continuation, ref _continuationState, ref _capturedContext)
            .SignalCompletion(runContinuationsAsynchronously: false);
    }

    /// <summary>Completes with a successful result.</summary>
    /// <param name="result">The result.</param>
    /// <param name="runContinuationsAsynchronously">whether to force continuations to run asynchronously this call.</param>
    public void SetResult(TResult result, bool runContinuationsAsynchronously)
    {
        var canCompleteConcurrently = _canCompleteConcurrently;
        if (_completed || (canCompleteConcurrently && Interlocked.CompareExchange(ref _completed, true, false) is not false))
            ThrowInvalidOperationException();
        if (!canCompleteConcurrently)
            _completed = true;
        _result = result;
        new ContinuationDispatcher(ref _continuation, ref _continuationState, ref _capturedContext)
            .SignalCompletion(runContinuationsAsynchronously);
    }

    /// <summary>Completes with an error.</summary>
    /// <param name="error">The exception.</param>
    public void SetException(Exception error)
    {
        var canCompleteConcurrently = _canCompleteConcurrently;
        if (_completed || (canCompleteConcurrently && Interlocked.CompareExchange(ref _completed, true, false) is not false))
            ThrowInvalidOperationException();
        if (!canCompleteConcurrently)
            _completed = true;
        _error = ExceptionDispatchInfo.Capture(error);
        new ContinuationDispatcher(ref _continuation, ref _continuationState, ref _capturedContext)
            .SignalCompletion(false);
    }

    /// <summary>Completes with an error.</summary>
    /// <param name="error">The exception.</param>
    /// <param name="runContinuationsAsynchronously">whether to force continuations to run asynchronously this call.</param>
    public void SetException(Exception error, bool runContinuationsAsynchronously)
    {
        var canCompleteConcurrently = _canCompleteConcurrently;
        if (_completed || (canCompleteConcurrently && Interlocked.CompareExchange(ref _completed, true, false) is not false))
            ThrowInvalidOperationException();
        if (!canCompleteConcurrently)
            _completed = true;
        _error = ExceptionDispatchInfo.Capture(error);
        new ContinuationDispatcher(ref _continuation, ref _continuationState, ref _capturedContext)
            .SignalCompletion(runContinuationsAsynchronously);
    }

    /// <summary>Completes with a successful result.</summary>
    /// <param name="result">The result.</param>
    public bool TrySetResult(TResult result)
    {
        if (!_canCompleteConcurrently)
            ThrowInvalidOperationException();
        if (_completed || Interlocked.CompareExchange(ref _completed, true, false) is not false)
            return false;
        _result = result;
        new ContinuationDispatcher(ref  _continuation, ref _continuationState, ref _capturedContext)
            .SignalCompletion(false);
        return true;
    }

    /// <summary>Completes with a successful result.</summary>
    /// <param name="result">The result.</param>
    /// <param name="runContinuationsAsynchronously">whether to force continuations to run asynchronously this call.</param>
    public bool TrySetResult(TResult result, bool runContinuationsAsynchronously)
    {
        if (!_canCompleteConcurrently)
            ThrowInvalidOperationException();
        if (_completed || Interlocked.CompareExchange(ref _completed, true, false) is not false)
            return false;
        _result = result;
        new ContinuationDispatcher(ref _continuation, ref _continuationState, ref _capturedContext)
            .SignalCompletion(runContinuationsAsynchronously);
        return true;
    }

    /// <summary>Completes with an error.</summary>
    /// <param name="error">The exception.</param>
    public bool TrySetException(Exception error)
    {
        if (!_canCompleteConcurrently)
            ThrowInvalidOperationException();
        if (_completed || Interlocked.CompareExchange(ref _completed, true, false) is not false)
            return false;
        _error = ExceptionDispatchInfo.Capture(error);
        new ContinuationDispatcher(ref _continuation, ref _continuationState, ref _capturedContext)
            .SignalCompletion(false);
        return true;
    }

    /// <summary>Completes with an error.</summary>
    /// <param name="error">The exception.</param>
    /// <param name="runContinuationsAsynchronously">whether to force continuations to run asynchronously this call.</param>
    public bool TrySetException(Exception error, bool runContinuationsAsynchronously)
    {
        if (!_canCompleteConcurrently)
            ThrowInvalidOperationException();
        if (_completed || Interlocked.CompareExchange(ref _completed, true, false) is not false)
            return false;
        _error = ExceptionDispatchInfo.Capture(error);
        new ContinuationDispatcher(ref _continuation, ref _continuationState, ref _capturedContext)
            .SignalCompletion(runContinuationsAsynchronously);
        return true;
    }

    /// <summary>Gets the operation version.</summary>
    public short Version => _version;

    /// <summary>Gets the status of the operation.</summary>
    /// <param name="token">Opaque value that was provided to the <see cref="ValueTask"/>'s constructor.</param>
    public ValueTaskSourceStatus GetStatus(short token)
    {
        if (token != _version)
        {
            ThrowInvalidOperationException();
        }
        return
            _continuation is null || !_completed ? ValueTaskSourceStatus.Pending :
            _error is null ? ValueTaskSourceStatus.Succeeded :
            _error.SourceException is OperationCanceledException ? ValueTaskSourceStatus.Canceled :
            ValueTaskSourceStatus.Faulted;
    }

    /// <summary>Gets the result of the operation.</summary>
    /// <param name="token">Opaque value that was provided to the <see cref="ValueTask"/>'s constructor.</param>
    [StackTraceHidden]
    public TResult GetResult(short token)
    {
        if (token != _version || !_completed || _error is not null)
        {
            ThrowForFailedGetResult();
        }

        return _result!;
    }

    /// <summary>Gets the result of the operation.</summary>
    /// <param name="token">Opaque value that was provided to the <see cref="ValueTask"/>'s constructor.</param>
    /// <param name="edi"></param>
    [StackTraceHidden]
    public TResult GetResult(short token, out ExceptionDispatchInfo? edi)
    {
        if (token != _version || !_completed || _error is not null)
        {
            edi = _error;
            return default!;
        }

        edi = null;
        return _result!;
    }

    /// <summary>Throws an exception in response to a failed <see cref="GetResult"/>.</summary>
    [StackTraceHidden]
    void ThrowForFailedGetResult()
    {
        _error?.Throw();
        throw new InvalidOperationException(); // not using ThrowHelper.ThrowInvalidOperationException so that the JIT sees ThrowForFailedGetResult as always throwing
    }

    /// <summary>Schedules the continuation action for this operation.</summary>
    /// <param name="continuation">The continuation to invoke when the operation has completed.</param>
    /// <param name="state">The state object to pass to <paramref name="continuation"/> when it's invoked.</param>
    /// <param name="token">Opaque value that was provided to the <see cref="ValueTask"/>'s constructor.</param>
    /// <param name="flags">The flags describing the behavior of the continuation.</param>
    public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
    {
        ArgumentNullException.ThrowIfNull(continuation);
        if (token != _version)
        {
            ThrowInvalidOperationException();
        }

        new ContinuationDispatcher(ref _continuation, ref _continuationState, ref _capturedContext)
            .OnCompleted(continuation, state, flags);
    }

    [DoesNotReturn]
    static void ThrowInvalidOperationException() => throw new InvalidOperationException();
}
