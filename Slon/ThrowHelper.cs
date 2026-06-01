using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace Slon;

public class ThrowHelper
{
    [DoesNotReturn]
    [StackTraceHidden]
    public static InvalidDataException ThrowNotEnoughData(string? dataDescription = null) 
        => throw new InvalidDataException($"Not enough {(dataDescription is null ? "data": $"{dataDescription} bytes")}received.");

    [DoesNotReturn]
    [StackTraceHidden]
    public static InvalidDataException ThrowUnexpectedData(string? dataDescription = null, object? dataDescriptionArgument = null)
        => throw new InvalidDataException($"Unexpected {(dataDescription is null ? "data" : string.Format(dataDescription, dataDescriptionArgument))}. Protocol synchronization could have been lost.");

    [DoesNotReturn]
    [StackTraceHidden]
    public static InvalidOperationException ThrowArgumentException(string parameter, string? message = null)
        => throw new ArgumentException(message, parameter);

    [DoesNotReturn]
    [StackTraceHidden]
    public static InvalidOperationException ThrowInvalidOperation(string? message = null)
        => throw new InvalidOperationException(message);

    [DoesNotReturn]
    [StackTraceHidden]
    public static OperationCanceledException ThrowOperationCanceled(CancellationToken cancellationToken)
        => throw new OperationCanceledException(cancellationToken);

    [DoesNotReturn]
    [StackTraceHidden]
    public static UnreachableException ThrowUnhandledCase(object value)
        => throw new UnreachableException("Unhandled case: " + value);

    [DoesNotReturn]
    [StackTraceHidden]
    public static UnreachableException ThrowUnexpected(string message)
        => throw new UnreachableException(message);

    [DoesNotReturn]
    [StackTraceHidden]
    public static NotSupportedException ThrowNotSupported(string message)
        => throw new NotSupportedException(message);

}
