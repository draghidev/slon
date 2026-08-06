namespace Slon.Pg.Protocol;

/// <summary>
/// Indicates that this operation failed because an action triggered by another operation reached it.
/// The inner exception preserves the concrete protocol or PostgreSQL failure.
/// </summary>
public sealed class PgCollateralException : PgClientException
{
    internal PgCollateralException(PgCollateralKind kind, Exception cause)
        : base(BuildMessage(kind), cause)
        => Kind = kind;

    public PgCollateralKind Kind { get; }

    internal static PgCollateralException ForProtocolFailure(Exception? cause)
        => cause as PgCollateralException
            ?? new(PgCollateralKind.ProtocolFailure,
                cause ?? new InvalidOperationException("The protocol was condemned without a specific cause."));

    static string BuildMessage(PgCollateralKind kind) => kind switch
    {
        PgCollateralKind.ProtocolFailure =>
            "This operation failed collaterally because another operation caused the PostgreSQL protocol to be condemned.",
        PgCollateralKind.Cancellation =>
            "This operation was canceled collaterally by a PostgreSQL CancelRequest intended for an earlier pipelined operation. " +
            "PostgreSQL applies the request to whichever operation is running when it is processed, so drivers cannot eliminate this race.",
        _ => throw ThrowHelper.ThrowUnhandledCase(kind)
    };
}

public enum PgCollateralKind : byte
{
    ProtocolFailure,
    Cancellation
}
