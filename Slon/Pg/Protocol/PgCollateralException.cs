namespace Slon.Pg.Protocol;

/// <summary>
/// Indicates that this operation failed because a shared-wire event not owned by it reached it.
/// The inner exception preserves the concrete protocol or PostgreSQL failure.
/// </summary>
public sealed class PgCollateralException : PgClientException
{
    internal PgCollateralException(PgCollateralSource collateralSource, Exception cause)
        : base(BuildMessage(collateralSource), cause)
        => CollateralSource = collateralSource;

    /// <summary>Identifies the shared-wire event that caused this operation to fail.</summary>
    public PgCollateralSource CollateralSource { get; }

    internal static PgCollateralException ForProtocolFailure(Exception? cause)
        => cause as PgCollateralException
            ?? new(PgCollateralSource.ProtocolFailure,
                cause ?? new InvalidOperationException("The protocol was condemned without a specific cause."));

    static string BuildMessage(PgCollateralSource collateralSource) => collateralSource switch
    {
        PgCollateralSource.ProtocolFailure =>
            "This operation failed collaterally because another operation caused the PostgreSQL protocol to be condemned.",
        PgCollateralSource.Cancellation =>
            "This operation was canceled collaterally by a PostgreSQL CancelRequest intended for an earlier pipelined operation. " +
            "PostgreSQL applies the request to whichever operation is running when it is processed, so drivers cannot eliminate this race.",
        PgCollateralSource.BackendTermination =>
            "This operation failed collaterally because PostgreSQL terminated the shared session.",
        _ => throw ThrowHelper.ThrowUnhandledCase(collateralSource)
    };
}

/// <summary>Identifies the source of a collateral PostgreSQL operation failure.</summary>
public enum PgCollateralSource : byte
{
    /// <summary>Another client-side failure condemned the shared protocol.</summary>
    ProtocolFailure,
    /// <summary>A PostgreSQL CancelRequest reached an operation other than its intended target.</summary>
    Cancellation,
    /// <summary>PostgreSQL terminated the shared backend session.</summary>
    BackendTermination
}
