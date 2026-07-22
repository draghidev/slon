using System.Buffers;
using System.Text;

namespace Slon.Pg.Protocol;

readonly struct ErrorOrNoticeMessage
{
    readonly PgTypes.BackendType[] _expected;
    // A VIEW into the live message buffer by default - valid only while the error is handled inline,
    // before the next read advances the buffer. Preserve() copies it for errors that escape that window.
    readonly ReadOnlySequence<byte> _body;
    public bool IsNotice { get; }
    public bool HasPriorCancellationExposure { get; }
    /// <summary>
    /// Specifies whether the exception is considered transient, that is, whether retrying the operation could
    /// succeed (e.g. a network error). Check <see cref="SqlState"/>.
    /// </summary>
    public bool IsTransientError
    {
        get
        {
            switch (SqlState)
            {
                case PgErrorCodes.InsufficientResources:
                case PgErrorCodes.DiskFull:
                case PgErrorCodes.OutOfMemory:
                case PgErrorCodes.TooManyConnections:
                case PgErrorCodes.ConfigurationLimitExceeded:
                case PgErrorCodes.CannotConnectNow:
                case PgErrorCodes.SystemError:
                case PgErrorCodes.IoError:
                case PgErrorCodes.SerializationFailure:
                case PgErrorCodes.DeadlockDetected:
                case PgErrorCodes.LockNotAvailable:
                case PgErrorCodes.ObjectInUse:
                case PgErrorCodes.ObjectNotInPrerequisiteState:
                case PgErrorCodes.ConnectionException:
                case PgErrorCodes.ConnectionDoesNotExist:
                case PgErrorCodes.ConnectionFailure:
                case PgErrorCodes.SqlClientUnableToEstablishSqlConnection:
                case PgErrorCodes.SqlServerRejectedEstablishmentOfSqlConnection:
                case PgErrorCodes.TransactionResolutionUnknown:
                case PgErrorCodes.AdminShutdown:
                case PgErrorCodes.CrashShutdown:
                case PgErrorCodes.IdleSessionTimeout:
                    return true;
                default:
                    return false;
            }
        }
    }
    // Eagerly parsed: the hot field (transient detection, recovery decisions, and ADO catch filters
    // all read it). Captured as a string, so it stays valid even on a view-only error that was never
    // Preserve()d. The rest stay lazy.
    public string SqlState { get; }

    // Lazily decoded ErrorResponse / NoticeResponse fields. The wire body is a sequence of
    // <field-type byte><null-terminated string> pairs, terminated by a zero field-type byte; we keep
    // the raw body and decode a field only when it is read (errors are rare, most fields go unread).
    // The (byte)'x' argument is the PG field identifier.
    // https://www.postgresql.org/docs/current/protocol-error-fields.html
    public string Severity => GetAscii((byte)'S');

    public string InvariantSeverity
    {
        get
        {
            var v = GetAscii((byte)'V');
            return v.Length is 0 ? Severity : v;
        }
    }

    public string MessageText => GetText((byte)'M');
    public string? Detail => GetTextOrNull((byte)'D');
    public string? Hint => GetTextOrNull((byte)'H');
    public int Position => GetInt((byte)'P');
    public int InternalPosition => GetInt((byte)'p');
    public string? InternalQuery => GetTextOrNull((byte)'q');
    public string? Where => GetTextOrNull((byte)'W');
    public string? SchemaName => GetTextOrNull((byte)'s');
    public string? TableName => GetTextOrNull((byte)'t');
    public string? ColumnName => GetTextOrNull((byte)'c');
    public string? DataTypeName => GetTextOrNull((byte)'d');
    public string? ConstraintName => GetTextOrNull((byte)'n');
    public string? File => GetTextOrNull((byte)'F');
    public string? Line => GetAsciiOrNull((byte)'L');
    public string? Routine => GetAsciiOrNull((byte)'R');

    public bool Unhandled { get; }

    public ReadOnlySpan<PgTypes.BackendType> Expected => _expected;

    ErrorOrNoticeMessage(ReadOnlySequence<byte> body, PgTypes.BackendType[] expected, bool isNotice, bool unhandled,
        bool hasPriorCancellationExposure = false)
    {
        _body = body;
        _expected = expected;
        IsNotice = isNotice;
        HasPriorCancellationExposure = hasPriorCancellationExposure;
        Unhandled = unhandled;
        SqlState = GetAscii((byte)'C');
    }

    /// Copies the underlying field bytes so the error can outlive the transient message buffer it was
    /// read from. By default the body is a view into that buffer (zero copy), valid only while the
    /// error is handled inline; holders that let it escape the read cycle must Preserve first. The
    /// human-readable fields decode as UTF8 (TODO: thread ClientEncoding for non-UTF8 connections);
    /// the protocol-defined fields (C/S/V/P/p/L/R) are always ASCII.
    public ErrorOrNoticeMessage Preserve()
        => new(new ReadOnlySequence<byte>(_body.ToArray()), _expected, IsNotice, Unhandled, HasPriorCancellationExposure);

    // Scan the field block for fieldType, returning its value bytes. One pass per access; the body is
    // small and the common path reads only a couple of fields.
    bool TryGetField(byte fieldType, out ReadOnlySequence<byte> value)
    {
        var reader = new SequenceReader<byte>(_body);
        while (reader.TryRead(out var type) && type is not 0)
        {
            if (!reader.TryReadTo(out ReadOnlySequence<byte> v, (byte)0))
                break;
            if (type == fieldType)
            {
                value = v;
                return true;
            }
        }
        value = default;
        return false;
    }

    string GetAscii(byte fieldType) => TryGetField(fieldType, out var v) ? Encoding.ASCII.GetString(v) : "";
    string? GetAsciiOrNull(byte fieldType) => TryGetField(fieldType, out var v) ? Encoding.ASCII.GetString(v) : null;
    string GetText(byte fieldType) => TryGetField(fieldType, out var v) ? Encoding.UTF8.GetString(v) : "";
    string? GetTextOrNull(byte fieldType) => TryGetField(fieldType, out var v) ? Encoding.UTF8.GetString(v) : null;
    int GetInt(byte fieldType) => TryGetField(fieldType, out var v) && int.TryParse(Encoding.ASCII.GetString(v), out var n) ? n : 0;

    public static ErrorOrNoticeMessage Create(BackendMessage message, ReadOnlySpan<PgTypes.BackendType> expected, bool unhandled = true)
    {
        message.EnsureExpected(PgTypes.BackendType.ErrorResponse, PgTypes.BackendType.NoticeResponse);
        message.EnsureBuffered();

        return new(message.GetSequence(), expected.ToArray(), message.Header.Type is PgTypes.BackendType.NoticeResponse,
            unhandled, message.HasPriorCancellationExposure);
    }

    // Test seam: build directly from a raw error/notice field block, bypassing the BackendMessage
    // wrapper, so the field parser can be exercised without a live connection. Exposed via
    // InternalsVisibleTo (Slon.Tests).
    internal static ErrorOrNoticeMessage FromFieldBlock(ReadOnlySequence<byte> fieldBlock, bool isNotice = false)
        => new(fieldBlock, [], isNotice, unhandled: true);
}

sealed class PgError
{
    readonly ErrorOrNoticeMessage _message;

    public PgError(ErrorOrNoticeMessage message)
    {
        if (message.IsNotice)
            throw new ArgumentException("Cannot be constructed from a notice message.", nameof(message));
        _message = message;
    }

    public ReadOnlySpan<PgTypes.BackendType> Expected => _message.Expected;

    public string Severity => _message.Severity;
    public string InvariantSeverity => _message.InvariantSeverity;
    public string SqlState => _message.SqlState;
    public string MessageText => _message.MessageText;
    public string? Detail => _message.Detail;
    public string? Hint => _message.Hint;
    public int Position => _message.Position;
    public int InternalPosition => _message.InternalPosition;
    public string? InternalQuery => _message.InternalQuery;
    public string? Where => _message.Where;
    public string? SchemaName => _message.SchemaName;
    public string? TableName => _message.TableName;
    public string? ColumnName => _message.ColumnName;
    public string? DataTypeName => _message.DataTypeName;
    public string? ConstraintName => _message.ConstraintName;
    public string? File => _message.File;
    public string? Line => _message.Line;
    public string? Routine => _message.Routine;
    public bool IsTransientError => _message.IsTransientError;
    public bool IsCollateralCancellation
        => _message.HasPriorCancellationExposure && SqlState == PgErrorCodes.QueryCanceled;

    /// Copies the underlying field bytes so the error can outlive the transient message buffer.
    /// See <see cref="ErrorOrNoticeMessage.Preserve"/>.
    public PgError Preserve() => new(_message.Preserve());

    public static implicit operator PgError(ErrorOrNoticeMessage message) => new(message);
}

sealed class PgNotice
{
    readonly ErrorOrNoticeMessage _message;

    public PgNotice(ErrorOrNoticeMessage message)
    {
        if (!message.IsNotice)
            throw new ArgumentException("Cannot be constructed from an error message.", nameof(message));
        _message = message;
    }

    public static implicit operator PgNotice(ErrorOrNoticeMessage message) => new(message);
}

