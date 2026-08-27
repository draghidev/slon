using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;

namespace Slon.Text;

// PostgreSQL protocol C-string whose encoded form is reused across Parse/Bind/Close operations.
// Defined as a struct wrapping a class so default cheaply represents the unnamed statement/portal.
[DebuggerDisplay("{_core,nq}")]
[Experimental(ExperimentalDiagnostics.PostgreSqlLowerLayer)]
public readonly struct EncodedCString
{
    readonly Core? _core;

    public EncodedCString(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        if (value.Contains('\0'))
            throw new ArgumentException("PostgreSQL protocol strings cannot contain NUL characters.", nameof(value));

        _core = new(value);
    }

    public bool IsDefault => _core is null;

    public ReadOnlySpan<byte> AsSpan(Encoding encoding) => _core is null ? [] : _core.AsSpan(encoding);
    public ReadOnlySpan<byte> AsNullTerminatedSpan(Encoding encoding) => _core is null ? [0] : _core.AsNullTerminatedSpan(encoding);

    public bool ValueEquals(EncodedCString other)
        => ReferenceEquals(_core, other._core)
            || (_core is not null && other._core is not null
                && string.Equals(_core.Value, other._core.Value, StringComparison.Ordinal));

    public override string ToString() => _core?.Value ?? "";

    public static implicit operator EncodedCString(string value) => new(value);

    // Used for long lived strings that may have to be re-encoded (but usually wont), thread-safe.
    [DebuggerDisplay("{_value,nq}")]
    sealed class Core(string value)
    {
        readonly string _value = value;
        public string Value => _value;
        EncodedValue? _encoded;

        public ReadOnlySpan<byte> AsSpan(Encoding encoding) => AsNullTerminatedSpan(encoding)[..^1];
        public ReadOnlySpan<byte> AsNullTerminatedSpan(Encoding encoding)
        {
            var encoded = Volatile.Read(ref _encoded);
            return encoded is not null && ReferenceEquals(encoding, encoded.Encoding)
                ? encoded.Bytes
                : Core();

            [MethodImpl(MethodImplOptions.NoInlining)]
            ReadOnlySpan<byte> Core()
            {
                lock (this)
                {
                    var encoded = _encoded;
                    if (encoded is not null && ReferenceEquals(encoding, encoded.Encoding))
                        return encoded.Bytes;

                    encoded = new(encoding, [..encoding.GetBytes(_value), 0]);
                    Volatile.Write(ref _encoded, encoded);
                    return encoded.Bytes;
                }
            }
        }

        sealed class EncodedValue(Encoding encoding, byte[] bytes)
        {
            public Encoding Encoding { get; } = encoding;
            public byte[] Bytes { get; } = bytes;
        }
    }
}
