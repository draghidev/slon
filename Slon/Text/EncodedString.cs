using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Text;

namespace Slon.Text;

// Defined as a struct wrapping a class so we can always pull some usable span out, without needing explicit null checks in consuming code.
[DebuggerDisplay("{_core,nq}")]
readonly struct EncodedString(string value) : IEquatable<EncodedString>
{
    readonly Core _core = new(value);

    [MemberNotNullWhen(false, nameof(Value))]
    public bool IsDefault => _core is null;

    public ReadOnlySpan<byte> AsSpan(Encoding encoding) => _core is null ? [] : _core.AsSpan(encoding);
    public ReadOnlySpan<byte> AsNullTerminatedSpan(Encoding encoding) => _core is null ? [0] : _core.AsNullTerminatedSpan(encoding);

    public string? Value => _core?.Value;

    public override string ToString() => _core?.Value ?? "";
    public bool Equals(EncodedString other)
    {
        if (_core is null && other._core is null)
            return true;

        return _core?.Value.Equals(other._core.Value) == true;
    }

    public override bool Equals(object? obj) => obj is EncodedString other && Equals(other);
    public override int GetHashCode() => _core.Value.GetHashCode();

    public static implicit operator EncodedString(string value) => new(value);
    public static bool operator ==(EncodedString left, EncodedString right) => left.Equals(right);
    public static bool operator !=(EncodedString left, EncodedString right) => !left.Equals(right);

    // Used for long lived strings that may have to be re-encoded (but usually wont), thread-safe.
    [DebuggerDisplay("{_value,nq}")]
    sealed class Core(string value)
    {
        readonly string _value = value;
        public string Value => _value;
        Encoding? _encoding;
        byte[]? _encoded;

        public ReadOnlySpan<byte> AsSpan(Encoding encoding) => AsNullTerminatedSpan(encoding)[..^1];
        public ReadOnlySpan<byte> AsNullTerminatedSpan(Encoding encoding)
        {
            return ReferenceEquals(encoding, _encoding) ? (ReadOnlySpan<byte>)_encoded! : Core();

            [MethodImpl(MethodImplOptions.NoInlining)]
            ReadOnlySpan<byte> Core()
            {
                lock (_value)
                {
                    if (ReferenceEquals(encoding, _encoding))
                    {
                        Debug.Assert(_encoded is not null);
                        return _encoded;
                    }

                    _encoding = encoding;
                    return _encoded = [..encoding.GetBytes(_value), 0];
                }
            }
        }
    }
}
