using System.Collections.Immutable;

namespace Slon.Pg.Types;

/// Enum of the kind of types supported by postgres.
abstract record PgTypeData
{
    PgTypeData(PgTypeKind kind) => Kind = kind;
    public PgTypeKind Kind { get; }

    public sealed record Enum(ImmutableArray<string> Variants) : PgTypeData(PgTypeKind.Enum)
    {
        public bool Equals(Enum? other) => base.Equals(other) && Variants.SequenceEqual(other.Variants);

        public override int GetHashCode()
        {
            var hashCode = new HashCode();
            hashCode.Add(base.GetHashCode());
            foreach (var value in Variants)
                hashCode.Add(value);
            return hashCode.ToHashCode();
        }
    }

    public sealed record Pseudo : PgTypeData
    {
        Pseudo() : base(PgTypeKind.Pseudo) {}
        internal static Pseudo Instance { get; } = new();
    }

    public sealed record Array(PgType ElementType) : PgTypeData(PgTypeKind.Array);
    public sealed record Range(PgType ElementType) : PgTypeData(PgTypeKind.Range);
    public sealed record Multirange(PgType RangeType) : PgTypeData(PgTypeKind.Multirange);
    public sealed record Domain(PgType UnderlyingType, bool IsNotNull) : PgTypeData(PgTypeKind.Domain);
    public sealed record Composite(ImmutableArray<PgCompositeFieldType> Fields) : PgTypeData(PgTypeKind.Composite)
    {
        public bool Equals(Composite? other) => base.Equals(other) && Fields.SequenceEqual(other.Fields);

        public override int GetHashCode()
        {
            var hashCode = new HashCode();
            hashCode.Add(base.GetHashCode());
            foreach (var value in Fields)
                hashCode.Add(value);
            return hashCode.ToHashCode();
        }
    }

}

/// Base field type shared between tables and composites.
[Experimental(ExperimentalDiagnostics.PostgreSqlLowerLayer)]
public readonly record struct Field(string Name, PgTypeId PgTypeId, int TypeModifier);

[Experimental(ExperimentalDiagnostics.PostgreSqlLowerLayer)]
public sealed record PgCompositeFieldType(Field Field)
{
    // Equality describes the catalog-independent field declaration. The mutable link is resolved
    // exactly once while sealing a catalog snapshot and deliberately does not participate in equality.
    PgType? _type;

    public PgType Type => _type
        ?? throw new InvalidOperationException("The composite field has not been linked to a type catalog.");

    internal void Link(PgType type) => _type = type;

    public bool Equals(PgCompositeFieldType? other)
        => other is not null && Field == other.Field;

    public override int GetHashCode() => Field.GetHashCode();
}
