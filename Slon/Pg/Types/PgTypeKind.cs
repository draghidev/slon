using System.Collections.Immutable;

namespace Slon.Pg.Types;

/// Enum of the kind of types supported by postgres.
abstract record PgTypeData
{
    PgTypeData(PgTypeKind kind) => Kind = kind;
    public PgTypeKind Kind { get; }

    public sealed record Base : PgTypeData
    {
        Base() : base(PgTypeKind.Base) {}
        internal static Base Instance => new();
    }

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
        internal static Pseudo Instance => new();
    }

    public sealed record Array(PgType ElementType) : PgTypeData(PgTypeKind.Array);
    public sealed record Range(PgType ElementType) : PgTypeData(PgTypeKind.Range);
    public sealed record Multirange(PgType RangeType) : PgTypeData(PgTypeKind.Multirange);
    public sealed record Domain(PgType UnderlyingType) : PgTypeData(PgTypeKind.Domain);
    public sealed record Composite(ImmutableArray<PgCompositeFieldType> FieldTypes) : PgTypeData(PgTypeKind.Composite)
    {
        public bool Equals(Composite? other) => base.Equals(other) && FieldTypes.SequenceEqual(other.FieldTypes);

        public override int GetHashCode()
        {
            var hashCode = new HashCode();
            hashCode.Add(base.GetHashCode());
            foreach (var value in FieldTypes)
                hashCode.Add(value);
            return hashCode.ToHashCode();
        }
    }

    public static Base BaseInstance => Base.Instance;
    public static Pseudo PseudoInstance => Pseudo.Instance;
}

readonly record struct PgCompositeFieldType(PgType Type, Field Field);

/// Base field type shared between tables and composites.
readonly record struct Field(string Name, PgTypeId PgTypeId, int TypeModifier);
