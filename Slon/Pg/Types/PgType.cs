using System.Collections.Immutable;

namespace Slon.Pg.Types;

readonly record struct PgType
{
    PgType(DataTypeName dataTypeName, Oid? oid)
    {
        Data = null;
        DataTypeName = dataTypeName;
        Oid = oid;
    }

    PgType(PgTypeData data, DataTypeName dataTypeName, Oid? oid)
    {
        Data = data;
        DataTypeName = dataTypeName;
        Oid = oid;
    }

    /// Returns the element type for arrays, ranges and multi ranges, returns itself for all the other cases.
    public PgType ElementType =>
        Kind switch
        {
            PgTypeKind.Base => this,
            PgTypeKind.Domain => this,
            PgTypeKind.Pseudo => this,
            PgTypeKind.Enum => this,
            PgTypeKind.Composite => this,
            PgTypeKind.Array => ((PgTypeData.Array)Data!).ElementType,
            PgTypeKind.Range => ((PgTypeData.Range)Data!).ElementType,
            PgTypeKind.Multirange => ((PgTypeData.Multirange)Data!).RangeType.ElementType,
            var v => throw new ArgumentOutOfRangeException(nameof(Data), v, null)
        };

    public PgType RangeType => Kind is PgTypeKind.Multirange
        ? ((PgTypeData.Multirange)Data!).RangeType
        : throw new InvalidOperationException("Type is not of kind Multirange.");

    public PgType UnderlyingType => Kind is PgTypeKind.Domain
        ? ((PgTypeData.Domain)Data!).UnderlyingType
        : throw new InvalidOperationException("Type is not of kind Domain.");

    public bool IsDomainNotNull => Kind is PgTypeKind.Domain
        ? ((PgTypeData.Domain)Data!).IsNotNull
        : throw new InvalidOperationException("Type is not of kind Domain.");

    public ImmutableArray<PgCompositeFieldType> CompositeFields => Kind is PgTypeKind.Composite
        ? ((PgTypeData.Composite)Data!).Fields
        : throw new InvalidOperationException("Type is not of kind Composite.");

    public ImmutableArray<string> EnumVariants => Kind is PgTypeKind.Enum
        ? ((PgTypeData.Enum)Data!).Variants
        : throw new InvalidOperationException("Type is not of kind Enum.");

    public PgTypeKind Kind => Data?.Kind ?? PgTypeKind.Base;
    PgTypeData? Data { get; init; }
    public DataTypeName DataTypeName { get; init; }
    public Oid? Oid { get; init; }

    public static PgType CreateBase(DataTypeName dataTypeName, Oid? oid = null)
        => new(dataTypeName, oid);

    public static PgType CreatePseudo(DataTypeName dataTypeName, Oid? oid = null)
        => new(PgTypeData.Pseudo.Instance, dataTypeName, oid);

    public static PgType CreateEnum(ImmutableArray<string> variants, DataTypeName dataTypeName, Oid? oid = null)
        => new(new PgTypeData.Enum(variants), dataTypeName, oid);

    public static PgType CreateArray(PgType elementType, Oid? oid = null)
        => new(new PgTypeData.Array(elementType), elementType.DataTypeName.ToArrayName(), oid);

    internal static PgType CreateArray(PgType elementType, DataTypeName dataTypeName, Oid? oid = null)
        => new(new PgTypeData.Array(elementType), dataTypeName, oid);

    public static PgType CreateRange(PgType elementType, DataTypeName dataTypeName, Oid? oid = null)
        => new(new PgTypeData.Range(elementType), dataTypeName, oid);

    public static PgType CreateMultirange(PgType rangeType, DataTypeName dataTypeName, Oid? oid = null)
        => new(new PgTypeData.Multirange(rangeType), dataTypeName, oid);

    public static PgType CreateDomain(PgType underlyingType, DataTypeName dataTypeName, Oid? oid = null,
        bool isNotNull = false)
        => new(new PgTypeData.Domain(underlyingType, isNotNull), dataTypeName, oid);

    public static PgType CreateComposite(ImmutableArray<PgCompositeFieldType> fields, DataTypeName dataTypeName, Oid? oid = null)
        => new(new PgTypeData.Composite(fields), dataTypeName, oid);
}

enum PgTypeKind
{
    /// A base type.
    Base,
    /// An enum carying its variants.
    Enum,
    /// A pseudo type like anyarray.
    Pseudo,
    // An array carying its element type.
    Array,
    // A range carying its element type.
    Range,
    // A multi-range carying its element type.
    Multirange,
    // A domain carying its underlying type.
    Domain,
    // A composite carying its constituent fields.
    Composite
}
