using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace Slon;

// Implements the size-optimized storage for the less frequently used DbParameter members.
public partial class SlonParameter
{
    const byte DirectionMask = 0b0000_0111;
    const byte NullableMask = 0b0100_0000;
    const byte FrozenNameMask = 0b1000_0000;

    // Either a parameter name (string) or a reference to additional (less commonly used) properties, see Props below.
    object _nameOrProps = "";
    byte _flags = (byte)ParameterDirection.Input;

    private protected void InitializeName(string parameterName)
        => _nameOrProps = parameterName ?? string.Empty;

    bool IsNameFrozen
    {
        get => (_flags & FrozenNameMask) != 0;
        set => _flags = value ? (byte)(_flags | FrozenNameMask) : (byte)(_flags & ~FrozenNameMask);
    }

    Props GetOrCreateProps()
    {
        var nameOrProps = _nameOrProps;
        if (nameOrProps is string name)
            nameOrProps = _nameOrProps = Props.Create(name);

        return (Props)nameOrProps;
    }

    private protected virtual ParameterDirection DirectionBase
    {
        get => (ParameterDirection)(_flags & DirectionMask);
        set => _flags = (byte)(((byte)value & DirectionMask) | (_flags & ~DirectionMask));
    }

    private protected virtual bool IsNullableBase
    {
        get => (_flags & NullableMask) != 0;
        set => _flags = value ? (byte)(_flags | NullableMask) : (byte)(_flags & ~NullableMask);
    }

    private protected virtual byte? PrecisionBase
    {
        get
        {
            if (_nameOrProps is Props { Precision: { } precision })
                return precision;

            return default;
        }
        set
        {
            if (!value.HasValue && _nameOrProps is not Props)
                return;

            GetOrCreateProps().Precision = value;
        }
    }

    private protected virtual byte? ScaleBase
    {
        get
        {
            if (_nameOrProps is Props { Scale: { } scale })
                return scale;

            return default;
        }
        set
        {
            if (!value.HasValue && _nameOrProps is not Props)
                return;

            GetOrCreateProps().Scale = value;
        }
    }

    private protected virtual int? SizeBase
    {
        get
        {
            // If we have props and it's set, return it, otherwise try to infer from the type.
            if (_nameOrProps is Props { Size: { } size })
                return size;

            return default;
        }
        set
        {
            if (value < -1)
                throw new ArgumentOutOfRangeException($"Invalid parameter value '{value}'. The value must be greater than or equal to 0.");

            if (!value.HasValue && _nameOrProps is not Props)
                return;

            GetOrCreateProps().Size = value;
        }
    }

    private protected virtual void ResetInference()
    {
        DbTypeCore = null;
        if (_nameOrProps is Props p)
            p.ResetFacets();
    }

    void CloneBase(SlonParameter instance)
    {
        if (_nameOrProps is Props p)
            instance._nameOrProps = p.Clone();
        else
            instance._nameOrProps = _nameOrProps;
        instance._flags = _flags;
    }

    internal void NotifyCollectionAdd() => IsNameFrozen = true;

    sealed class Props
    {
        public string ParameterName { get; set; } = string.Empty;
        public byte? Precision { get; set; }
        public byte? Scale { get; set; }
        public int? Size { get; set; }
        public string SourceColumn { get; set; } = string.Empty;
        public bool SourceColumnNullMapping { get; set; }
        public DataRowVersion SourceVersion { get; set; }

        public Props Clone() => (Props)MemberwiseClone();

        public void ResetFacets()
        {
            Precision = default;
            Scale = default;
            Size = default;
        }

        public static Props Create(string parameterName)
            => new() { ParameterName = parameterName };
    }
}

// Public surface & ADO.NET
/// <inheritdoc cref="System.Data.Common.DbParameter" />
public partial class SlonParameter: DbParameter
{
    [AllowNull]
    public sealed override string ParameterName
    {
        get
        {
            if (_nameOrProps is string name)
                return name;

            return ((Props)_nameOrProps).ParameterName;
        }
        set
        {
            if (IsNameFrozen)
                throw new InvalidOperationException("Parameter has been added to a collection at least once, clone the parameter to change the name.");

            value ??= string.Empty;
            if (_nameOrProps is Props p)
            {
                p.ParameterName = value;
                return;
            }
            _nameOrProps = value;
        }
    }
    public sealed override object? Value
    {
        get => ValueCore;
        set => ValueCore = value;
    }
    public sealed override DbType DbType
    {
        get => DbTypeCore ?? DbType.String;
        set
        {
            if ((int)value is 24 or > 27 && !Enum.IsDefined(value))
                throw new ArgumentOutOfRangeException(nameof(value), $"Invalid {nameof(System.Data.DbType)} value.");
            DbTypeCore = value;
        }
    }
    public sealed override ParameterDirection Direction
    {
        get => DirectionCore;
        set
        {
            switch (value)
            {
                case ParameterDirection.Input or ParameterDirection.Output or ParameterDirection.InputOutput or ParameterDirection.ReturnValue:
                case var _ when Enum.IsDefined(value):
                    DirectionCore = value;
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(value), $"Invalid {nameof(ParameterDirection)} value.");
            }
        }
    }

    public sealed override bool IsNullable
    {
        get => IsNullableBase;
        set => IsNullableBase = value;
    }

    public sealed override byte Precision
    {
        get => PrecisionBase.GetValueOrDefault();
        set => PrecisionBase = value;
    }

    public sealed override byte Scale
    {
        get => ScaleBase.GetValueOrDefault();
        set => ScaleBase = value;
    }

    public sealed override int Size
    {
        get => SizeBase.GetValueOrDefault();
        set => SizeBase = value;
    }

    public sealed override void ResetDbType() => ResetInference();

    [AllowNull]
    public sealed override string SourceColumn
    {
        get
        {
            if (_nameOrProps is Props p)
                return p.SourceColumn;

            return string.Empty;
        }
        set
        {
            value ??= string.Empty;
            if (value is "")
                return;

            GetOrCreateProps().SourceColumn = value;
        }
    }
    public sealed override bool SourceColumnNullMapping
    {
        get
        {
            if (_nameOrProps is Props p)
                return p.SourceColumnNullMapping;

            return false;
        }
        set
        {
            if (value == default)
                return;

            GetOrCreateProps().SourceColumnNullMapping = value;
        }
    }
    public sealed override DataRowVersion SourceVersion
    {
        get
        {
            if (_nameOrProps is Props p)
                return p.SourceVersion;

            return default;
        }
        set
        {
            if (value == default)
                return;

            GetOrCreateProps().SourceVersion = value;
        }
    }
}

public partial class SlonParameter
{
    bool? _preferTextualFormat;
    SlonDbType _slonDbType;
    int _typeRevision;

    /// <summary>Creates a new instance of a <see cref="T:Slon.SlonParameter" /> object.</summary>
    /// <returns>The new instance.</returns>
    public SlonParameter Clone() => CloneCore();

    /// Some converters support both a textual and binary format for the postgres type this parameter maps to.
    /// When this property is set to true a textual format should be preferred.
    /// When its set to false a non-textual (binary) format is preferred.
    /// The default value is null which allows the converter to pick the most optimal format.
    public bool? PreferTextualFormat
    {
        get => _preferTextualFormat;
        set => _preferTextualFormat = value;
    }

    public SlonDbType SlonDbType
    {
        get => SlonDbTypeCore;
        set => SlonDbTypeCore = value;
    }

    protected ParameterDirection DirectionCore
    {
        get => DirectionBase;
        set
        {
            // Output values do not participate in resolution. Switching back to input invalidates it.
            if (DirectionCore is ParameterDirection.Output or ParameterDirection.ReturnValue
                && value is ParameterDirection.Input or ParameterDirection.InputOutput)
                AdvanceTypeRevision();

            DirectionBase = value;
        }
    }

    protected DbType? DbTypeCore
    {
        get => SlonDbTypeCore.ToDbType();
        set => SlonDbTypeCore = (SlonDbType)value;
    }

    SlonDbType SlonDbTypeCore
    {
        get => _slonDbType;
        set
        {
            if (value != _slonDbType)
                AdvanceTypeRevision();
            _slonDbType = value;
        }
    }

    private protected virtual Type StaticValueType => typeof(object);

    internal int TypeRevision => _typeRevision;
    internal (SlonDbType SlonDbType, Type? ValueType) GetResolutionInput()
    {
        var staticValueType = StaticValueType;
        return (_slonDbType, staticValueType == typeof(object) ? ValueCore?.GetType() : staticValueType);
    }

    private protected SlonParameter Clone(SlonParameter instance)
    {
        CloneBase(instance);
        instance._preferTextualFormat = _preferTextualFormat;
        instance._slonDbType = _slonDbType;
        instance._typeRevision = 0;
        return instance;
    }

    private protected void TrackObjectValueTypeChange(object? previousValue, object? value)
    {
        // We ignore the value for output parameters.
        if (previousValue == value
            || Direction is ParameterDirection.Output or ParameterDirection.ReturnValue)
            return;

        // DBNull can be written through any existing resolution.
        if (value is DBNull)
            return;

        if (previousValue is not null && value is not null
            && previousValue.GetType() == value.GetType())
            return;

        AdvanceTypeRevision();
    }

    void AdvanceTypeRevision() => _typeRevision = checked(_typeRevision + 1);
}
