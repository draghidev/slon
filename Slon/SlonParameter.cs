using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using Slon.Data;
using Slon.Pg.Serialization;

namespace Slon;

// Implementation
// Implements size-optimized storage for the less frequently used DbParameter members.
public partial class SlonParameter
{
    const byte DirectionMask = 0b0000_0111;
    const byte NullableMask = 0b0100_0000;
    const byte FrozenNameMask = 0b1000_0000;
    static readonly object UnboxedValue = new();

    // Either a parameter name (string) or a reference to additional (less commonly used) properties, see Props below.
    object _nameOrProps = "";
    byte _flags = (byte)ParameterDirection.Input;
    SlonDbType _slonDbType;
    int _typeRevision;
    object? _value;

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

    ParameterDirection DirectionCore
    {
        get => (ParameterDirection)(_flags & DirectionMask);
        set
        {
            // Output values do not participate in resolution. Switching back to input invalidates it.
            if (DirectionCore is ParameterDirection.Output or ParameterDirection.ReturnValue
                && value is ParameterDirection.Input or ParameterDirection.InputOutput)
                AdvanceTypeRevision();

            _flags = (byte)(((byte)value & DirectionMask) | (_flags & ~DirectionMask));
        }
    }

    bool IsNullableCore
    {
        get => (_flags & NullableMask) != 0;
        set => _flags = value ? (byte)(_flags | NullableMask) : (byte)(_flags & ~NullableMask);
    }

    byte? PrecisionCore
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

    byte? ScaleCore
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

    int? SizeCore
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
                throw new ArgumentOutOfRangeException(nameof(value), value, "The value must be greater than or equal to -1.");

            if (!value.HasValue && _nameOrProps is not Props)
                return;

            GetOrCreateProps().Size = value;
        }
    }

    internal void NotifyCollectionAdd() => IsNameFrozen = true;

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

    DbType? DbTypeCore
    {
        get => SlonDbTypeCore.ToDbType();
        set => SlonDbTypeCore = (SlonDbType)value;
    }

    private protected virtual Type StaticValueType => typeof(object);

    private protected virtual object? ValueCore
    {
        get => _value;
        set
        {
            TrackObjectValueTypeChange(_value, value);
            _value = value;
        }
    }

    private protected object? GetOrCacheBoxedValue<T>(T? value)
    {
        if (ReferenceEquals(_value, UnboxedValue))
            _value = value;
        return _value;
    }

    private protected void InvalidateBoxedValue() => _value = UnboxedValue;

    private protected virtual SlonParameter CloneCore()
        => CloneTo(new SlonParameter { _value = _value });

    internal int TypeRevision => _typeRevision;

    internal (SlonDbType SlonDbType, Type? ValueType) GetResolutionInput()
    {
        var staticValueType = StaticValueType;
        return (_slonDbType, staticValueType == typeof(object) ? ValueCore?.GetType() : staticValueType);
    }

    private protected SlonParameter CloneTo(SlonParameter instance)
    {
        instance._nameOrProps = _nameOrProps is Props props ? props.Clone() : _nameOrProps;
        instance._flags = (byte)(_flags & ~FrozenNameMask);
        instance._slonDbType = _slonDbType;
        instance._typeRevision = 0;
        return instance;
    }

    private protected void TrackObjectValueTypeChange(object? previousValue, object? value)
    {
        // We ignore the value for output parameters.
        if (previousValue == value
            || DirectionCore is ParameterDirection.Output or ParameterDirection.ReturnValue)
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

    internal virtual void Apply(PgTypeInfo typeInfo, PgParameterValueOperation operation)
        => operation.Apply(typeInfo, _value);

    sealed class Props
    {
        public string ParameterName = string.Empty;
        public byte? Precision;
        public byte? Scale;
        public int? Size;
        public string SourceColumn = string.Empty;
        public bool SourceColumnNullMapping;
        public DataRowVersion SourceVersion = DataRowVersion.Current;

        public Props Clone() => (Props)MemberwiseClone();

        public static Props Create(string parameterName)
            => new() { ParameterName = parameterName };
    }
}

// Public surface & ADO.NET
/// <inheritdoc cref="System.Data.Common.DbParameter" />
public partial class SlonParameter : DbParameter
{
    /// Initializes an unnamed parameter with no value.
    public SlonParameter() { }

    /// <summary>Initializes an unnamed parameter with the specified value.</summary>
    /// <param name="value">The parameter value.</param>
    public SlonParameter(object? value) : this(string.Empty, value) { }

    /// <summary>Initializes a parameter with the specified name and value.</summary>
    /// <param name="parameterName">The parameter name, or an empty string for a positional parameter.</param>
    /// <param name="value">The parameter value.</param>
    public SlonParameter(string parameterName, object? value)
    {
        InitializeName(parameterName);
        Value = value;
    }

    /// <summary>Creates a new instance of a <see cref="T:Slon.SlonParameter" /> object.</summary>
    /// <returns>The new instance.</returns>
    public SlonParameter Clone() => CloneCore();

    /// Gets or sets the PostgreSQL type requested for this parameter.
    public SlonDbType SlonDbType
    {
        get => SlonDbTypeCore;
        set => SlonDbTypeCore = value;
    }

    /// <inheritdoc />
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
    /// <inheritdoc />
    public sealed override object? Value
    {
        get => ValueCore;
        set => ValueCore = value;
    }
    /// <inheritdoc />
    public sealed override DbType DbType
    {
        get => DbTypeCore ?? DbType.String;
        set
        {
            if (!Enum.IsDefined(value))
                throw new ArgumentOutOfRangeException(nameof(value), $"Invalid {nameof(System.Data.DbType)} value.");
            DbTypeCore = value;
        }
    }
    /// <inheritdoc />
    public sealed override ParameterDirection Direction
    {
        get => DirectionCore;
        set
        {
            if (value is not (ParameterDirection.Input or ParameterDirection.Output
                or ParameterDirection.InputOutput or ParameterDirection.ReturnValue))
                throw new ArgumentOutOfRangeException(nameof(value), $"Invalid {nameof(ParameterDirection)} value.");

            DirectionCore = value;
        }
    }

    /// <inheritdoc />
    public sealed override bool IsNullable
    {
        get => IsNullableCore;
        set => IsNullableCore = value;
    }

    /// <inheritdoc />
    public sealed override byte Precision
    {
        get => PrecisionCore.GetValueOrDefault();
        set => PrecisionCore = value;
    }

    /// <inheritdoc />
    public sealed override byte Scale
    {
        get => ScaleCore.GetValueOrDefault();
        set => ScaleCore = value;
    }

    /// <inheritdoc />
    public sealed override int Size
    {
        get => SizeCore.GetValueOrDefault();
        set => SizeCore = value;
    }

    /// <inheritdoc />
    public sealed override void ResetDbType() => DbTypeCore = null;

    /// <inheritdoc />
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
            if (value is "" && _nameOrProps is not Props)
                return;

            GetOrCreateProps().SourceColumn = value;
        }
    }
    /// <inheritdoc />
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
            if (!value && _nameOrProps is not Props)
                return;

            GetOrCreateProps().SourceColumnNullMapping = value;
        }
    }
    /// <inheritdoc />
    public sealed override DataRowVersion SourceVersion
    {
        get
        {
            if (_nameOrProps is Props p)
                return p.SourceVersion;

            return DataRowVersion.Current;
        }
        set
        {
            if (value is DataRowVersion.Current && _nameOrProps is not Props)
                return;

            GetOrCreateProps().SourceVersion = value;
        }
    }
}

/// <inheritdoc cref="Slon.SlonParameter" />
public sealed class SlonParameter<T> : SlonParameter, IDbDataParameter<T>
{
    T? _value;

    /// Initializes an unnamed typed parameter with no value.
    public SlonParameter() => InvalidateBoxedValue();

    /// <summary>Initializes an unnamed typed parameter with the specified value.</summary>
    /// <param name="value">The parameter value.</param>
    public SlonParameter(T? value) : this(string.Empty, value) { }

    /// <summary>Initializes a typed parameter with the specified name and value.</summary>
    /// <param name="parameterName">The parameter name, or an empty string for a positional parameter.</param>
    /// <param name="value">The parameter value.</param>
    public SlonParameter(string parameterName, T? value)
    {
        InitializeName(parameterName);
        Value = value;
    }

    /// <summary>Creates a new instance of a <see cref="Slon.SlonParameter{T}" /> object.</summary>
    /// <returns>The new instance.</returns>
    public new SlonParameter<T> Clone() => (SlonParameter<T>)CloneCore();

    /// Gets or sets the strongly typed parameter value.
    public new T? Value
    {
        get => _value;
        set => SetValue(value);
    }

    void SetValue(T? value)
    {
        if (typeof(T) == typeof(object))
            TrackObjectValueTypeChange(_value, value);
        _value = value;
        InvalidateBoxedValue();
    }

    private protected override Type StaticValueType => typeof(T);

    private protected override object? ValueCore
    {
        get => typeof(T).IsValueType ? GetOrCacheBoxedValue(_value) : _value;
        set => Value = (T?)value;
    }

    private protected override SlonParameter CloneCore()
        => CloneTo(new SlonParameter<T> { _value = _value });

    internal override void Apply(PgTypeInfo typeInfo, PgParameterValueOperation operation)
        => operation.Apply(typeInfo, _value);
}
