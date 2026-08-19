using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Slon.Pg;

namespace Slon;

// TODO old code, should redo it with a bit less twiddling.

// Size optimized base class.
// Originally DbDataParameter but now just a partial of SlonDbParameter, implements all the uninteresting base members.
public abstract partial class SlonDbParameter
{
    // Either a parameter name (string) or a reference to additional (less commonly used) properties, see Props below.
    object _nameOrProps = "";

    // TODO Maybe want to use a BitVector32 for _combinedEnums.
    // Combines 'Uses' (first 3 bytes), the last byte contains 'ParameterDirection' (least significant 3 bits), 'IsNullable' (bit 7) and 'IsFrozenName' (bit 8).
    volatile uint _combinedEnums;

    // Internal for now.
    private protected SlonDbParameter() {}
    private protected SlonDbParameter(string parameterName)
        :this()
        => _nameOrProps = parameterName ?? string.Empty; // Just to be sure, it's relied upon in the implementation.

    bool IsReadOnlyName
    {
        get => (_combinedEnums & 0x80) == 0x80; // get the most significant bit.
        set
        {
            uint current;
            uint newValue;
            do
            {
                current = _combinedEnums;
                newValue = (uint)(value ? 1 : 0) << 7 | (current & 0xffffff7f); // 0x7f == 255 - 128
            } while (Interlocked.CompareExchange(ref _combinedEnums, newValue, current) != (int)current);
        }
    }

    Props GetOrCreateProps()
    {
        var nameOrProps = _nameOrProps;
        if (nameOrProps is string name)
            nameOrProps = _nameOrProps = Props.Create(name);

        return (Props)nameOrProps;
    }

    private protected abstract object? ValueCore { get; set; }

    int UsesCount => (int)_combinedEnums >> 8;
    bool IsReadOnly => UsesCount != 0;

    // private protected for testing.
    private protected int IncrementUses(int count = 1)
    {
        if (count < 0)
            throw new ArgumentOutOfRangeException(nameof(count), "Cannot be negative.");

        uint current;
        uint newValue;
        do
        {
            // Operate on an unsigned int as we don't want the top bit to be interpreted as a sign bit, we have 24 bits.
            current = _combinedEnums;
            if ((current >> 8) + count > (int)Math.Pow(2, 24) - 1)
                throw new InvalidOperationException("Cannot increment past uint24.MaxValue.");

            var incremented = (uint)((current >> 8) + count) << 8;
            newValue = incremented | (current & 0x000000ff);
        } while (Interlocked.CompareExchange(ref _combinedEnums, newValue, current) != (int)current);
        return (int)(newValue >> 8);
    }

    /// <returns>The new value that was stored by this operation.</returns>
    int IncrementUses() => IncrementUses(count: 1);

    // private protected for testing.
    private protected int DecrementUses(int count = 1)
    {
        if (count < 0)
            throw new ArgumentOutOfRangeException(nameof(count), "Cannot be negative.");

        uint current;
        uint newValue;
        do
        {
            // Operate on an unsigned int as we don't want the top bit to be interpreted as a sign bit, we have 24 bits.
            current = _combinedEnums;
            if ((current >> 8) - count < 0)
                throw new InvalidOperationException("Cannot decrement past 0.");

            var incremented = (uint)((current >> 8) - count) << 8;
            newValue = incremented | (current & 0x000000ff);
        } while (Interlocked.CompareExchange(ref _combinedEnums, newValue, current) != (int)current);
        return (int)(newValue >> 8);
    }

    /// <returns>The new value that was stored by this operation.</returns>
    int DecrementUses() => DecrementUses(count: 1);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private protected void ThrowIfReadOnly()
    {
        if (IsReadOnly)
            Throw();

        static void Throw() => throw new InvalidOperationException("This parameter is currently in use for a command execution, clone the parameter to change its values or wait for execution to end.");
    }

    private protected virtual ParameterDirection DirectionBase
    {
        get => (ParameterDirection)(_combinedEnums & 0x07); // take the first 3 bits.
        set => _combinedEnums = (byte)value | (_combinedEnums & 0xfffffff8); // 0xf8 == 255 - 7
    }

    private protected virtual bool IsNullableBase
    {
        get => (_combinedEnums & 0x40) == 0x40; // get the second most significant bit of the first byte.
        set => _combinedEnums = (uint)(value ? 1 : 0) << 6 | (_combinedEnums & 0xffffffbf); // 0xbf == 255 - 64
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

    private protected abstract SlonDbParameter CloneCore();
    void CloneBase(SlonDbParameter instance)
    {
        if (_nameOrProps is Props p)
            instance._nameOrProps = p.Clone();
        else
            instance._nameOrProps = _nameOrProps;
        instance._combinedEnums = _combinedEnums;
    }

    internal void NotifyCollectionAdd() => IsReadOnlyName = true;

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
public partial class SlonDbParameter: DbParameter
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
            if (IsReadOnlyName)
                throw new InvalidOperationException("Parameter has been added to a collection at least once, clone the parameter to change the name.");

            ThrowIfReadOnly();
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
        set
        {
            ThrowIfReadOnly();
            ValueCore = value;
        }
    }
    public sealed override DbType DbType
    {
        get => DbTypeCore ?? DbType.String;
        set
        {
            ThrowIfReadOnly();
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
            ThrowIfReadOnly();
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
        set
        {
            ThrowIfReadOnly();
            IsNullableBase = value;
        }
    }

    public sealed override byte Precision
    {
        get => PrecisionBase.GetValueOrDefault();
        set
        {
            ThrowIfReadOnly();
            PrecisionBase = value;
        }
    }

    public sealed override byte Scale
    {
        get => ScaleBase.GetValueOrDefault();
        set
        {
            ThrowIfReadOnly();
            ScaleBase = value;
        }
    }

    public sealed override int Size
    {
        get => SizeBase.GetValueOrDefault();
        set
        {
            ThrowIfReadOnly();
            SizeBase = value;
        }
    }

    public sealed override void ResetDbType()
    {
        ThrowIfReadOnly();
        ResetInference();
    }

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
            ThrowIfReadOnly();
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
            ThrowIfReadOnly();
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
            ThrowIfReadOnly();
            if (value == default)
                return;

            GetOrCreateProps().SourceVersion = value;
        }
    }
}

// Base class for the two parameter types in Slon, see SlonDbParameter.Base.cs for all the uninteresting parts.
public abstract partial class SlonDbParameter: IParameter
{
    bool? _preferTextualFormat;
    SlonDbType _slonDbType;
    int _typeRevision;

    /// <summary>Creates a new instance of a <see cref="T:Slon.SlonDbParameter" /> object.</summary>
    /// <returns>The new instance.</returns>
    public SlonDbParameter Clone() => CloneCore();

    /// Some converters support both a textual and binary format for the postgres type this parameter maps to.
    /// When this property is set to true a textual format should be preferred.
    /// When its set to false a non-textual (binary) format is preferred.
    /// The default value is null which allows the converter to pick the most optimal format.
    public bool? PreferTextualFormat
    {
        get => _preferTextualFormat;
        set
        {
            ThrowIfReadOnly();
            _preferTextualFormat = value;
        }
    }

    public SlonDbType SlonDbType
    {
        get => SlonDbTypeCore;
        set
        {
            ThrowIfReadOnly();
            SlonDbTypeCore = value;
        }
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

    private protected SlonDbParameter Clone(SlonDbParameter instance)
    {
        CloneBase(instance);
        instance._preferTextualFormat = _preferTextualFormat;
        instance._slonDbType = _slonDbType;
        instance._typeRevision = 0;
        return instance;
    }

    internal IParameter StartSession()
    {
        if (IncrementUses() > 1 && Direction is not ParameterDirection.Input)
        {
            DecrementUses();
            ThrowHelper.ThrowInvalidOperation("An output or return value direction parameter can't be used by commands executing in parallel.");
        }

        return this;
    }

    internal void EndSession() => DecrementUses();

    ParameterKind IParameter.Kind => (ParameterKind)Direction;
    Type IParameter.StaticValueType => StaticValueType;
    string IParameter.Name => ParameterName;
    object? IParameter.Value => Value;

    private protected abstract void SetOutputValue(object? value);
    void IParameter.SetOutputResult(object? value) => SetOutputValue(value);

    internal abstract void Bind(ref SerializerParameterWriterStrategy.ParameterBinder binder);
    internal abstract void Write(ref SerializerParameterWriterStrategy.ParameterWriter writer);
    internal abstract void WriteAsync(ref SerializerParameterWriterStrategy.AsyncParameterWriter writer);

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
