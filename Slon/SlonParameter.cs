using System.Data;
using Slon.Data;

namespace Slon;

/// <inheritdoc cref="System.Data.Common.DbParameter" />
public partial class SlonParameter
{
    static readonly object UnboxedValue = new();
    object? _value;

    public SlonParameter() {}
    public SlonParameter(object? value) : this(string.Empty, value) {}
    public SlonParameter(string parameterName, object? value)
    {
        InitializeName(parameterName);
        // Make sure it goes through value update.
        Value = value;
    }

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

    private protected virtual SlonParameter CloneCore() => Clone(new SlonParameter { ValueCore = ValueCore });

    internal virtual void Bind(ref SerializerParameterWriterStrategy.ParameterBinder binder)
        => binder.Bind(_value);
    internal virtual void Write(ref SerializerParameterWriterStrategy.ParameterWriter writer)
        => writer.Write(_value);
    internal virtual void WriteAsync(ref SerializerParameterWriterStrategy.AsyncParameterWriter writer)
        => writer.Write(_value);

}


/// <inheritdoc cref="Slon.SlonParameter" />
public sealed class SlonParameter<T> : SlonParameter, IDbDataParameter<T>
{
    T? _value;

    public SlonParameter() => InvalidateBoxedValue();
    public SlonParameter(T? value) : this(string.Empty, value) {}
    public SlonParameter(string parameterName, T? value)
    {
        InitializeName(parameterName);
        // Make sure it goes through value update.
        Value = value;
    }

    /// <summary>Creates a new instance of a <see cref="Slon.SlonParameter{T}" /> object.</summary>
    /// <returns>The new instance.</returns>
    public new SlonParameter<T> Clone() => (SlonParameter<T>)CloneCore();

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
    private protected override SlonParameter CloneCore() => Clone(new SlonParameter<T> { _value = _value });
    internal override void Bind(ref SerializerParameterWriterStrategy.ParameterBinder binder)
        => binder.Bind(_value);
    internal override void Write(ref SerializerParameterWriterStrategy.ParameterWriter writer)
        => writer.Write(_value);
    internal override void WriteAsync(ref SerializerParameterWriterStrategy.AsyncParameterWriter writer)
        => writer.Write(_value);

}
