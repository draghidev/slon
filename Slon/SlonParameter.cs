using System.Data;
using Slon.Data;
using Slon.Pg;

namespace Slon;

/// <inheritdoc cref="Slon.SlonDbParameter" />
public sealed class SlonParameter: SlonDbParameter, IParameter<object>
{
    object? _value;

    public SlonParameter() {}
    public SlonParameter(object? value) : this(string.Empty, value) {}
    public SlonParameter(string parameterName, object? value)
        :base(parameterName)
    {
        // Make sure it goes through value update.
        Value = value;
    }

    /// <summary>Creates a new instance of a <see cref="T:Slon.SlonParameter" /> object.</summary>
    /// <returns>The new instance.</returns>
    public new SlonParameter Clone() => (SlonParameter)CloneCore();

    private protected override object? ValueCore
    {
        get => _value;
        set
        {
            TrackObjectValueTypeChange(_value, value);
            _value = value;
        }
    }

    private protected override SlonDbParameter CloneCore() => Clone(new SlonParameter { ValueCore = ValueCore });

    internal override void Bind(ref SerializerParameterWriterStrategy.ParameterBinder binder)
        => binder.Bind(_value);
    internal override void Write(ref SerializerParameterWriterStrategy.ParameterWriter writer)
        => writer.Write(_value);
    internal override void WriteAsync(ref SerializerParameterWriterStrategy.AsyncParameterWriter writer)
        => writer.Write(_value);

    private protected override void SetOutputValue(object? value)
    {
        if (Direction is not (ParameterDirection.Output or ParameterDirection.InputOutput))
            throw new InvalidOperationException("Cannot change value of a non-output parameter.");

        // An input/output result can become the value of a later execution.
        if (Direction is ParameterDirection.InputOutput)
            ValueCore = value;
        else
            _value = value;
    }

    void IParameter<object>.SetOutputResult(object? value) => SetOutputValue(value);
}


/// <inheritdoc cref="Slon.SlonDbParameter" />
public sealed class SlonParameter<T> : SlonDbParameter, IDbDataParameter<T>, IParameter<T>
{
    T? _value;

    public SlonParameter() {}
    public SlonParameter(T? value) : this(string.Empty, value) {}
    public SlonParameter(string parameterName, T? value)
        :base(parameterName)
    {
        // Make sure it goes through value update.
        Value = value;
    }

    /// <summary>Creates a new instance of a <see cref="Slon.SlonParameter{T}" /> object.</summary>
    /// <returns>The new instance.</returns>
    public new SlonParameter<T> Clone() => (SlonParameter<T>)CloneCore();

    public new T? Value
    {
        get => _value;
        set
        {
            ThrowIfReadOnly();
            SetValue(value);
        }
    }

    void SetValue(T? value)
    {
        if (typeof(T) == typeof(object))
            TrackObjectValueTypeChange(_value, value);
        _value = value;
    }

    private protected override Type StaticValueType => typeof(T);
    private protected override object? ValueCore { get => Value; set => Value = (T?)value; }
    private protected override SlonDbParameter CloneCore() => Clone(new SlonParameter<T> { _value = _value });
    private protected override void SetOutputValue(object? value) => ((IParameter<T>)this).SetOutputResult((T?)value);

    internal override void Bind(ref SerializerParameterWriterStrategy.ParameterBinder binder)
        => binder.Bind(_value);
    internal override void Write(ref SerializerParameterWriterStrategy.ParameterWriter writer)
        => writer.Write(_value);
    internal override void WriteAsync(ref SerializerParameterWriterStrategy.AsyncParameterWriter writer)
        => writer.Write(_value);

    T? IParameter<T>.Value => _value;
    void IParameter<T>.SetOutputResult(T? value)
    {
        if (Direction is not (ParameterDirection.Output or ParameterDirection.InputOutput))
            ThrowHelper.ThrowInvalidOperation("Cannot change value of a non-output parameter.");

        SetValue(value);
    }
}
