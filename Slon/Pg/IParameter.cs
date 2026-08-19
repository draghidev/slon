namespace Slon.Pg;

// Shared exchange type to bridge ADO.NET and protocol layers without re-boxing, allowing us to surface more info etc..
interface IParameter
{
    ParameterKind Kind { get; }
    // Name is used to link up columns with output parameters.
    string Name { get; }

    // The actual runtime type of the value.
    Type StaticValueType { get; }

    // Value will be boxed on return if the static type is a value type.
    object? Value { get; }
    void SetOutputResult(object? value);
}

interface IParameter<T> : IParameter
{
    new T? Value { get; }
    void SetOutputResult(T? value);
}

enum ParameterKind: byte
{
    Input = 1,
    Output = 2,
    InputOutput = 3,
    ReturnValue = 6
}
