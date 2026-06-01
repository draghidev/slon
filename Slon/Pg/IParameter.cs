namespace Slon.Pg;

/// Support type for reading a value stored on an instance of IParameter{T}, allows values to stay unboxed if they are.
interface IParameterValueReader
{
    void Read<T>(T? value);
}

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

    /// Apply a reader to a value stored on an instance of IParameter{T}, allows values to stay unboxed if they are.
    void ApplyReader<TReader>(ref TReader reader) where TReader: IParameterValueReader, allows ref struct;
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
