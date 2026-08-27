using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Slon.Text;

namespace Slon.Pg;

[Experimental(ExperimentalDiagnostics.PostgreSqlLowerLayer)]
public readonly struct CommandDescriptor
{
    readonly object? _rowDescriptionOrCommandText;
    readonly EncodedCString _commandName;
    readonly ParameterTypeList _parameterTypes;

    CommandDescriptor(EncodedCString commandName, ParameterTypeList parameterTypes, RowDescription? rowDescription)
    {
        Debug.Assert(Unsafe.SizeOf<CommandDescriptor>() <= 40);
        if (commandName.IsDefault)
            ThrowHelper.ThrowArgumentException(nameof(commandName), "Command name must be provided.");
        CommandName = commandName;
        _parameterTypes = parameterTypes;
        _rowDescriptionOrCommandText = rowDescription;
    }

    CommandDescriptor(string commandText, ParameterTypeList parameterTypes, EncodedCString commandName)
    {
        ArgumentNullException.ThrowIfNull(commandText);
        _rowDescriptionOrCommandText = commandText;
        _parameterTypes = parameterTypes;
        CommandName = commandName;
    }

    [MemberNotNullWhen(false, nameof(UnpreparedCommandText))]
    public bool IsPrepared => _rowDescriptionOrCommandText is not string;

    public EncodedCString CommandName
    {
        get => _commandName;
        init => _commandName = value;
    }

    public ParameterTypeList ParameterTypes => _parameterTypes;

    /// Can be null when the row description is indeterminate (e.g. due to an error before describe).
    public RowDescription? PreparedRowDescription
    {
        get
        {
            if (!IsPrepared)
                ThrowNotPrepared();

            return _rowDescriptionOrCommandText as RowDescription;

            [DoesNotReturn]
            static void ThrowNotPrepared() => throw new InvalidOperationException("Statement is not prepared.");
        }
    }

    public string UnpreparedCommandText
    {
        get
        {
            if (IsPrepared)
                ThrowPrepared();

            Debug.Assert(_rowDescriptionOrCommandText is not null);
            return (string)_rowDescriptionOrCommandText;

            [DoesNotReturn]
            static void ThrowPrepared() => throw new InvalidOperationException("Statement is prepared.");
        }
    }

    public static CommandDescriptor CreatePrepared(EncodedCString commandName, ParameterTypeList parameterTypes, RowDescription? rowDescription)
        => new(commandName, parameterTypes, rowDescription);

    public static CommandDescriptor Create(string commandText, ParameterTypeList parameterTypes = default, EncodedCString commandName = default)
        => new(commandText, parameterTypes, commandName);
}
