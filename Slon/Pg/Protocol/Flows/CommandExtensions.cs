using System.Diagnostics;
using System.Runtime.CompilerServices;
using Slon.Runtime.CompilerServices;

namespace Slon.Pg.Protocol.Flows;

static class CommandExtensions
{
    // TODO this requires quite some work, including having to support text format param/field values. Maybe not worth it to support at all for queries.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsSimple(this in Command command) =>
        command.PreferSimple && command.WithSync && !command.DescribeOnly && !command.Descriptor.IsPrepared && command.Descriptor.ParameterTypes.Count is 0;

    // Sync/async pair at the command-list (full composition) level. No *Auto wrapper here.
    // Callers picking sync vs async make that choice once at this level rather than threading
    // a mode flag through every encoder helper underneath. Keeping the list loop in this state
    // machine avoids one nested async invocation per command while preserving the one-command path.
    public static ValueTask WriteCommandsAsync(this CommandList commands, PgEncoder encoder, bool appendSync,
        ParameterWriterStrategy? parameterWriterStrategy, CancellationToken cancellationToken = default)
    {
        for (var i = 0; i < commands.Count; i++)
        {
            var command = commands[i];
            var descriptor = command.Descriptor;
            if (!descriptor.IsPrepared || command.Parameters.Count is not 0
                || descriptor.ParameterTypes.Count is not 0 || command.ResultFormats.Length is not 0)
                return WriteCommandsAsyncCore(commands, encoder, appendSync, parameterWriterStrategy, cancellationToken, i);

            encoder.WriteBind(descriptor.CommandName);
            CompletePreparedWrite(command, descriptor, encoder);
        }
        if (appendSync)
            encoder.WriteSync();
        return encoder.FlushAsync(cancellationToken);

        static async ValueTask WriteCommandsAsyncCore(CommandList commands, PgEncoder encoder, bool appendSync,
            ParameterWriterStrategy? parameterWriterStrategy, CancellationToken cancellationToken, int startIndex)
        {
            for (var i = startIndex; i < commands.Count; i++)
            {
                var command = commands[i];
                var descriptor = command.Descriptor;
                if (command.DescribeForPreparation)
                {
                    Debug.Assert(!descriptor.IsPrepared);
                    await encoder.WriteParseAsync(descriptor.UnpreparedCommandText, descriptor.CommandName,
                        descriptor.ParameterTypes, cancellationToken).ConfigureAwait(false);
                    encoder.WriteDescribe(descriptor.CommandName, portalName: false);
                    var parameters = descriptor.ParameterTypes.CreateNullParameters();
                    encoder.WriteBind(descriptor.CommandName,
                        parameters: parameters, resultFormats: command.ResultFormats,
                        parameterWriterStrategy: parameterWriterStrategy);
                    encoder.WriteDescribe();
                    if (command.WithSync)
                        encoder.WriteSync();
                }
                else if (descriptor.IsPrepared)
                {
                    var parameters = ResolveParameters(command, descriptor, parameterWriterStrategy);
                    await encoder.WriteBindAsync(descriptor.CommandName, parameters: parameters,
                        resultFormats: command.ResultFormats, parameterWriterStrategy: parameterWriterStrategy,
                        cancellationToken: cancellationToken).ConfigureAwait(false);
                    CompletePreparedWrite(command, descriptor, encoder);
                }
                else if (command.IsSimple())
                {
                    await encoder.WriteQueryAsync(descriptor.UnpreparedCommandText).ConfigureAwait(false);
                }
                else
                {
                    var parameters = ResolveParameters(command, descriptor, parameterWriterStrategy);

                    // Extended unprepared.
                    await encoder.WriteParseAsync(descriptor.UnpreparedCommandText, descriptor.CommandName, descriptor.ParameterTypes, cancellationToken: cancellationToken).ConfigureAwait(false);
                    await encoder.WriteBindAsync(descriptor.CommandName, parameters: parameters,
                        resultFormats: command.ResultFormats, parameterWriterStrategy: parameterWriterStrategy,
                        cancellationToken: cancellationToken).ConfigureAwait(false);
                    encoder.WriteDescribe();
                    if (!command.DescribeOnly)
                        encoder.WriteExecute();
                    if (command.WithSync)
                        encoder.WriteSync();
                }
            }
            if (appendSync)
                encoder.WriteSync();
            await encoder.FlushAsync(cancellationToken).ConfigureAwait(false);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static void CompletePreparedWrite(in Command command, in CommandDescriptor descriptor, PgEncoder encoder)
        {
            if (command.DescribeOnly)
                encoder.WriteDescribe();
            else
            {
                if (descriptor.PreparedRowDescription is null)
                    encoder.WriteDescribe();
                encoder.WriteExecute();
            }

            if (command.WithSync)
                encoder.WriteSync();
        }
    }

    // Sync coroutine variant, composes the encoder's *Resumable primitives into a single
    // async state machine for the whole command. Any WouldBlock from a mid-message auto-flush
    // (post-serializer) suspends here. The resumption picks up at the exact same composition
    // point with all state intact. Cf. encoder.WriteQueryResumable for the per-message contract.
    public static async ValueTask WriteCommandsResumable(this CommandList commands, PgEncoder encoder,
        bool appendSync, ParameterWriterStrategy? parameterWriterStrategy)
    {
        for (var i = 0; i < commands.Count; i++)
        {
            var command = commands[i];
            var descriptor = command.Descriptor;
            if (command.DescribeForPreparation)
            {
                Debug.Assert(!descriptor.IsPrepared);
                await encoder.WriteParseResumable(descriptor.UnpreparedCommandText, descriptor.CommandName,
                    descriptor.ParameterTypes).ConfigureAwait(false);
                encoder.WriteDescribe(descriptor.CommandName, portalName: false);
                var parameters = descriptor.ParameterTypes.CreateNullParameters();
                encoder.WriteBind(descriptor.CommandName,
                    parameters: parameters, resultFormats: command.ResultFormats,
                    parameterWriterStrategy: parameterWriterStrategy);
                encoder.WriteDescribe();
                if (command.WithSync)
                    encoder.WriteSync();
            }
            else if (descriptor.IsPrepared)
            {
                var parameters = ResolveParameters(command, descriptor, parameterWriterStrategy);
                await encoder.WriteBindResumable(descriptor.CommandName, parameters: parameters,
                    resultFormats: command.ResultFormats,
                    parameterWriterStrategy: parameterWriterStrategy).ConfigureAwait(false);

                if (command.DescribeOnly)
                    encoder.WriteDescribe();
                else
                {
                    if (descriptor.PreparedRowDescription is null)
                        encoder.WriteDescribe();

                    encoder.WriteExecute();
                }

                if (command.WithSync)
                    encoder.WriteSync();
            }
            else if (command.IsSimple())
            {
                await encoder.WriteQueryResumable(descriptor.UnpreparedCommandText).ConfigureAwait(false);
            }
            else
            {
                var parameters = ResolveParameters(command, descriptor, parameterWriterStrategy);

                await encoder.WriteParseResumable(descriptor.UnpreparedCommandText, descriptor.CommandName, descriptor.ParameterTypes).ConfigureAwait(false);
                await encoder.WriteBindResumable(descriptor.CommandName, parameters: parameters,
                    resultFormats: command.ResultFormats,
                    parameterWriterStrategy: parameterWriterStrategy).ConfigureAwait(false);
                encoder.WriteDescribe();
                if (!command.DescribeOnly)
                    encoder.WriteExecute();
                if (command.WithSync)
                    encoder.WriteSync();
            }
        }
        if (appendSync)
            encoder.WriteSync();
        await encoder.FlushResumable().ConfigureAwait(false);
    }

    static ParameterSource ResolveParameters(in Command command, in CommandDescriptor descriptor,
        ParameterWriterStrategy? strategy)
    {
        var parameters = command.Parameters;
        if (parameters.Count == descriptor.ParameterTypes.Count)
            return parameters;
        if (!command.DescribeOnly)
        {
            ThrowHelper.ThrowInvalidOperation(descriptor.IsPrepared
                ? $"Prepared command parameter count mismatch with descriptor, expected: ${descriptor.ParameterTypes.Count}."
                : "Parameter count mismatch between descriptor and command, unprepared parameter sources must match.");
        }
        return descriptor.ParameterTypes.CreateNullParameters();
    }

    /// Reads the response messages up to (but not including) the actual row stream / CommandComplete.
    /// Async variant. Yields on missing bytes. Caller picks this vs <see cref="ReadUntilExecute"/>
    /// based on per-call <c>IsAsync</c>. No wrapper method, dispatch inlined at the call site.
    public static ValueTask<(PgError?, RowDescription?)> ReadUntilExecuteAsync(
        this in Command command, PgDecoder decoder, RowDescription rowDescription)
    {
        if (command.IsSimple())
            return ReadSimpleAsync(decoder, rowDescription);

        return ReadExtendedAsync(decoder, rowDescription,
            readParse: !command.Descriptor.IsPrepared,
            readDescribe: command.DescribeOnly || !command.Descriptor.IsPrepared || command.Descriptor.PreparedRowDescription is null,
            readExecute: !command.DescribeOnly);

        [AsyncMethodBuilder(typeof(NonContextRestoringPoolingValueTaskMethodBuilder<>))]
        static async ValueTask<(PgError?, RowDescription?)> ReadSimpleAsync(
            PgDecoder decoder, RowDescription rowDescription)
        {
            if (!decoder.TryGetNext(out var message))
            {
                if (!await decoder.MoveNextAsync().ConfigureAwait(false))
                    decoder.ThrowUnexpectedEof();
                message = decoder.Current;
            }
            if (message.EnsureExpectedOrError(PgTypes.BackendType.RowDescription, PgTypes.BackendType.NoData)
                    is var result && result.Error is { } describeError)
                return (describeError, null);

            RowDescription? requestedRowDescription;
            switch (result.Type)
            {
                case PgTypes.BackendType.RowDescription:
                    requestedRowDescription = rowDescription;
                    requestedRowDescription.Initialize(message.BodyReader, decoder.ClientEncoding);
                    break;
                case PgTypes.BackendType.NoData:
                    // Nothing to do for NoData.
                    Debug.Assert(message.Header.BodyLength is 0);
                    requestedRowDescription = RowDescription.NoData;
                    break;
                default:
                    ThrowHelper.ThrowUnhandledCase(result.Type);
                    return default!;
            }

            if (!decoder.TryGetNext(out message))
            {
                if (!await decoder.MoveNextAsync().ConfigureAwait(false))
                    decoder.ThrowUnexpectedEof();
                message = decoder.Current;
            }
            message.DebugEnsureExpected(PgTypes.BackendType.DataRow, PgTypes.BackendType.CommandComplete);
            return (null, requestedRowDescription);
        }

        [AsyncMethodBuilder(typeof(NonContextRestoringPoolingValueTaskMethodBuilder<>))]
        static async ValueTask<(PgError?, RowDescription?)> ReadExtendedAsync(
            PgDecoder decoder, RowDescription rowDescription, bool readParse, bool readDescribe, bool readExecute)
        {
            BackendMessage message;
            if (readParse)
            {
                if (!decoder.TryGetNext(out message))
                {
                    if (!await decoder.MoveNextAsync().ConfigureAwait(false))
                        decoder.ThrowUnexpectedEof();
                    message = decoder.Current;
                }
                if (message.EnsureExpectedOrError(PgTypes.BackendType.ParseComplete) is { } parseError)
                    return (parseError, null);

                // Nothing to do for ParseComplete.
                Debug.Assert(message.Header.BodyLength is 0);
            }

            if (!decoder.TryGetNext(out message))
            {
                if (!await decoder.MoveNextAsync().ConfigureAwait(false))
                    decoder.ThrowUnexpectedEof();
                message = decoder.Current;
            }
            if (message.EnsureExpectedOrError(PgTypes.BackendType.BindComplete) is { } bindError)
                return (bindError, null);

            // Nothing to do for BindComplete.
            Debug.Assert(message.Header.BodyLength is 0);

            RowDescription? requestedRowDescription = null;
            if (readDescribe)
            {
                if (!decoder.TryGetNext(out message))
                {
                    if (!await decoder.MoveNextAsync().ConfigureAwait(false))
                        decoder.ThrowUnexpectedEof();
                    message = decoder.Current;
                }
                if (message.EnsureExpectedOrError(PgTypes.BackendType.RowDescription, PgTypes.BackendType.NoData)
                        is var result && result.Error is { } describeError)
                    return (describeError, null);

                switch (result.Type)
                {
                    case PgTypes.BackendType.RowDescription:
                        requestedRowDescription = rowDescription;
                        requestedRowDescription.Initialize(message.BodyReader, decoder.ClientEncoding);
                        break;
                    case PgTypes.BackendType.NoData:
                        // Nothing to do for NoData.
                        Debug.Assert(message.Header.BodyLength is 0);
                        requestedRowDescription = RowDescription.NoData;
                        break;
                    default:
                        ThrowHelper.ThrowUnhandledCase(result.Type);
                        return default!;
                }
            }

            if (readExecute)
            {
                if (!decoder.TryGetNext(out message))
                {
                    if (!await decoder.MoveNextAsync().ConfigureAwait(false))
                        decoder.ThrowUnexpectedEof();
                    message = decoder.Current;
                }
                message.DebugEnsureExpected(PgTypes.BackendType.DataRow, PgTypes.BackendType.CommandComplete);
            }

            Debug.Assert(!readDescribe || requestedRowDescription is not null);
            return (null, requestedRowDescription);
        }
    }

    /// Sync counterpart to <see cref="ReadUntilExecuteAsync"/>. Blocks via <see cref="PgDecoder.GetNext"/>
    /// when bytes aren't already buffered. Safe on the executor's thread when that thread is dedicated
    /// to this flow's processing (handoff mode sync, or dedicated mode + <c>MoveNext()</c> per-call).
    public static (PgError?, RowDescription?) ReadUntilExecute(
        this in Command command, PgDecoder decoder, RowDescription rowDescription)
    {
        if (command.IsSimple())
            return ReadSimpleSync(decoder, rowDescription);

        return ReadExtendedSync(decoder, rowDescription,
            readParse: !command.Descriptor.IsPrepared,
            readDescribe: command.DescribeOnly || !command.Descriptor.IsPrepared || command.Descriptor.PreparedRowDescription is null,
            readExecute: !command.DescribeOnly);

        static (PgError?, RowDescription?) ReadSimpleSync(PgDecoder decoder, RowDescription rowDescription)
        {
            var message = decoder.TryGetNext(out var m) ? m : decoder.GetNext();
            if (message.EnsureExpectedOrError(PgTypes.BackendType.RowDescription, PgTypes.BackendType.NoData)
                    is var result && result.Error is { } describeError)
                return (describeError, null);

            RowDescription? requestedRowDescription;
            switch (result.Type)
            {
                case PgTypes.BackendType.RowDescription:
                    requestedRowDescription = rowDescription;
                    requestedRowDescription.Initialize(message.BodyReader, decoder.ClientEncoding);
                    break;
                case PgTypes.BackendType.NoData:
                    Debug.Assert(message.Header.BodyLength is 0);
                    requestedRowDescription = RowDescription.NoData;
                    break;
                default:
                    ThrowHelper.ThrowUnhandledCase(result.Type);
                    return default!;
            }

            message = decoder.TryGetNext(out m) ? m : decoder.GetNext();
            message.DebugEnsureExpected(PgTypes.BackendType.DataRow, PgTypes.BackendType.CommandComplete);
            return (null, requestedRowDescription);
        }

        static (PgError?, RowDescription?) ReadExtendedSync(
            PgDecoder decoder, RowDescription rowDescription, bool readParse, bool readDescribe, bool readExecute)
        {
            BackendMessage message;
            if (readParse)
            {
                message = decoder.TryGetNext(out var m) ? m : decoder.GetNext();
                if (message.EnsureExpectedOrError(PgTypes.BackendType.ParseComplete) is { } parseError)
                    return (parseError, null);

                Debug.Assert(message.Header.BodyLength is 0);
            }

            message = decoder.TryGetNext(out var bm) ? bm : decoder.GetNext();
            if (message.EnsureExpectedOrError(PgTypes.BackendType.BindComplete) is { } bindError)
                return (bindError, null);

            Debug.Assert(message.Header.BodyLength is 0);

            RowDescription? requestedRowDescription = null;
            if (readDescribe)
            {
                message = decoder.TryGetNext(out var dm) ? dm : decoder.GetNext();
                if (message.EnsureExpectedOrError(PgTypes.BackendType.RowDescription, PgTypes.BackendType.NoData)
                        is var result && result.Error is { } describeError)
                    return (describeError, null);

                switch (result.Type)
                {
                    case PgTypes.BackendType.RowDescription:
                        requestedRowDescription = rowDescription;
                        requestedRowDescription.Initialize(message.BodyReader, decoder.ClientEncoding);
                        break;
                    case PgTypes.BackendType.NoData:
                        Debug.Assert(message.Header.BodyLength is 0);
                        requestedRowDescription = RowDescription.NoData;
                        break;
                    default:
                        ThrowHelper.ThrowUnhandledCase(result.Type);
                        return default!;
                }
            }

            if (readExecute)
            {
                message = decoder.TryGetNext(out var em) ? em : decoder.GetNext();
                message.DebugEnsureExpected(PgTypes.BackendType.DataRow, PgTypes.BackendType.CommandComplete);
            }

            Debug.Assert(!readDescribe || requestedRowDescription is not null);
            return (null, requestedRowDescription);
        }
    }

    public static async ValueTask<(PgError?, ParameterTypeList, RowDescription?)> ReadPreparationDescriptionAsync(
        this Command command, PgDecoder decoder, RowDescription rowDescription)
    {
        Debug.Assert(command.DescribeForPreparation);
        if (!decoder.TryGetNext(out var message))
        {
            if (!await decoder.MoveNextAsync().ConfigureAwait(false))
                decoder.ThrowUnexpectedEof();
            message = decoder.Current;
        }
        if (message.EnsureExpectedOrError(PgTypes.BackendType.ParseComplete) is { } parseError)
            return (parseError, default, null);

        if (!decoder.TryGetNext(out message))
        {
            if (!await decoder.MoveNextAsync().ConfigureAwait(false))
                decoder.ThrowUnexpectedEof();
            message = decoder.Current;
        }
        if (message.EnsureExpectedOrError(PgTypes.BackendType.ParameterDescription) is { } parameterError)
            return (parameterError, default, null);
        var parameterTypes = ParameterDescription.Parse(message.BodyReader);

        if (!decoder.TryGetNext(out message))
        {
            if (!await decoder.MoveNextAsync().ConfigureAwait(false))
                decoder.ThrowUnexpectedEof();
            message = decoder.Current;
        }
        if (message.EnsureExpectedOrError(PgTypes.BackendType.RowDescription, PgTypes.BackendType.NoData)
                is var result && result.Error is { } describeError)
            return (describeError, default, null);

        // Statement Describe supplies parameter inference. Its RowDescription always reports text formats,
        // so consume it and retain the concrete description from the NULL-bound binary portal below.
        if (result.Type is PgTypes.BackendType.NoData)
            Debug.Assert(message.Header.BodyLength is 0);

        if (!decoder.TryGetNext(out message))
        {
            if (!await decoder.MoveNextAsync().ConfigureAwait(false))
                decoder.ThrowUnexpectedEof();
            message = decoder.Current;
        }
        if (message.EnsureExpectedOrError(PgTypes.BackendType.BindComplete) is { } bindError)
            return (bindError, default, null);

        if (!decoder.TryGetNext(out message))
        {
            if (!await decoder.MoveNextAsync().ConfigureAwait(false))
                decoder.ThrowUnexpectedEof();
            message = decoder.Current;
        }
        if (message.EnsureExpectedOrError(PgTypes.BackendType.RowDescription, PgTypes.BackendType.NoData)
                is var portalResult && portalResult.Error is { } portalDescribeError)
            return (portalDescribeError, default, null);

        RowDescription requestedRowDescription;
        if (portalResult.Type is PgTypes.BackendType.RowDescription)
        {
            requestedRowDescription = rowDescription;
            requestedRowDescription.Initialize(message.BodyReader, decoder.ClientEncoding);
        }
        else
        {
            Debug.Assert(portalResult.Type is PgTypes.BackendType.NoData && message.Header.BodyLength is 0);
            requestedRowDescription = RowDescription.NoData;
        }
        return (null, parameterTypes, requestedRowDescription);
    }

    public static (PgError?, ParameterTypeList, RowDescription?) ReadPreparationDescription(
        this in Command command, PgDecoder decoder, RowDescription rowDescription)
    {
        Debug.Assert(command.DescribeForPreparation);
        var message = decoder.TryGetNext(out var current) ? current : decoder.GetNext();
        if (message.EnsureExpectedOrError(PgTypes.BackendType.ParseComplete) is { } parseError)
            return (parseError, default, null);

        message = decoder.TryGetNext(out current) ? current : decoder.GetNext();
        if (message.EnsureExpectedOrError(PgTypes.BackendType.ParameterDescription) is { } parameterError)
            return (parameterError, default, null);
        var parameterTypes = ParameterDescription.Parse(message.BodyReader);

        message = decoder.TryGetNext(out current) ? current : decoder.GetNext();
        if (message.EnsureExpectedOrError(PgTypes.BackendType.RowDescription, PgTypes.BackendType.NoData)
                is var result && result.Error is { } describeError)
            return (describeError, default, null);

        // Statement Describe supplies parameter inference. Its RowDescription always reports text formats,
        // so consume it and retain the concrete description from the NULL-bound binary portal below.
        if (result.Type is PgTypes.BackendType.NoData)
            Debug.Assert(message.Header.BodyLength is 0);

        message = decoder.TryGetNext(out current) ? current : decoder.GetNext();
        if (message.EnsureExpectedOrError(PgTypes.BackendType.BindComplete) is { } bindError)
            return (bindError, default, null);

        message = decoder.TryGetNext(out current) ? current : decoder.GetNext();
        if (message.EnsureExpectedOrError(PgTypes.BackendType.RowDescription, PgTypes.BackendType.NoData)
                is var portalResult && portalResult.Error is { } portalDescribeError)
            return (portalDescribeError, default, null);

        RowDescription requestedRowDescription;
        if (portalResult.Type is PgTypes.BackendType.RowDescription)
        {
            requestedRowDescription = rowDescription;
            requestedRowDescription.Initialize(message.BodyReader, decoder.ClientEncoding);
        }
        else
        {
            Debug.Assert(portalResult.Type is PgTypes.BackendType.NoData && message.Header.BodyLength is 0);
            requestedRowDescription = RowDescription.NoData;
        }
        return (null, parameterTypes, requestedRowDescription);
    }

    /// Any command without a Sync boundary - or when its transaction status is not clean - completing with an error
    /// will result in the return of this error, alerting about changes in the upcoming message stream.
    /// If more commands before the next Sync are expected these would be discarded and absent from the message stream.
    /// In case of an error inside an explicit transaction block all commands until rollback are affected.
    public static (PgError, TransactionStatus)? Complete(this in Command command, PgDecoder decoder)
    {
        PgError? errorMessage = null;
        if (!command.DescribeOnly)
        {
            // https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
            // "Therefore, an Execute phase is always terminated by the appearance of exactly one of these messages:
            // CommandComplete, EmptyQueryResponse (if the portal was created from an empty query string), ErrorResponse, or PortalSuspended"
            (errorMessage, _) = decoder.Current.EnsureExpectedOrError(
                PgTypes.BackendType.CommandComplete, PgTypes.BackendType.EmptyQueryResponse, PgTypes.BackendType.PortalSuspended);
        }

        if (!command.WithSync)
            return errorMessage is not null ? (errorMessage, TransactionStatus.Unknown) : null;

        // Reading the following RFQ may retire the batch which owns the ErrorResponse body.
        errorMessage = errorMessage?.Preserve();
        var message = decoder.GetNext();
        // When an error is returned while we expect an RFQ it's going to be some unexpected server issue, just throw it.
        if (message.TryCreateError(out var syncError))
            PgErrorException.Throw(syncError);

        var transactionStatus = ReadyForQueryMessage.Create(message).TransactionStatus;
        return errorMessage is not null && transactionStatus is TransactionStatus.Error
            ? (errorMessage, transactionStatus)
            : null;
    }

    /// Any command without a Sync boundary - or when its transaction status is not clean - completing with an error
    /// will result in the return of this error, alerting about changes in the upcoming message stream.
    /// If more commands before the next Sync are expected these would be discarded and absent from the message stream.
    /// In case of an error inside an explicit transaction block all commands until rollback are affected.
    public static ValueTask<(PgError, TransactionStatus)?> CompleteAsync(this in Command command, PgDecoder decoder)
    {
        PgError? errorMessage = null;
        if (!command.DescribeOnly)
        {
            // https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
            // "Therefore, an Execute phase is always terminated by the appearance of exactly one of these messages:
            // CommandComplete, EmptyQueryResponse (if the portal was created from an empty query string), ErrorResponse, or PortalSuspended"
            (errorMessage, _) = decoder.Current.EnsureExpectedOrError(
                PgTypes.BackendType.CommandComplete, PgTypes.BackendType.EmptyQueryResponse, PgTypes.BackendType.PortalSuspended);
        }

        if (!command.WithSync)
            return errorMessage is not null ? new((errorMessage, TransactionStatus.Unknown)) : new(result: null);

        // Reading the following RFQ may retire the batch which owns the ErrorResponse body.
        errorMessage = errorMessage?.Preserve();
        if (!decoder.TryGetNext(out var message))
            return Core(decoder, errorMessage);

        // When an error is returned while we expect an RFQ it's going to be some unexpected server issue, just throw it.
        if (message.TryCreateError(out var syncError))
            PgErrorException.Throw(syncError);

        var transactionStatus = ReadyForQueryMessage.Create(message).TransactionStatus;
        return errorMessage is not null && transactionStatus is TransactionStatus.Error
            ? new((errorMessage, transactionStatus))
            : new(result: null);

        [AsyncMethodBuilder(typeof(NonContextRestoringPoolingValueTaskMethodBuilder<>))]
        static async ValueTask<(PgError, TransactionStatus)?> Core(PgDecoder decoder, PgError? errorMessage)
        {
            var message = await decoder.GetNextAsync().ConfigureAwait(false);
            // When an error is returned while we expect an RFQ it's going to be some unexpected server issue, just throw it.
            if (message.TryCreateError(out var syncError))
                PgErrorException.Throw(syncError);

            var transactionStatus = ReadyForQueryMessage.Create(message).TransactionStatus;
            return errorMessage is not null && transactionStatus is TransactionStatus.Error
                ? (errorMessage, transactionStatus)
                : null;
        }
    }

}
