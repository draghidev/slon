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

    // Sync/async pair at the Command (full composition) level. No *Auto wrapper here.
    // Callers picking sync vs async make that choice once at this level rather than threading
    // a mode flag through every encoder helper underneath. WriteAllCommands always picks
    // WriteAsync for the back-pressure handoff to work. Mode-adaptive callers do
    // `command.IsAsync ? command.WriteAsync(...) : command.Write(...)` themselves.
    public static async ValueTask WriteAsync(this Command command, PgEncoder encoder, CancellationToken cancellationToken = default)
    {
        var descriptor = command.Descriptor;
        if (descriptor.IsPrepared)
        {
            var parameters = command.Parameters;
            if (parameters.Length != descriptor.ParameterTypes.Count)
            {
                if (!command.DescribeOnly)
                    ThrowHelper.ThrowInvalidOperation($"Prepared command parameter count mismatch with descriptor, expected: ${descriptor.ParameterTypes.Count}.");

                parameters = descriptor.ParameterTypes.ToDbNullParameterList();
            }

            await encoder.WriteBindAsync(descriptor.CommandName, parameters: parameters, cancellationToken: cancellationToken).ConfigureAwait(false);

            // We always re-request the description for describe only, even for prepared statements.
            if (command.DescribeOnly)
            {
                encoder.WriteDescribe();
            }
            else
            {
                // We also need to re-describe if the description returned by the previous preparation was indeterminate.
                if (descriptor.IsPrepared && descriptor.PreparedRowDescription is null)
                    encoder.WriteDescribe();

                encoder.WriteExecute();
            }

            if (command.WithSync)
                encoder.WriteSync();
        }
        else if (command.IsSimple())
        {
            await encoder.WriteQueryAsync(descriptor.UnpreparedCommandText).ConfigureAwait(false);
        }
        else
        {
            var parameters = command.Parameters;
            if (parameters.Length != descriptor.ParameterTypes.Count)
            {
                if (!command.DescribeOnly)
                    ThrowHelper.ThrowInvalidOperation("Parameter count mismatch between descriptor and command, unprepared parameter sources must match.");

                parameters = descriptor.ParameterTypes.ToDbNullParameterList();
            }

            // Extended unprepared.
            await encoder.WriteParseAsync(descriptor.UnpreparedCommandText, descriptor.CommandName, descriptor.ParameterTypes, cancellationToken: cancellationToken).ConfigureAwait(false);
            await encoder.WriteBindAsync(descriptor.CommandName, parameters: parameters, cancellationToken: cancellationToken).ConfigureAwait(false);
            encoder.WriteDescribe();
            if (!command.DescribeOnly)
                encoder.WriteExecute();
            if (command.WithSync)
                encoder.WriteSync();
        }
    }

    // Sync coroutine variant, composes the encoder's *Resumable primitives into a single
    // async state machine for the whole command. Any WouldBlock from a mid-message auto-flush
    // (post-serializer) suspends here. The resumption picks up at the exact same composition
    // point with all state intact. Cf. encoder.WriteQueryResumable for the per-message contract.
    public static async ValueTask WriteResumable(this Command command, PgEncoder encoder)
    {
        var descriptor = command.Descriptor;
        if (descriptor.IsPrepared)
        {
            var parameters = command.Parameters;
            if (parameters.Length != descriptor.ParameterTypes.Count)
            {
                if (!command.DescribeOnly)
                    ThrowHelper.ThrowInvalidOperation($"Prepared command parameter count mismatch with descriptor, expected: ${descriptor.ParameterTypes.Count}.");

                parameters = descriptor.ParameterTypes.ToDbNullParameterList();
            }

            await encoder.WriteBindResumable(descriptor.CommandName, parameters: parameters).ConfigureAwait(false);

            if (command.DescribeOnly)
            {
                encoder.WriteDescribe();
            }
            else
            {
                if (descriptor.IsPrepared && descriptor.PreparedRowDescription is null)
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
            var parameters = command.Parameters;
            if (parameters.Length != descriptor.ParameterTypes.Count)
            {
                if (!command.DescribeOnly)
                    ThrowHelper.ThrowInvalidOperation("Parameter count mismatch between descriptor and command, unprepared parameter sources must match.");

                parameters = descriptor.ParameterTypes.ToDbNullParameterList();
            }

            await encoder.WriteParseResumable(descriptor.UnpreparedCommandText, descriptor.CommandName, descriptor.ParameterTypes).ConfigureAwait(false);
            await encoder.WriteBindResumable(descriptor.CommandName, parameters: parameters).ConfigureAwait(false);
            encoder.WriteDescribe();
            if (!command.DescribeOnly)
                encoder.WriteExecute();
            if (command.WithSync)
                encoder.WriteSync();
        }
    }

    // TODO make actually auto (e.g. sync compat)
    public static ValueTask<(PgError?, RowDescription?)> ReadUntilExecuteAuto(this in Command command, PgDecoder decoder)
    {
        return command.IsSimple()
            ? ReadSimple(decoder)
            : ReadExtended(decoder,
                readParse: !command.Descriptor.IsPrepared,
                readDescribe: command.DescribeOnly || !command.Descriptor.IsPrepared || command.Descriptor.PreparedRowDescription is null,
                readExecute: !command.DescribeOnly);

        [AsyncMethodBuilder(typeof(NonContextRestoringPoolingValueTaskMethodBuilder<>))]
        static async ValueTask<(PgError?, RowDescription?)> ReadSimple(PgDecoder decoder)
        {
            if (!decoder.TryGetNext(out var message))
                message = await decoder.GetNextAsync().ConfigureAwait(false);
            if (message.EnsureExpectedOrError(PgTypes.BackendType.RowDescription, PgTypes.BackendType.NoData)
                    is var result && result.Error is { } describeError)
                return (describeError, null);

            RowDescription? requestedRowDescription;
            switch (result.Type)
            {
                case PgTypes.BackendType.RowDescription:
                    requestedRowDescription = new RowDescription();
                    requestedRowDescription.Initialize(message.BodyReader);
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
                message = await decoder.GetNextAsync().ConfigureAwait(false);
            message.DebugEnsureExpected(PgTypes.BackendType.DataRow, PgTypes.BackendType.CommandComplete);
            return (null, requestedRowDescription);
        }

        [AsyncMethodBuilder(typeof(NonContextRestoringPoolingValueTaskMethodBuilder<>))]
        static async ValueTask<(PgError?, RowDescription?)> ReadExtended(PgDecoder decoder, bool readParse, bool readDescribe, bool readExecute)
        {
            BackendMessage message;
            if (readParse)
            {
                if (!decoder.TryGetNext(out message))
                    message = await decoder.GetNextAsync().ConfigureAwait(false);
                if (message.EnsureExpectedOrError(PgTypes.BackendType.ParseComplete) is { } parseError)
                    return (parseError, null);

                // Nothing to do for ParseComplete.
                Debug.Assert(message.Header.BodyLength is 0);
            }

            if (!decoder.TryGetNext(out message))
                message = await decoder.GetNextAsync().ConfigureAwait(false);
            if (message.EnsureExpectedOrError(PgTypes.BackendType.BindComplete) is { } bindError)
                return (bindError, null);

            // Nothing to do for BindComplete.
            Debug.Assert(message.Header.BodyLength is 0);

            RowDescription? requestedRowDescription = null;
            if (readDescribe)
            {
                if (!decoder.TryGetNext(out message))
                    message = await decoder.GetNextAsync().ConfigureAwait(false);
                if (message.EnsureExpectedOrError(PgTypes.BackendType.RowDescription, PgTypes.BackendType.NoData)
                        is var result && result.Error is { } describeError)
                    return (describeError, null);

                switch (result.Type)
                {
                    case PgTypes.BackendType.RowDescription:
                        requestedRowDescription = new RowDescription();
                        requestedRowDescription.Initialize(message.BodyReader);
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
                    message = await decoder.GetNextAsync().ConfigureAwait(false);
                message.DebugEnsureExpected(PgTypes.BackendType.DataRow, PgTypes.BackendType.CommandComplete);
            }

            Debug.Assert(!readDescribe || requestedRowDescription is not null);
            return (null, requestedRowDescription);
        }
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

        var message = decoder.GetNext();
        // When an error is returned while we expect an RFQ it's going to be some unexpected server issue, just throw it.
        if (message.TryCreateError(out var syncError))
            PostgresException.Throw(syncError);

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

        if (!decoder.TryGetNext(out var message))
            return Core(decoder, errorMessage);

        // When an error is returned while we expect an RFQ it's going to be some unexpected server issue, just throw it.
        if (message.TryCreateError(out var syncError))
            PostgresException.Throw(syncError);

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
                PostgresException.Throw(syncError);

            var transactionStatus = ReadyForQueryMessage.Create(message).TransactionStatus;
            return errorMessage is not null && transactionStatus is TransactionStatus.Error
                ? (errorMessage, transactionStatus)
                : null;
        }
    }
}
