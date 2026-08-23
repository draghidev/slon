using System.Data;
using System.Data.Common;
using Slon.Pg;
using Slon.Pg.Protocol.Flows;
using Slon.Runtime.CompilerServices;

namespace Slon;

partial struct AdoBatchCore<TCommand> where TCommand : IAdoCommand
{
    public void Prepare(DbParameterCollection? parameters)
    {
        using var activity = StartActivity();
        try
        {
            PrepareCore(parameters);
        }
        catch (Exception ex)
        {
            SlonTracing.RecordException(activity, ex);
            AdoException.Throw(ex);
        }
    }

    void PrepareCore(DbParameterCollection? parameters)
    {
        var operation = Preparation.Begin(_fieldRef);
        CommandFlow.Enumerator enumerator = default;
        try
        {
            var flow = Enqueue(parameters, CommandBehavior.SchemaOnly, GetDependencies(),
                preparing: true);
            enumerator = flow.GetEnumerator();
            for (var i = 0; i < operation.CommandCount; i++)
            {
                if (!enumerator.MoveNext())
                    ThrowHelper.ThrowUnexpected("Not enough results returned.");
                operation.Observe(enumerator.Current);
            }
            operation.ThrowIfFailed();
            enumerator.Dispose();
            enumerator = default;
            operation.Commit();
        }
        catch
        {
            operation.Rollback();
            throw;
        }
        finally
        {
            try
            {
                enumerator.Dispose();
            }
            finally
            {
                operation.ReleaseFailedPreparation();
            }
        }
    }

    public ValueTask PrepareAsync(DbParameterCollection? parameters,
        CancellationToken cancellationToken = default)
        => PrepareAsyncProjected(_fieldRef, parameters, cancellationToken);

    static async ValueTask PrepareAsyncProjected(FieldRef<AdoBatchCore<TCommand>> fieldRef,
        DbParameterCollection? parameters, CancellationToken cancellationToken)
    {
        using var activity = fieldRef.Invoke().StartActivity();
        try
        {
            await PrepareAsyncCore(fieldRef, parameters, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            SlonTracing.RecordException(activity, ex);
            AdoException.Throw(ex);
        }
    }

    // Async instance methods on structs copy this, so the state machine resolves the live core
    // through its stable field reference instead.
    static async ValueTask PrepareAsyncCore(FieldRef<AdoBatchCore<TCommand>> fieldRef,
        DbParameterCollection? parameters, CancellationToken cancellationToken)
    {
        var operation = Preparation.Begin(fieldRef);
        CommandFlow.Enumerator enumerator = default;
        try
        {
            var dependencies = await fieldRef.Invoke().GetDependenciesAsync(cancellationToken)
                .ConfigureAwait(false);
            var flow = await fieldRef.Invoke().EnqueueAsync(parameters, CommandBehavior.SchemaOnly,
                dependencies, cancellationToken, preparing: true).ConfigureAwait(false);
            enumerator = flow.GetAsyncEnumerator(cancellationToken);
            for (var i = 0; i < operation.CommandCount; i++)
            {
                if (!await enumerator.MoveNextAsync(cancellationToken).ConfigureAwait(false))
                    ThrowHelper.ThrowUnexpected("Not enough results returned.");
                operation.Observe(enumerator.Current);
            }
            operation.ThrowIfFailed();
            await enumerator.DisposeAsync().ConfigureAwait(false);
            enumerator = default;
            operation.Commit();
        }
        catch
        {
            operation.Rollback();
            throw;
        }
        finally
        {
            try
            {
                await enumerator.DisposeAsync().ConfigureAwait(false);
            }
            finally
            {
                await operation.ReleaseFailedPreparationAsync().ConfigureAwait(false);
            }
        }
    }

    struct Preparation
    {
        readonly FieldRef<AdoBatchCore<TCommand>> _fieldRef;
        readonly SlonDataSource? _dataSource;
        readonly SlonConnection? _connection;
        List<Exception>? _exceptions;

        Preparation(FieldRef<AdoBatchCore<TCommand>> fieldRef,
            SlonDataSource? dataSource, SlonConnection? connection)
        {
            _fieldRef = fieldRef;
            _dataSource = dataSource;
            _connection = connection;
        }

        internal static Preparation Begin(FieldRef<AdoBatchCore<TCommand>> fieldRef)
        {
            ref var core = ref fieldRef.Invoke();
            core.ThrowIfDisposedOrReadOnly();
            core.TryGetDataSource(out var dataSource, out var connection);
            core._explicitlyPrepared = true;
            return new(fieldRef, dataSource, connection);
        }

        internal int CommandCount => _fieldRef.Invoke()._commands.Count;

        internal void Observe(CommandResult result)
        {
            try
            {
                if (result.HasRows)
                    ThrowHelper.ThrowUnexpected("Rows were returned?");
            }
            catch (Exception ex)
            {
                (_exceptions ??= []).Add(AdoException.Project(ex));
            }
        }

        internal void ThrowIfFailed()
        {
            if (_exceptions is not null)
                throw new AggregateException(_exceptions);
        }

        internal void Commit()
        {
            foreach (ref var command in _fieldRef.Invoke()._commands.AsSpan())
                command.MakeReadOnly();
        }

        internal void Rollback()
            => _fieldRef.Invoke()._explicitlyPrepared = false;

        internal void ReleaseFailedPreparation()
        {
            if (_fieldRef.Invoke()._explicitlyPrepared)
                return;

            if (_connection is not null)
                _connection.UnprepareOwned(async: false, _fieldRef.Instance).GetAwaiter().GetResult();
            else if (_dataSource is not null)
                _ = _dataSource.ReleaseOwnedPreparedCommand(
                    _fieldRef.Instance, awaitable: false);
        }

        internal ValueTask ReleaseFailedPreparationAsync()
        {
            if (_fieldRef.Invoke()._explicitlyPrepared)
                return default;

            if (_connection is not null)
                return _connection.UnprepareOwned(async: true, _fieldRef.Instance);
            return _dataSource?.ReleaseOwnedPreparedCommand(
                _fieldRef.Instance, awaitable: true) ?? default;
        }
    }
}
