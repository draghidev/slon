using Slon.Pg;
using Slon.Pg.Types;
using Slon.Pooling;

namespace Slon;

public sealed partial class SlonDataSource
{
    readonly PgTypeCatalogPlugin[] _userTypeCatalogPlugins;
    readonly Lock _reloadLock = new();
    PgTypeCatalogFactory _typeCatalogFactory = null!;
    PgTypeCatalogPlugin[] _typeCatalogPlugins = null!;
    PgConnectionFactory _typeReloadConnectionFactory = null!;
    Task? _typeReload;

    /// Reloads PostgreSQL type metadata and publishes a new serializer snapshot.
    public void ReloadTypes()
        => ReloadTypes(async: false, CancellationToken.None).GetAwaiter().GetResult();

    /// <summary>Reloads PostgreSQL type metadata and publishes a new serializer snapshot.</summary>
    /// <param name="cancellationToken">A token for cancelling the reload.</param>
    public ValueTask ReloadTypesAsync(CancellationToken cancellationToken = default)
        => ReloadTypes(async: true, cancellationToken);

    async ValueTask ReloadTypes(bool async, CancellationToken cancellationToken)
    {
        try
        {
            ThrowIfDisposed();
            if (Volatile.Read(ref _dbDependencies) is null)
            {
                if (async)
                    await EnsureInitializedAsync(ConnectionTimeout, cancellationToken)
                        .ConfigureAwait(false);
                else
                    EnsureInitialized(ConnectionTimeout);
                return;
            }

            // Prebuilt catalogs have nothing to reload. Keep this an internal factory distinction so
            // generic ADO integrations can request a reload without first discovering the load strategy.
            if (!_typeCatalogFactory.SupportsReload)
                return;

            Task reload;
            TaskCompletionSource? owner = null;
            lock (_reloadLock)
            {
                if (_typeReload is { } existing)
                {
                    reload = existing;
                }
                else
                {
                    owner = new(TaskCreationOptions.RunContinuationsAsynchronously);
                    _typeReload = reload = owner.Task;
                }
            }
            // The synchronous path may run to completion inline, including clearing _typeReload, so
            // never start it while holding the publication lock.
            if (owner is not null)
                _ = RunTypeReloadAsync(async, owner);

            await reload.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            AdoException.Throw(ex);
        }
    }

    async Task RunTypeReloadAsync(bool async, TaskCompletionSource completion)
    {
        try
        {
            if (async)
                await _lifecycleLock.WaitAsync(CancellationToken.None).ConfigureAwait(false);
            else
                _lifecycleLock.Wait(CancellationToken.None);

            try
            {
                ThrowIfDisposed();
                var current = GetDbDependencies();
                var loaded = await LoadTypeCatalogAsync(current, async, _shutdown.Token).ConfigureAwait(false);

                // Publish only the complete replacement. Existing executions retain their old
                // immutable bundle; future executions observe this revision in one reference read.
                // The backend identity and catalog retain the same physical-load provenance; the
                // provider's compatibility policy licenses publishing that pair datasource-wide.
                // The tracker can survive only because preparation identity includes the resolved
                // parameter type/OID shape. Keep that shape in the identity as serializer resolution
                // matures; SQL text alone is not stable across mapping-affecting reloads.
                Volatile.Write(ref _dbDependencies, new PgDbDependencies(
                    loaded.BackendInfo, loaded.Catalog, current.CommandsTracker, _dbDepsRevision++));
            }
            finally
            {
                _lifecycleLock.Release();
            }

            ClearTypeReloadOwner();
            completion.TrySetResult();
        }
        catch (Exception ex)
        {
            ClearTypeReloadOwner();
            completion.TrySetException(ex);
        }

        void ClearTypeReloadOwner()
        {
            lock (_reloadLock)
            {
                if (ReferenceEquals(_typeReload, completion.Task))
                    _typeReload = null;
            }
        }
    }

    async ValueTask<LoadedTypeCatalog> LoadTypeCatalogAsync(
        PgDbDependencies current, bool async, CancellationToken shutdownToken)
    {
        if (!_typeCatalogFactory.RequiresProtocol)
        {
            var context = new PgTypeCatalogFactoryContext(current.BackendInfo, shutdownToken);
            var catalog = async
                ? await _typeCatalogFactory.CreateAsync(
                    context, _typeCatalogPlugins, shutdownToken).ConfigureAwait(false)
                : _typeCatalogFactory.Create(context, _typeCatalogPlugins);
            return new(catalog, context.BackendInfo);
        }

        if (_connectionPool is not null)
        {
            var state = new TypeReloadScheduleState(
                _typeCatalogFactory, _typeCatalogPlugins, async, shutdownToken);
            if (async)
                await _connectionPool.GetAsync(
                    static (candidate, state) => state.TrySchedule(candidate), state,
                    ConnectionTimeout, shutdownToken).ConfigureAwait(false);
            else
                _connectionPool.GetAsync(
                        static (candidate, state) => state.TrySchedule(candidate), state,
                        ConnectionTimeout, shutdownToken)
                    .AsTask().GetAwaiter().GetResult();
            var catalog = async
                ? await state.Load.ConfigureAwait(false)
                : state.Load.GetAwaiter().GetResult();
            return new(catalog, state.BackendInfo);
        }

        // An unpooled datasource has no configured capacity to adopt from.
        PgConnection? connection = null;
        Exception? error = null;
        try
        {
            connection = async
                ? await _typeReloadConnectionFactory.CreateAsync(shutdownToken).ConfigureAwait(false)
                : _typeReloadConnectionFactory.Create(ConnectionTimeout);
            var context = new PgTypeCatalogFactoryContext(connection.Protocol, shutdownToken);
            var catalog = async
                ? await _typeCatalogFactory.CreateAsync(
                    context, _typeCatalogPlugins, shutdownToken).ConfigureAwait(false)
                : _typeCatalogFactory.Create(context, _typeCatalogPlugins);
            return new(catalog, context.BackendInfo);
        }
        catch (Exception ex)
        {
            error = ex;
            throw;
        }
        finally
        {
            if (connection is not null)
            {
                if (async)
                    await connection.CompleteAsync(error).ConfigureAwait(false);
                else
                    connection.CompleteAsync(error).GetAwaiter().GetResult();
            }
        }
    }

    sealed class ConfiguredTypeLoadingRequirements(
        IReadOnlyList<string> schemas, bool loadTableComposites) : PgTypeCatalogPlugin
    {
        public override void Configure(PgTypeLoadingOptionsBuilder options)
        {
            for (var i = 0; i < schemas.Count; i++)
                options.AddTypeLoadingSchema(schemas[i]);
            options.EnableTableCompositesLoading(loadTableComposites);
        }
    }

    sealed class TypeReloadScheduleState(PgTypeCatalogFactory factory,
        PgTypeCatalogPlugin[] plugins, bool async,
        CancellationToken shutdownToken)
    {
        public ValueTask<PgTypeCatalog> Load { get; private set; }
        public PgBackendInfo BackendInfo { get; private set; } = null!;

        public bool TrySchedule(ConnectionCandidate<PgConnection> candidate)
        {
            // Pool callbacks run outside its synchronization lock. The synchronous path may therefore
            // perform the cold catalog load inline while owning only this candidate.
            var context = new PgTypeCatalogFactoryContext(
                candidate.Connection.Protocol, shutdownToken);
            BackendInfo = context.BackendInfo;
            try
            {
                Load = async
                    ? factory.CreateAsync(context, plugins, shutdownToken)
                    : new(factory.Create(context, plugins));
            }
            catch (Exception ex) when (context.FlowQueued)
            {
                // Queuing transfers candidate retirement to the flow. Preserve that ownership fact
                // even when extensible synchronous setup throws after the transfer; the caller
                // observes the failure from Load after pool placement has committed successfully.
                Load = ValueTask.FromException<PgTypeCatalog>(ex);
                return true;
            }

            if (!context.FlowQueued)
            {
                if (!Load.IsCompleted)
                    throw new InvalidOperationException(
                        "A protocol-backed type catalog factory yielded before queuing its load flow.");
                // Surface a synchronous setup failure inside pool admission so its idle token is
                // returned. A successful protocol factory must have queued a flow.
                Load.GetAwaiter().GetResult();
                throw new InvalidOperationException(
                    "A protocol-backed type catalog factory completed without queuing a load flow.");
            }

            return true;
        }
    }

    readonly record struct LoadedTypeCatalog(PgTypeCatalog Catalog, PgBackendInfo BackendInfo);
}
