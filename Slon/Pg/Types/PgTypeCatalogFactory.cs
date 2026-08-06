using System.Text;
using Slon.Pg.Protocol;

namespace Slon.Pg.Types;

abstract class PgTypeCatalogFactory
{
    // Kept internal while Queue exposes the internal flow composition model. A future external
    // dialect API must lift query/result processing rather than publishing PgClientFlow machinery.
    // Protocol-backed factories must enqueue their load flow before CreateAsync first yields.
    // This lets a datasource submit the operation through normal pool admission without first
    // renting an uncommitted connection token. Prebuilt factories bypass connection selection.
    internal virtual bool RequiresProtocol => true;
    internal virtual bool SupportsReload => true;

    public PgTypeCatalog Create(
        PgTypeCatalogFactoryContext context,
        IReadOnlyList<PgTypeCatalogPlugin> plugins)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(plugins);

        var (builder, options) = Configure(plugins);
        Populate(builder, context, options);
        Apply(builder, plugins);
        return builder.Build();
    }

    public async ValueTask<PgTypeCatalog> CreateAsync(
        PgTypeCatalogFactoryContext context,
        IReadOnlyList<PgTypeCatalogPlugin> plugins,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(plugins);

        var (builder, options) = Configure(plugins);
        await PopulateAsync(builder, context, options, cancellationToken).ConfigureAwait(false);
        Apply(builder, plugins);
        return builder.Build();
    }

    static (PgTypeCatalogBuilder Builder, PgTypeLoadingOptions Options) Configure(
        IReadOnlyList<PgTypeCatalogPlugin> plugins)
    {
        var optionsBuilder = new PgTypeLoadingOptionsBuilder();
        for (var i = 0; i < plugins.Count; i++)
            plugins[i].Configure(optionsBuilder);
        return (new PgTypeCatalogBuilder(), optionsBuilder.Build());
    }

    static void Apply(PgTypeCatalogBuilder builder, IReadOnlyList<PgTypeCatalogPlugin> plugins)
    {
        // Plugins arrive in registration order. Applying them in that same order gives the
        // most recently registered plugin the final whole-entry replacement. This is Slon's
        // explicit override rule; Npgsql's prepend-ordered resolver queries use the opposite
        // traversal shape even though they serve a related plugin-composition purpose.
        for (var i = 0; i < plugins.Count; i++)
            plugins[i].Apply(builder);
    }

    protected abstract void Populate(
        PgTypeCatalogBuilder builder,
        PgTypeCatalogFactoryContext context,
        PgTypeLoadingOptions options);

    protected abstract ValueTask PopulateAsync(
        PgTypeCatalogBuilder builder,
        PgTypeCatalogFactoryContext context,
        PgTypeLoadingOptions options,
        CancellationToken cancellationToken);

    public static PgTypeCatalogFactory FromBaseline(PgTypeCatalog baseline)
        => new PrebuiltBaselineFactory(baseline);

    sealed class PrebuiltBaselineFactory(PgTypeCatalog baseline) : PgTypeCatalogFactory
    {
        internal override bool RequiresProtocol => false;
        internal override bool SupportsReload => false;

        protected override void Populate(PgTypeCatalogBuilder builder,
            PgTypeCatalogFactoryContext context, PgTypeLoadingOptions options)
            => Copy(builder);

        protected override ValueTask PopulateAsync(PgTypeCatalogBuilder builder,
            PgTypeCatalogFactoryContext context, PgTypeLoadingOptions options,
            CancellationToken cancellationToken)
        {
            Copy(builder);
            return ValueTask.CompletedTask;
        }

        void Copy(PgTypeCatalogBuilder builder)
        {
            foreach (var type in baseline.Types)
                builder.Add(type);
        }
    }
}

sealed class PgTypeCatalogFactoryContext
{
    readonly PgClientProtocol? _protocol;
    readonly CancellationToken _stoppingToken;
    bool _flowQueued;

    public PgTypeCatalogFactoryContext(PgClientProtocol protocol,
        CancellationToken stoppingToken = default)
    {
        _protocol = protocol;
        _stoppingToken = stoppingToken;
        BackendInfo = protocol.FlowControl.BackendInfo;
    }

    public PgTypeCatalogFactoryContext(PgBackendInfo backendInfo,
        CancellationToken stoppingToken = default)
    {
        _stoppingToken = stoppingToken;
        BackendInfo = backendInfo;
    }

    PgClientProtocol Protocol => _protocol
        ?? throw new InvalidOperationException("This type catalog factory does not have a protocol.");
    internal bool FlowQueued => _flowQueued;
    internal CancellationToken StoppingToken => _stoppingToken;
    public Encoding ClientEncoding => Protocol.FlowControl.ClientEncoding;

    public T Queue<T>(T flow, CancellationToken cancellationToken = default) where T : PgClientFlow
    {
        if (!cancellationToken.CanBeCanceled)
            cancellationToken = _stoppingToken;
        var queued = Protocol.Queue(flow, cancellationToken);
        _flowQueued = true;
        return queued;
    }

    public PgBackendInfo BackendInfo { get; }
    public PgBackendCapabilities Capabilities => BackendInfo.Capabilities;
}
