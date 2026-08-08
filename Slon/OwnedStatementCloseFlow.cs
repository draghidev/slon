using Slon.Pg.Protocol;
using Slon.Text;
using static Slon.Pg.Protocol.PgTypes;

namespace Slon;

// Closes statements owned by an ADO command inside its connection's exclusive pipeline. Unlike
// session maintenance, this flow is ordered directly after every use by the same lease.
sealed class OwnedStatementCloseFlow : PgClientFlow
{
    readonly EncodedString[] _names;

    public OwnedStatementCloseFlow(EncodedString[] names, bool async) : base(supportsDeferredFlush: true)
    {
        _names = names;
        IsAsync = async;
    }

    protected override async ValueTask<FlowTasks> ExecuteAuto(Context context)
    {
        var encoder = context.GetEncoder();
        foreach (var name in _names)
            encoder.WriteClose(name);
        encoder.WriteSync();
        await encoder.FlushAuto().ConfigureAwait(false);

        var decoder = await context.GetDecoderAuto().ConfigureAwait(false);
        while (true)
        {
            var message = await decoder.GetNextAuto().ConfigureAwait(false);
            if (message.Header.Type is BackendType.ErrorResponse)
                PgErrorException.Throw(ErrorOrNoticeMessage.Create(message, []));
            if (message.Header.Type is BackendType.ReadyForQuery)
                return ValueTask.CompletedTask;
        }
    }
}
