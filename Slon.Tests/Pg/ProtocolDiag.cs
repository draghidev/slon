using System.Reflection;
using Slon.Pg.Protocol;
using Slon.Pg.Protocol.Flows;

namespace Slon.Tests.Pg;

// Shared wedge readout for the ARMED instruments (pre-turn strand classifier, scope-cascade
// premise dump, forceful-shutdown amplifier) - the open hunts. Fixed families drop their dumps
// with the fix (this file's consumers shrink as faces close; delete it when the last hunt does).
// A hang self-classifies from these gauges without a live debugger: pumpTask is the pump-death
// detector (Faulted carries the killer's stack; a completed task with the source still holding
// items means the pump exited while flows were pending); the armed bit classifies a stranded
// backlog (armed with driving=0 = wake lost while idle-parked; not armed = the pump never
// returned from an off-park suspension; driving=1 = a runner wedged mid-turn).
static class ProtocolDiag
{
    const BindingFlags All = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

    internal static string Describe(PgClientFlow? f)
    {
        if (f is null)
            return "null";
        var common = $"type={f.GetType().Name},completed={f.IsCompleted},started={f.IsStarted},pending={f.IsPending}";
        if (f is not CommandFlow)
            return $"{{{common}}}";

        var type = f.GetType();
        object? F(string name) => FindField(type, name).GetValue(f);
        object? B(string name) => typeof(PgClientFlow).GetField(name, All)!.GetValue(f);
        var core = F("_callerInteractionCore")!;
        var coreType = core.GetType();
        object? C(string name) => coreType.GetField(name, All)!.GetValue(core);
        var gate = C("_gate")!;
        var gateType = gate.GetType();
        var version = gateType.GetProperty("Version", All)!.GetValue(gate)!;
        var status = gateType.GetMethod("GetStatus", All)!.Invoke(gate, [version]);
        return $"{{{common},command={F("_commandIndex")},window={B("_cancellationWindow")},rfqs={B("_rfqCount")}," +
               $"scope={F("_cancellationScope")},timing={F("_backendCancellationTiming")}/" +
               $"{F("_subsequentBackendCancellationTiming")},context={F("_contextPublished")},body={F("_bodyState")}," +
               $"draining={F("_draining")},disposed={F("_consumerDisposed")}," +
               $"terminal={F("_enumeratorCompleted")},cancel={F("_cancelRequested")}," +
               $"gate={status},wake={C("_wakeRequested")},pendingContinuation={C("_pendingContinuation") is not null}," +
               $"progress={C("_progressSignaled")}}}";
    }

    internal static string Gauges(PgClientProtocol protocol)
        => $"backlog={protocol.Backlog} outstanding={protocol.Outstanding} " +
           $"executor={Describe(protocol.FlowControl.ExecutingFlow)} activated={Describe(protocol.FlowControl.ActivatedFlow)}";

    // Reflection readout of the source's pump state, test-only.
    internal static string SourceState(PgClientProtocol protocol)
    {
        var source = typeof(PgClientProtocol).GetField("_source", All)!.GetValue(protocol)!;
        var state = source.GetType().GetField("_state", All)!.GetValue(source)!;
        var st = state.GetType();
        object? S(string name) => st.GetField(name, All)!.GetValue(state);
        var wake = S("WakeEvent")!;
        var wt = wake.GetType();
        object? W(string name) => wt.GetField(name, All)!.GetValue(wake);
        var driver = S("WakeDriver")!;
        var dt = driver.GetType();
        object? D(string name) => dt.GetField(name, All)!.GetValue(driver);
        var pipeline = typeof(PgClientProtocol).GetField("_pipeline", All)!.GetValue(protocol)!;
        var pumpTask = (Task)pipeline.GetType().GetField("_executionTask", All)!.GetValue(pipeline)!;
        var pump = pumpTask.Status + (pumpTask.Exception is { } ex
            ? $" ex={ex.InnerException?.GetType().Name}: {ex.InnerException?.Message}\n  pump fault stack: {ex.InnerException?.StackTrace}"
            : "");
        return $"pumpTask={pump} armed={W("_pending")} registered={W("_waitContinuation") is not null} " +
            $"driving={D("_active")} redrive={D("_redrive")} " +
            $"parkedSyncHead={S("SyncHeadReserved")} held={S("HeldSyncFlow") is not null} " +
            $"takeoverPending={S("TakeoverPending")} takeoverActive={S("TakeoverActive")}";
    }

    internal static string CancellationState(PgClientProtocol protocol)
        => protocol.DescribeCancellationState();

    internal static string CancellationFlowState(CommandFlow flow)
    {
        var type = flow.GetType();
        object? F(string name) => FindField(type, name).GetValue(flow);
        return $"command={F("_commandIndex")},window={F("_cancellationWindow")},rfqs={F("_rfqCount")}," +
               $"scope={F("_cancellationScope")},timing={F("_backendCancellationTiming")}/" +
               $"{F("_subsequentBackendCancellationTiming")},context={F("_contextPublished")}," +
               $"body={F("_bodyState")},draining={F("_draining")},disposed={F("_consumerDisposed")}," +
               $"terminal={F("_enumeratorCompleted")},cancel={F("_cancelRequested")}";
    }

    static FieldInfo FindField(Type type, string name)
    {
        for (var current = type; current is not null; current = current.BaseType)
        {
            if (current.GetField(name, All) is { } field)
                return field;
        }
        throw new MissingFieldException(type.FullName, name);
    }

}
