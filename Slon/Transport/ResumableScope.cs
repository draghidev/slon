namespace Slon.Transport;

// Installs a write-resumption signal and optional deadline for one resumable call. Dispose restores
// the previous TLS values, preserving nested scopes; ref safety prevents the scope from escaping.
readonly ref struct ResumableScope
{
    readonly WriteResumeSignal? _previousSignal;
    readonly Deadline? _previousDeadline;

    public ResumableScope(WriteResumeSignal signal) : this(signal, default) { }

    public ResumableScope(WriteResumeSignal signal, Deadline? deadline)
    {
        _previousSignal = TransportConnection.SyncNonBlockingSignal;
        _previousDeadline = TransportConnection.SyncNonBlockingDeadline;
        TransportConnection.SyncNonBlockingSignal = signal;
        TransportConnection.SyncNonBlockingDeadline = deadline;
    }

    public void Dispose()
    {
        TransportConnection.SyncNonBlockingSignal = _previousSignal;
        TransportConnection.SyncNonBlockingDeadline = _previousDeadline;
    }
}
