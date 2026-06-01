using System.Runtime.CompilerServices;

namespace Slon.Pipelines;

sealed class AutoResetCancellationTokenSource : IDisposable
{
    CancellationTokenSource? _cancellationTokenSource;

    CancellationTokenSource Source
    {
        get
        {
            return Volatile.Read(ref _cancellationTokenSource) ?? Initialize();

            [MethodImpl(MethodImplOptions.NoInlining)]
            CancellationTokenSource Initialize()
            {
                var source = new CancellationTokenSource();
                var actual = Interlocked.CompareExchange(ref _cancellationTokenSource, source, null);
                if (actual != null)
                    source = actual;

                return source;
            }
        }
    }

    public CancellationToken Token => Source.Token;

    public CancellationTokenRegistration Register(CancellationToken cancellationToken)
        => cancellationToken.Register(static state => ((AutoResetCancellationTokenSource)state!).Cancel(), this);

    public CancellationTokenRegistration UnsafeRegister(CancellationToken cancellationToken)
        => cancellationToken.UnsafeRegister(static state => ((AutoResetCancellationTokenSource)state!).Cancel(), this);

    public void Cancel()
    {
        var tokenSource = Interlocked.Exchange(ref _cancellationTokenSource, null) ?? throw new InvalidOperationException("Invalid concurrent operations");
        tokenSource.Cancel();
    }

    public void Dispose()
    {
        _cancellationTokenSource?.Dispose();
    }
}
