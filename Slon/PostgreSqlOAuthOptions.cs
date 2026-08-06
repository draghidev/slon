using System.Net;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Slon;

public readonly record struct PostgreSqlOAuthToken(string AccessToken, DateTimeOffset? ExpiresAt = null)
{
    public override string ToString()
        => $"{nameof(PostgreSqlOAuthToken)} {{ AccessToken = <redacted:{AccessToken?.Length ?? 0} chars>, " +
           $"ExpiresAt = {ExpiresAt} }}";
}

public readonly record struct PostgreSqlOAuthContext(EndPoint EndPoint, string Username, string? Database);

public sealed class PostgreSqlOAuthOptions
{
    public Func<PostgreSqlOAuthContext, CancellationToken, ValueTask<PostgreSqlOAuthToken>>? TokenProvider { get; init; }
    public TimeSpan RefreshBeforeExpiration { get; init; } = TimeSpan.FromMinutes(1);

    internal void Validate()
    {
        if (TokenProvider is null)
            throw new InvalidOperationException("OAuth requires a token provider.");
        if (RefreshBeforeExpiration < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(RefreshBeforeExpiration));
    }
}

sealed class OAuthTokenCache(PostgreSqlOAuthOptions options, PostgreSqlOAuthContext context,
    ILogger? logger = null)
{
    readonly Lock _lock = new();
    readonly ILogger _logger = logger ?? NullLogger.Instance;
    PostgreSqlOAuthToken _token;
    Task<PostgreSqlOAuthToken>? _refresh;
    bool _fallbackFailureLogged;

    public ValueTask<PostgreSqlOAuthToken> GetAsync(bool async, CancellationToken cancellationToken)
        => async
            ? GetAsyncCore(async: true, cancellationToken)
            : new(GetSynchronously(cancellationToken));

    PostgreSqlOAuthToken GetSynchronously(CancellationToken cancellationToken)
        => GetAsyncCore(async: false, cancellationToken).AsTask().GetAwaiter().GetResult();

    ValueTask<PostgreSqlOAuthToken> GetAsyncCore(bool async, CancellationToken cancellationToken)
    {
        TaskCompletionSource<PostgreSqlOAuthToken> completion;
        Task<PostgreSqlOAuthToken> refresh;
        lock (_lock)
        {
            if (IsFresh(_token))
                return new(_token);
            if (_refresh is not null)
                return new(_refresh.WaitAsync(cancellationToken));

            completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
            _refresh = completion.Task;
            refresh = _refresh;
        }
        _ = RefreshAsync(async, completion);
        return new(refresh.WaitAsync(cancellationToken));
    }

    async Task RefreshAsync(bool async, TaskCompletionSource<PostgreSqlOAuthToken> completion)
    {
        try
        {
            var token = async
                ? await options.TokenProvider!(context, CancellationToken.None).ConfigureAwait(false)
                : await Task.Factory.StartNew(
                        () => options.TokenProvider!(context, CancellationToken.None).AsTask(),
                        CancellationToken.None,
                        TaskCreationOptions.DenyChildAttach | TaskCreationOptions.LongRunning,
                        TaskScheduler.Default)
                    .Unwrap().ConfigureAwait(false);

            if (string.IsNullOrEmpty(token.AccessToken))
                throw new InvalidOperationException("The OAuth token provider returned an empty token.");
            if (token.ExpiresAt is { } expiry && expiry <= DateTimeOffset.UtcNow)
                throw new InvalidOperationException("The OAuth token provider returned an expired token.");
            lock (_lock)
            {
                _token = token;
                _fallbackFailureLogged = false;
            }
            completion.TrySetResult(token);
        }
        catch (Exception ex)
        {
            PostgreSqlOAuthToken fallback;
            bool logFallbackFailure;
            lock (_lock)
            {
                fallback = _token;
                logFallbackFailure = !_fallbackFailureLogged;
                _fallbackFailureLogged = true;
            }
            if (IsUsable(fallback))
            {
                if (logFallbackFailure)
                    SlonLogMessages.OAuthRefreshFailedUsingFallback(_logger, ex);
                completion.TrySetResult(fallback);
            }
            else
                completion.TrySetException(ex);
        }
        finally
        {
            lock (_lock)
                _refresh = null;
        }
    }

    bool IsFresh(PostgreSqlOAuthToken token)
        => !string.IsNullOrEmpty(token.AccessToken)
            && (token.ExpiresAt is null || token.ExpiresAt > DateTimeOffset.UtcNow + options.RefreshBeforeExpiration);

    static bool IsUsable(PostgreSqlOAuthToken token)
        => !string.IsNullOrEmpty(token.AccessToken)
            && (token.ExpiresAt is null || token.ExpiresAt > DateTimeOffset.UtcNow);
}
