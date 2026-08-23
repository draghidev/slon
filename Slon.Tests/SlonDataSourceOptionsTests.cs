using System.Net;

namespace Slon.Tests;

[TestClass]
public class SlonDataSourceOptionsTests
{
    [TestMethod]
    public void ToString_DoesNotExposePassword()
    {
        const string password = "unique-secret-that-must-not-be-rendered";
        var options = new SlonDataSourceOptions
        {
            EndPoint = TestEndPoint.Default,
            Username = "postgres",
            Password = password,
            Database = "postgres"
        };

        var text = options.ToString();

        Assert.DoesNotContain(password, text, StringComparison.Ordinal);
        Assert.Contains("Password = <redacted>", text, StringComparison.Ordinal);
    }

    [TestMethod]
    public void OAuthToken_ToString_DoesNotExposeAccessToken()
    {
        const string accessToken = "unique-bearer-token-that-must-not-be-rendered";
        var token = new PostgreSqlOAuthToken(accessToken, DateTimeOffset.UnixEpoch);

        var text = token.ToString();

        Assert.DoesNotContain(accessToken, text, StringComparison.Ordinal);
        Assert.Contains($"<redacted:{accessToken.Length} chars>", text, StringComparison.Ordinal);
        Assert.Contains(nameof(token.ExpiresAt), text, StringComparison.Ordinal);
    }

    [TestMethod]
    public void Snapshot_CopiesMutableNestedOptionsAndCredential()
    {
        var credential = new NetworkCredential("before-user", "before-password", "before-domain");
        var endpoint = new IPEndPoint(IPAddress.Loopback, 5432);
        var options = new SlonDataSourceOptions
        {
            EndPoint = endpoint,
            Username = "postgres",
            Ssl = new() { Mode = PostgreSqlSslMode.Require },
            Authentication = new() { AllowInsecureTransport = true },
            IntegratedSecurity = new() { Credential = credential },
            SessionReset = new() { ResetParameters = true }
        };

        var snapshot = options.Snapshot();
        options.Ssl.Mode = PostgreSqlSslMode.Disable;
        options.SessionReset.ResetParameters = false;
        endpoint.Port = 6432;
        credential.UserName = "after-user";
        credential.Password = "after-password";

        Assert.AreEqual(PostgreSqlSslMode.Require, snapshot.Ssl.Mode);
        Assert.AreEqual(5432, ((IPEndPoint)snapshot.EndPoint).Port);
        Assert.IsTrue(snapshot.Authentication.AllowInsecureTransport);
        Assert.IsTrue(snapshot.SessionReset.ResetParameters);
        Assert.AreEqual("before-user", snapshot.IntegratedSecurity!.Credential.UserName);
        Assert.AreEqual("before-password", snapshot.IntegratedSecurity.Credential.Password);
    }

    [TestMethod]
    public void ConnectionInitializers_MustBeConfiguredAsPair()
    {
        var syncOnly = new SlonDataSourceOptions
        {
            EndPoint = TestEndPoint.Default,
            Username = "postgres",
            ConnectionInitializer = static (_, _) => { }
        };
        var asyncOnly = syncOnly with
        {
            ConnectionInitializer = null,
            AsyncConnectionInitializer = static (_, _) => ValueTask.CompletedTask
        };

        Assert.ThrowsExactly<ArgumentException>(() => new SlonDataSource(syncOnly));
        Assert.ThrowsExactly<ArgumentException>(() => new SlonDataSource(asyncOnly));
    }

    [TestMethod]
    public void RequiredIdentityAndPoolBounds_AreValidatedAtConstruction()
    {
        var options = new SlonDataSourceOptions
        {
            EndPoint = TestEndPoint.Default,
            Username = "postgres"
        };

        Assert.ThrowsExactly<ArgumentNullException>(() => new SlonDataSource(null!));
        Assert.ThrowsExactly<ArgumentNullException>(() =>
            new SlonDataSource(options with { EndPoint = null! }));
        Assert.ThrowsExactly<ArgumentException>(() =>
            new SlonDataSource(options with { Username = " " }));
        Assert.ThrowsExactly<ArgumentOutOfRangeException>(() =>
            new SlonDataSource(options with { MaxPoolSize = 0, MinPoolSize = 0 }));
        Assert.ThrowsExactly<ArgumentOutOfRangeException>(() =>
            new SlonDataSource(options with { MaxPoolSize = 1, MinPoolSize = 2 }));
        Assert.ThrowsExactly<ArgumentOutOfRangeException>(() =>
            new SlonDataSource(options with { MinPoolSize = -1 }));
    }

    [TestMethod]
    public void CancellationConvergenceTiming_MustBeFiniteAndOrdered()
    {
        var options = new SlonDataSourceOptions
        {
            EndPoint = TestEndPoint.Default,
            Username = "postgres"
        };

        Assert.ThrowsExactly<ArgumentOutOfRangeException>(() =>
            new SlonDataSource(options with { CancellationTimeout = Timeout.InfiniteTimeSpan }));
        Assert.ThrowsExactly<ArgumentOutOfRangeException>(() =>
            new SlonDataSource(options with { CancellationTimeout = TimeSpan.Zero }));
        Assert.ThrowsExactly<ArgumentOutOfRangeException>(() =>
            new SlonDataSource(options with { CancellationRetryInterval = TimeSpan.Zero }));
        Assert.ThrowsExactly<ArgumentOutOfRangeException>(() =>
            new SlonDataSource(options with
            {
                CancellationTimeout = TimeSpan.FromSeconds(1),
                CancellationRetryInterval = TimeSpan.FromSeconds(1)
            }));

        using var dataSource = new SlonDataSource(options with
        {
            CancellationTimeout = TimeSpan.FromSeconds(2),
            CancellationRetryInterval = TimeSpan.FromSeconds(1)
        });
    }
}
