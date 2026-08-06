using Slon.Tests.Pg;
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
            ScopeReset = new() { ResetParameters = true }
        };

        var snapshot = options.Snapshot();
        options.Ssl.Mode = PostgreSqlSslMode.Disable;
        options.ScopeReset.ResetParameters = false;
        endpoint.Port = 6432;
        credential.UserName = "after-user";
        credential.Password = "after-password";

        Assert.AreEqual(PostgreSqlSslMode.Require, snapshot.Ssl.Mode);
        Assert.AreEqual(5432, ((IPEndPoint)snapshot.EndPoint).Port);
        Assert.IsTrue(snapshot.Authentication.AllowInsecureTransport);
        Assert.IsTrue(snapshot.ScopeReset.ResetParameters);
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
}
