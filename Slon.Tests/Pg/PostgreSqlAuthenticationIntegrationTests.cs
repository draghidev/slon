using System.Net;
using Slon.Pg;
using Slon.Pg.Protocol;
using Slon.Transport;

namespace Slon.Tests.Pg;

[TestClass]
[TestCategory(Category)]
public class PostgreSqlAuthenticationIntegrationTests
{
    public const string Category = "PostgreSqlAuthenticationIntegration";

    static EndPoint EndPoint
    {
        get
        {
            var host = Environment.GetEnvironmentVariable("SLON_TEST_HOST") ?? "127.0.0.1";
            var port = int.TryParse(Environment.GetEnvironmentVariable("SLON_TEST_PORT"), out var value)
                ? value
                : 55432;
            return IPAddress.TryParse(host, out var address)
                ? new IPEndPoint(address, port)
                : new DnsEndPoint(host, port);
        }
    }

    [TestInitialize]
    public void RequireOptIn()
    {
        if (Environment.GetEnvironmentVariable("SLON_AUTH_INTEGRATION") is not "1")
            Assert.Inconclusive(
                "Set SLON_AUTH_INTEGRATION=1 and provide the categorized authentication server, " +
                "or run Slon/test.sh --auth.");
    }

    [TestMethod]
    [DataRow("slon_scram", "scram-password", DisplayName = "SCRAM-SHA-256")]
    [DataRow("slon_md5", "md5-password", DisplayName = "MD5")]
    [DataRow("slon_password", "cleartext-password", DisplayName = "cleartext password")]
    public async Task PasswordMethod_AuthenticatesAgainstPostgreSql(string username, string password)
    {
        var options = new PgClientOptions
        {
            EndPoint = EndPoint,
            Username = username,
            Password = password,
            Database = "postgres",
            Ssl = new() { Mode = PostgreSqlSslMode.Disable },
            AllowInsecureTransport = true
        };

        var transport = await SocketStreamConnection.ConnectAsync(options.EndPoint);
        var protocol = PgClientProtocol.Create(new PgClientProtocolOptions(options));
        try
        {
            await protocol.StartAsync(options, transport);
            await PgTestPool.RunAsync(protocol, "select current_user");
            await protocol.CompleteAsync();
        }
        catch
        {
            try { await protocol.DisposeAsync(); } catch { }
            throw;
        }
    }
}
