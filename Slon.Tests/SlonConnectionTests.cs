namespace Slon.Tests;

[TestClass]
public class SlonConnectionTests
{
    [TestMethod]
    public void ConnectionConfigurationIsOwnedByDataSource()
    {
        using var dataSource = AdoTestPool.NewIsolatedDataSource();
        using var connection = dataSource.CreateConnection();

        Assert.AreEqual(dataSource.ConnectionString, connection.ConnectionString);
        Assert.IsFalse(connection.ConnectionString.Contains("Password", StringComparison.OrdinalIgnoreCase));
        Assert.ThrowsExactly<NotSupportedException>(() => connection.ConnectionString = "Database=other");
        Assert.ThrowsExactly<NotSupportedException>(() => connection.ChangeDatabase("other"));
    }

    [TestMethod]
    public async Task ChangeDatabaseAsyncIsRejectedWithoutChangingConnection()
    {
        await using var dataSource = AdoTestPool.NewIsolatedDataSource();
        await using var connection = dataSource.CreateConnection();

        await Assert.ThrowsExactlyAsync<NotSupportedException>(
            () => connection.ChangeDatabaseAsync("other"));
        Assert.AreSame(dataSource, connection.DbDataSource);
    }

    [TestMethod]
    public void AmbientTransactionsAreExplicitlyRejected()
    {
        using var dataSource = AdoTestPool.NewIsolatedDataSource();
        using var connection = dataSource.CreateConnection();
        using var transaction = new System.Transactions.CommittableTransaction();

        var exception = Assert.ThrowsExactly<NotSupportedException>(
            () => connection.EnlistTransaction(transaction));
        StringAssert.Contains(exception.Message, nameof(SlonTransaction));
    }
}
