namespace Slon.Tests;

[TestClass]
public class AdoTimeoutTests
{
    [TestMethod]
    public void PendingTimeout_FollowsCommandTimeoutUntilOverridden()
    {
        using var command = new SlonCommand { CommandTimeout = 12 };
        using var batch = new SlonBatch { Timeout = 12 };

        Assert.AreEqual(TimeSpan.FromSeconds(12), command.PendingTimeout);
        Assert.AreEqual(TimeSpan.FromSeconds(12), batch.PendingTimeout);

        command.PendingTimeout = TimeSpan.FromSeconds(3);
        batch.PendingTimeout = TimeSpan.FromSeconds(3);
        command.CommandTimeout = 20;
        batch.Timeout = 20;

        Assert.AreEqual(TimeSpan.FromSeconds(3), command.PendingTimeout);
        Assert.AreEqual(TimeSpan.FromSeconds(3), batch.PendingTimeout);
    }

    [TestMethod]
    public void PendingTimeout_RejectsInvalidNegativeValues()
    {
        using var command = new SlonCommand();
        using var batch = new SlonBatch();

        Assert.ThrowsExactly<ArgumentOutOfRangeException>(
            () => command.PendingTimeout = TimeSpan.FromMilliseconds(-2));
        Assert.ThrowsExactly<ArgumentOutOfRangeException>(
            () => batch.PendingTimeout = TimeSpan.FromMilliseconds(-2));

        command.PendingTimeout = Timeout.InfiniteTimeSpan;
        batch.PendingTimeout = Timeout.InfiniteTimeSpan;
    }
}
