using FluentAssertions;
using SDRLoggerPlus.Server.Services.Backup;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

[Trait("Category", "Unit")]
public class BackupScheduleTests
{
    private static readonly DateTime Now = new(2026, 6, 10, 12, 0, 0, DateTimeKind.Utc);

    [Theory]
    [InlineData("daily", 1)]
    [InlineData("weekly", 7)]
    public void NextDue_AnchorsToLastRun(string interval, int days)
    {
        var last = Now.AddHours(-2);
        BackupSchedule.NextDue(interval, last).Should().Be(last.AddDays(days));
    }

    [Fact]
    public void NextDue_OnExit_ReturnsNull()
        => BackupSchedule.NextDue("on_exit", Now).Should().BeNull();

    [Fact]
    public void NextDue_NoHistory_ReturnsNull()
        => BackupSchedule.NextDue("daily", null).Should().BeNull();

    [Fact]
    public void IsDue_NoHistory_True()
        => BackupSchedule.IsDue("daily", null, Now).Should().BeTrue();

    [Fact]
    public void IsDue_RanRecently_False()
        => BackupSchedule.IsDue("daily", Now.AddHours(-23), Now).Should().BeFalse();

    [Fact]
    public void IsDue_IntervalElapsed_True()
        => BackupSchedule.IsDue("daily", Now.AddHours(-25), Now).Should().BeTrue();

    [Fact]
    public void IsDue_OnExit_NeverDueFromTimer()
        => BackupSchedule.IsDue("on_exit", null, Now).Should().BeFalse();

    [Fact]
    public void IsDue_UnknownInterval_TreatedAsDaily()
        => BackupSchedule.IsDue("bogus", Now.AddHours(-25), Now).Should().BeTrue();
}
