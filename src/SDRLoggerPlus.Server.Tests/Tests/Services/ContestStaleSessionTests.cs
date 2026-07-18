using FluentAssertions;
using SDRLoggerPlus.Server.Services.Contesting;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

/// <summary>
/// The stale-session guard: an active contest session left idle beyond the
/// threshold is flagged stale so the client offers a resume prompt instead of
/// dropping straight back into last year's log.
/// </summary>
[Trait("Category", "Unit")]
public class ContestStaleSessionTests
{
    private static readonly DateTime Now = new(2026, 7, 17, 12, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void FreshSession_NoQsos_IsNotStale()
    {
        // Started an hour ago, nothing logged yet.
        ContestService.IsSessionStale(Now.AddHours(-1), Array.Empty<DateTime>(), Now)
            .Should().BeFalse();
    }

    [Fact]
    public void OldSession_NoQsos_IsStale()
    {
        // Started last year, never logged — the exact case we're guarding against.
        ContestService.IsSessionStale(Now.AddYears(-1), Array.Empty<DateTime>(), Now)
            .Should().BeTrue();
    }

    [Fact]
    public void RecentActivity_KeepsSessionFresh_EvenIfStartedLongAgo()
    {
        // A 48h contest: started 47h ago, last QSO 20 min ago → still in progress.
        var qsos = new[] { Now.AddHours(-47), Now.AddMinutes(-20) };
        ContestService.IsSessionStale(Now.AddHours(-47), qsos, Now).Should().BeFalse();
    }

    [Fact]
    public void LongDormant_AfterActivity_IsStale()
    {
        // Worked a contest a week ago, app left open, reopened today.
        var qsos = new[] { Now.AddDays(-7), Now.AddDays(-7).AddHours(6) };
        ContestService.IsSessionStale(Now.AddDays(-7), qsos, Now).Should().BeTrue();
    }

    [Fact]
    public void JustPastThreshold_IsStale_JustUnder_IsNot()
    {
        var overStart = Now - ContestService.StaleAfter - TimeSpan.FromMinutes(1);
        var underStart = Now - ContestService.StaleAfter + TimeSpan.FromMinutes(1);
        ContestService.IsSessionStale(overStart, Array.Empty<DateTime>(), Now).Should().BeTrue();
        ContestService.IsSessionStale(underStart, Array.Empty<DateTime>(), Now).Should().BeFalse();
    }
}
