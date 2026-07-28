using FluentAssertions;
using SDRLoggerPlus.Contracts.Events;
using SDRLoggerPlus.Server.Services;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

/// <summary>
/// S-COORD presence board: the host tracks each station's latest band/mode for the "who's on what" view
/// and the RF-collision warning, and drops stations that have gone quiet so it doesn't warn about a rig
/// that's no longer on.
/// </summary>
[Trait("Category", "Unit")]
public class StationPresenceTrackerTests
{
    [Fact]
    public void Current_returnsLatestPerStation()
    {
        var t = new StationPresenceTracker();
        t.Update(new StationPresenceEvent("A", "N8SDR", "20m", "USB", DateTime.UtcNow));
        t.Update(new StationPresenceEvent("B", "N9BC", "40m", "CW", DateTime.UtcNow));
        t.Update(new StationPresenceEvent("A", "N8SDR", "15m", "USB", DateTime.UtcNow)); // A changed band

        var board = t.Current();
        board.Should().HaveCount(2);
        board.Single(p => p.StationId == "A").Band.Should().Be("15m"); // latest wins
    }

    [Fact]
    public void Current_prunesStaleStations()
    {
        var t = new StationPresenceTracker();
        t.Update(new StationPresenceEvent("gone", "X", "20m", "USB", DateTime.UtcNow.AddMinutes(-20)));
        t.Update(new StationPresenceEvent("here", "Y", "40m", "CW", DateTime.UtcNow));

        t.Current().Should().ContainSingle(p => p.StationId == "here");
    }
}
