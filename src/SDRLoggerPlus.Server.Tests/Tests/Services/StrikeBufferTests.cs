using FluentAssertions;
using SDRLoggerPlus.Contracts.Events;
using SDRLoggerPlus.Server.Services.Weather;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

[Trait("Category", "Unit")]
public class StrikeBufferTests
{
    private static readonly DateTime T0 = new(2026, 7, 5, 12, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void Add_returns_only_new_strikes_and_dedupes()
    {
        var buf = new StrikeBuffer(localWindow: TimeSpan.FromMinutes(5), globalWindow: TimeSpan.FromMinutes(10), cap: 2000);
        var a = new LightningStrike(44.8, -91.6, T0, Local: true);
        var added1 = buf.Add(new[] { a }, T0, stationLat: 44.8, stationLon: -91.6);
        var added2 = buf.Add(new[] { a }, T0, 44.8, -91.6); // same strike again

        added1.Should().HaveCount(1);
        added2.Should().BeEmpty();
        buf.Current(T0).Should().HaveCount(1);
    }

    [Fact]
    public void Current_prunes_by_window_local_longer_than_global()
    {
        var buf = new StrikeBuffer(TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(10), 2000);
        var local = new LightningStrike(44.8, -91.6, T0, Local: true);
        var global = new LightningStrike(0, 100, T0, Local: false);
        buf.Add(new[] { local, global }, T0, 44.8, -91.6);

        // 7 min later: global window (10m) keeps it, local window (5m) drops the local one.
        var later = buf.Current(T0.AddMinutes(7));
        later.Should().ContainSingle(s => !s.Local);
        later.Should().NotContain(s => s.Local);
    }

    [Fact]
    public void Cap_trims_farthest_first_and_always_keeps_local()
    {
        var buf = new StrikeBuffer(TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(10), cap: 2);
        var near = new LightningStrike(44.9, -91.6, T0, Local: true);   // ~11 km
        var mid = new LightningStrike(50.0, -91.6, T0, Local: false);   // ~580 km
        var far = new LightningStrike(0.0, 100.0, T0, Local: false);    // ~half a world
        buf.Add(new[] { near, mid, far }, T0, 44.8, -91.6);

        var current = buf.Current(T0);
        current.Should().HaveCount(2);
        current.Should().Contain(near);       // local always kept
        current.Should().Contain(mid);        // nearer of the two globals
        current.Should().NotContain(far);     // farthest trimmed
    }
}
