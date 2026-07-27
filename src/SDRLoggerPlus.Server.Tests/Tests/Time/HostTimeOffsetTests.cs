using FluentAssertions;
using SDRLoggerPlus.Server.Core.Time;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Time;

/// <summary>
/// S4 time authority: a machine is its own authority until it syncs to a host, after which its clock is
/// corrected into the host's frame so all stations agree even with no internet.
/// </summary>
[Trait("Category", "Unit")]
public class HostTimeOffsetTests
{
    [Fact]
    public void Default_isTheAuthority_zeroOffset()
    {
        var o = new HostTimeOffset();
        o.IsHost.Should().BeTrue();
        o.Offset.Should().Be(TimeSpan.Zero);
        o.LastSyncUtc.Should().BeNull();
    }

    [Fact]
    public void Update_recordsOffset_marksNotHost_andCorrectsNowIntoHostFrame()
    {
        var o = new HostTimeOffset();

        o.Update(TimeSpan.FromSeconds(45)); // this clock reads 45 s behind the host

        o.IsHost.Should().BeFalse();
        o.Offset.Should().Be(TimeSpan.FromSeconds(45));
        o.LastSyncUtc.Should().NotBeNull();
        (o.NowInHostFrame() - DateTime.UtcNow).TotalSeconds.Should().BeApproximately(45, 2);
    }
}
