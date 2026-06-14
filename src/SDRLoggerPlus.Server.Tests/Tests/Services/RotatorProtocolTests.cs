using System.Text;
using FluentAssertions;
using SDRLoggerPlus.Server.Services.Rotator;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

/// <summary>
/// Wire-protocol strategy tests. The protocols operate on a StreamReader/StreamWriter
/// pair so they can be exercised entirely in memory — no rotator hardware needed.
/// </summary>
[Trait("Category", "Unit")]
public class RotatorProtocolTests
{
    private static StreamReader ReaderFrom(string s) =>
        new(new MemoryStream(Encoding.ASCII.GetBytes(s)), Encoding.ASCII);

    private static (StreamWriter writer, MemoryStream sink) MakeWriter()
    {
        var sink = new MemoryStream();
        var writer = new StreamWriter(sink, Encoding.ASCII) { AutoFlush = true };
        return (writer, sink);
    }

    private static string Written(MemoryStream sink) => Encoding.ASCII.GetString(sink.ToArray());

    // ───────────────────────── microHAM ARCO ─────────────────────────

    [Fact]
    public async Task Arco_Poll_WritesC2_AndParsesAzimuth()
    {
        var (writer, sink) = MakeWriter();
        var reader = ReaderFrom("+0270+0000\r\n");

        var az = await new ArcoTcpProtocol().PollAzimuthAsync(reader, writer, default);

        az.Should().Be(270.0);
        Written(sink).Should().Be("C2\r");
    }

    [Theory]
    [InlineData("+0090+0000\r\n", 90.0)]
    [InlineData("+0270+0000\r\n", 270.0)]
    [InlineData("+0359+0010\r\n", 359.0)]
    [InlineData("+0000+0000\r\n", 0.0)]
    [InlineData("+0180\r\n", 180.0)]   // AZ only, no EL field
    public async Task Arco_Poll_ParsesAzimuthFromReply(string reply, double expected)
    {
        var (writer, _) = MakeWriter();
        var az = await new ArcoTcpProtocol().PollAzimuthAsync(ReaderFrom(reply), writer, default);
        az.Should().Be(expected);
    }

    [Theory]
    [InlineData("")]
    [InlineData("\r\n")]
    [InlineData("garbage\r\n")]
    [InlineData("?>\r\n")]
    public async Task Arco_Poll_ReturnsNull_OnUnparseableReply(string reply)
    {
        var (writer, _) = MakeWriter();
        var az = await new ArcoTcpProtocol().PollAzimuthAsync(ReaderFrom(reply), writer, default);
        az.Should().BeNull();
    }

    [Theory]
    [InlineData(5.0, "M005\r")]
    [InlineData(270.0, "M270\r")]
    [InlineData(270.6, "M270\r")]   // truncated integer degrees, matching SDRLogger+ int(az)
    [InlineData(359.9, "M359\r")]
    [InlineData(0.0, "M000\r")]
    public async Task Arco_SetAzimuth_WritesMCommand(double azimuth, string expected)
    {
        var (writer, sink) = MakeWriter();
        await new ArcoTcpProtocol().SetAzimuthAsync(azimuth, ReaderFrom(""), writer, default);
        Written(sink).Should().Be(expected);
    }

    [Fact]
    public async Task Arco_Stop_WritesS()
    {
        var (writer, sink) = MakeWriter();
        await new ArcoTcpProtocol().StopAsync(ReaderFrom(""), writer, default);
        Written(sink).Should().Be("S\r");
    }

    // ───────────────────────── hamlib rotctld (characterization) ─────────────────────────

    [Fact]
    public async Task Rotctld_Poll_WritesP_AndParsesAzimuth()
    {
        var (writer, sink) = MakeWriter();
        var reader = ReaderFrom("270.5\n0.0\n");   // rotctld: az line, then el line

        var az = await new RotctldProtocol().PollAzimuthAsync(reader, writer, default);

        az.Should().Be(270.5);
        Written(sink).Trim().Should().Be("p");
    }

    [Fact]
    public async Task Rotctld_SetAzimuth_WritesPCommand_AndReadsResponse()
    {
        var (writer, sink) = MakeWriter();
        var reader = ReaderFrom("RPRT 0\n");

        await new RotctldProtocol().SetAzimuthAsync(270.6, reader, writer, default);

        Written(sink).Trim().Should().Be("P 270.6 0");
    }

    [Fact]
    public async Task Rotctld_Stop_WritesS()
    {
        var (writer, sink) = MakeWriter();
        await new RotctldProtocol().StopAsync(ReaderFrom("RPRT 0\n"), writer, default);
        Written(sink).Trim().Should().Be("S");
    }

    [Fact]
    public async Task Rotctld_Poll_ReturnsNull_OnUnparseableReply()
    {
        var (writer, _) = MakeWriter();
        var az = await new RotctldProtocol().PollAzimuthAsync(ReaderFrom("RPRT -1\n\n"), writer, default);
        az.Should().BeNull();
    }
}
