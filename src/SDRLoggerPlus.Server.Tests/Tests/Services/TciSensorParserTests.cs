using System.Globalization;
using FluentAssertions;
using SDRLoggerPlus.Server.Services.Tci;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

/// <summary>
/// TciSensorParser decodes the sensor frames Thetis's TCI server emits
/// (formats verified against Thetis TCIServer.cs sendTextFrame calls):
///   rx_sensors:&lt;rx&gt;,&lt;dBm&gt;;
///   rx_channel_sensors:&lt;rx&gt;,&lt;ch&gt;,&lt;dBm&gt;;
///   rx_channel_sensors_ex:&lt;rx&gt;,&lt;ch&gt;,&lt;dBm&gt;,&lt;avg dBm&gt;,&lt;peak-bin dBm&gt;;
///   tx_sensors:&lt;trx&gt;,&lt;mic dBm&gt;,&lt;watts&gt;,&lt;peak watts&gt;,&lt;swr&gt;;
/// Args arrive pre-split on ',' by TciRadioConnection. Malformed frames must
/// return null (dropped silently) — a misbehaving radio must never crash the
/// receive loop.
/// </summary>
[Trait("Category", "Unit")]
public class TciSensorParserTests
{
    [Fact]
    public void ParseRxSensors_ValidArgs_ReturnsReading()
    {
        var reading = TciSensorParser.ParseRxSensors(new[] { "0", "-97.4" });

        reading.Should().NotBeNull();
        reading!.Value.Rx.Should().Be(0);
        reading.Value.Dbm.Should().BeApproximately(-97.4, 0.001);
    }

    [Theory]
    [InlineData("")]
    [InlineData("0")]
    [InlineData("x,-97.4")]
    [InlineData("0,notanumber")]
    public void ParseRxSensors_MalformedArgs_ReturnsNull(string argsCsv)
    {
        TciSensorParser.ParseRxSensors(SplitArgs(argsCsv)).Should().BeNull();
    }

    private static string[] SplitArgs(string csv) =>
        csv.Length == 0 ? [] : csv.Split(',');

    [Fact]
    public void ParseRxChannelSensors_ThreeArgs_ReturnsReadingWithoutAverages()
    {
        var reading = TciSensorParser.ParseRxChannelSensors(new[] { "1", "0", "-88.5" });

        reading.Should().NotBeNull();
        reading!.Value.Rx.Should().Be(1);
        reading.Value.Channel.Should().Be(0);
        reading.Value.Dbm.Should().BeApproximately(-88.5, 0.001);
        reading.Value.AvgDbm.Should().BeNull();
        reading.Value.PeakBinDbm.Should().BeNull();
    }

    [Fact]
    public void ParseRxChannelSensors_ExtendedArgs_ReturnsAverages()
    {
        var reading = TciSensorParser.ParseRxChannelSensors(
            new[] { "0", "0", "-97.4", "-99.1", "-85.0" });

        reading.Should().NotBeNull();
        reading!.Value.AvgDbm.Should().BeApproximately(-99.1, 0.001);
        reading.Value.PeakBinDbm.Should().BeApproximately(-85.0, 0.001);
    }

    [Theory]
    [InlineData("0,0")]
    [InlineData("0,ch,-97.4")]
    public void ParseRxChannelSensors_MalformedArgs_ReturnsNull(string argsCsv)
    {
        TciSensorParser.ParseRxChannelSensors(SplitArgs(argsCsv)).Should().BeNull();
    }

    [Fact]
    public void ParseTxSensors_ValidArgs_ReturnsReading()
    {
        // Real Thetis/ExpertSDR3 shape: trx index first, then 4 floats.
        var reading = TciSensorParser.ParseTxSensors(new[] { "0", "-12.3", "4.8", "5.0", "1.4" });

        reading.Should().NotBeNull();
        reading!.Value.Trx.Should().Be(0);
        reading.Value.MicDbm.Should().BeApproximately(-12.3, 0.001);
        reading.Value.PowerWatts.Should().BeApproximately(4.8, 0.001);
        reading.Value.PeakPowerWatts.Should().BeApproximately(5.0, 0.001);
        reading.Value.Swr.Should().BeApproximately(1.4, 0.001);
    }

    [Theory]
    [InlineData("-12.3,4.8,5.0,1.4")] // 4-arg (no trx index) — not a real radio frame; drop
    [InlineData("0,-12.3,4.8,5.0")]
    [InlineData("0,-12.3,4.8,5.0,swr")]
    [InlineData("x,-12.3,4.8,5.0,1.4")]
    public void ParseTxSensors_MalformedArgs_ReturnsNull(string argsCsv)
    {
        TciSensorParser.ParseTxSensors(SplitArgs(argsCsv)).Should().BeNull();
    }

    [Theory]
    [InlineData("0,NaN")]
    [InlineData("0,Infinity")]
    [InlineData("0,-Infinity")]
    [InlineData("0,1e309")] // overflows to Infinity
    public void ParseRxSensors_NonFiniteValues_ReturnsNull(string argsCsv)
    {
        // System.Text.Json cannot serialize non-finite doubles; a hostile
        // peer must not be able to break the hub broadcast with one frame.
        TciSensorParser.ParseRxSensors(SplitArgs(argsCsv)).Should().BeNull();
    }

    [Fact]
    public void ParseTxSensors_NonFiniteSwr_ReturnsNull()
    {
        TciSensorParser.ParseTxSensors(new[] { "0", "-12.3", "4.8", "5.0", "NaN" }).Should().BeNull();
    }

    [Fact]
    public void Parsing_IsCultureInvariant()
    {
        var original = CultureInfo.CurrentCulture;
        try
        {
            // de-DE uses ',' as the decimal separator — "−97.4" must still parse
            // as ninety-seven-point-four, not fail or mis-parse.
            CultureInfo.CurrentCulture = new CultureInfo("de-DE");

            var reading = TciSensorParser.ParseRxSensors(new[] { "0", "-97.4" });

            reading.Should().NotBeNull();
            reading!.Value.Dbm.Should().BeApproximately(-97.4, 0.001);
        }
        finally
        {
            CultureInfo.CurrentCulture = original;
        }
    }
}
