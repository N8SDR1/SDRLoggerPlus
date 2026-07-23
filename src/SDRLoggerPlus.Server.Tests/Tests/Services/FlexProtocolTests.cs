using System.Text;
using FluentAssertions;
using SDRLoggerPlus.Server.Services.Flex;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

[Trait("Category", "Unit")]
public class FlexProtocolTests
{
    [Fact]
    public void ParseDiscoveryPayload_ExtractsFields_AndDecodesNameUnderscores()
    {
        var payload = "model=FLEX-6600 serial=1234-5678-9012-3456 version=3.4.23 " +
                      "name=Shack_Flex callsign=N8SDR ip=192.168.1.50 port=4992";
        var info = FlexProtocol.ParseDiscoveryPayload(payload);
        info.Should().NotBeNull();
        info!.Model.Should().Be("FLEX-6600");
        info.Serial.Should().Be("1234-5678-9012-3456");
        info.Version.Should().Be("3.4.23");
        info.Name.Should().Be("Shack Flex");   // underscores → spaces
        info.Callsign.Should().Be("N8SDR");
        info.Ip.Should().Be("192.168.1.50");
        info.Port.Should().Be(4992);
    }

    [Fact]
    public void ParseDiscoveryPayload_MissingRequiredFields_ReturnsNull()
    {
        FlexProtocol.ParseDiscoveryPayload("version=3.4 name=foo").Should().BeNull();
        FlexProtocol.ParseDiscoveryPayload("").Should().BeNull();
        FlexProtocol.ParseDiscoveryPayload(null).Should().BeNull();
    }

    [Fact]
    public void FindDiscoveryPayload_LocatesStringAfterBinaryVitaHeader()
    {
        // Simulate a VITA-49 datagram: binary header bytes, then the ASCII payload, then NUL padding.
        var header = new byte[] { 0x38, 0x00, 0x00, 0x08, 0x53, 0x4C, 0xFF, 0xFF };
        var ascii = Encoding.ASCII.GetBytes("model=FLEX-6400 serial=AA-BB ip=10.0.0.9 port=4992");
        var datagram = header.Concat(ascii).Concat(new byte[] { 0, 0, 0 }).ToArray();

        var payload = FlexProtocol.FindDiscoveryPayload(datagram, datagram.Length);
        payload.Should().StartWith("model=FLEX-6400");
        FlexProtocol.ParseDiscoveryPayload(payload)!.Ip.Should().Be("10.0.0.9");
    }

    [Fact]
    public void TuneCommand_FormatsFrequencyInMhz()
    {
        FlexProtocol.TuneCommand(12, 1, 14_205_500).Should().Be("C12|slice t 1 14.205500");
        FlexProtocol.HzToMhz(7_040_000).Should().Be("7.040000");
    }

    [Fact]
    public void ModeCommand_MapsAppModeToFlexToken()
    {
        FlexProtocol.ModeCommand(41, 0, "USB").Should().Be("C41|slice s 0 mode=USB");
        FlexProtocol.ModeCommand(42, 0, "CWU").Should().Be("C42|slice s 0 mode=CW");  // CWU/CWL collapse
        FlexProtocol.ModeCommand(43, 0, "FT8").Should().Be("C43|slice s 0 mode=DIGU");
    }

    [Fact]
    public void ParseSliceStatus_ReadsIndexFreqModeAndFlags()
    {
        var s = FlexProtocol.ParseSliceStatus("slice 1 RF_frequency=14.074000 mode=DIGU active=1 in_use=1");
        s.Should().NotBeNull();
        s!.Index.Should().Be(1);
        s.FrequencyHz.Should().Be(14_074_000);
        s.Mode.Should().Be("DIGU");
        s.Active.Should().BeTrue();
        s.InUse.Should().BeTrue();
    }

    [Fact]
    public void ParseSliceStatus_NonSlice_ReturnsNull()
    {
        FlexProtocol.ParseSliceStatus("radio slices=4").Should().BeNull();
        FlexProtocol.ParseSliceStatus("interlock state=READY").Should().BeNull();
    }

    [Fact]
    public void RadioId_IsStablePerSerial()
        => FlexProtocol.RadioId("1234-5678").Should().Be("flex-1234-5678");
}
