using FluentAssertions;
using SDRLoggerPlus.Server.Controllers;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Controllers;

[Trait("Category", "Unit")]
public class PskReporterControllerTests
{
    // Real shape of a PSK Reporter retrieve API response (attribute names/casing match live data).
    private const string SampleXml = """
        <?xml version="1.0"?>
        <receptionReports>
          <lastSequenceNumber value="69673623560"/>
          <maxFlowStartSeconds value="1781291147"/>
          <receptionReport receiverCallsign="K1RA-PI" receiverLocator="FM18cr" senderCallsign="W1AW" senderLocator="EM65vi" frequency="14075745" flowStartSeconds="1781290305" mode="FT8" isSender="1" sNR="-18" />
          <receptionReport receiverCallsign="VE3ABC" receiverLocator="FN03" senderCallsign="W1AW" senderLocator="EM65vi" frequency="7074000" flowStartSeconds="1781290000" mode="FT4" sNR="3" />
        </receptionReports>
        """;

    [Fact]
    public void ParseReports_ExtractsAllFields()
    {
        var reports = PskReporterController.ParseReports(SampleXml);

        reports.Should().HaveCount(2);

        var first = reports[0];
        first.ReceiverCallsign.Should().Be("K1RA-PI");
        first.ReceiverLocator.Should().Be("FM18cr");
        first.SenderCallsign.Should().Be("W1AW");
        first.SenderLocator.Should().Be("EM65vi");
        first.FrequencyHz.Should().Be(14075745);
        first.Mode.Should().Be("FT8");
        first.Snr.Should().Be(-18);
        first.FlowStartSeconds.Should().Be(1781290305);
    }

    [Fact]
    public void ParseReports_SkipsReportsMissingEitherLocator()
    {
        const string xml = """
            <receptionReports>
              <receptionReport receiverCallsign="A" receiverLocator="FN03" senderCallsign="W1AW" frequency="14074000" sNR="-5" />
              <receptionReport receiverCallsign="B" senderCallsign="W1AW" senderLocator="EM65" frequency="14074000" sNR="-5" />
            </receptionReports>
            """;

        // Both rows lack one of the two grids needed to draw a path, so neither survives.
        PskReporterController.ParseReports(xml).Should().BeEmpty();
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("not xml at all <<<")]
    [InlineData("<html><body>503 rate limited</body></html>")]
    public void ParseReports_ReturnsEmptyOnBadInput(string xml)
    {
        PskReporterController.ParseReports(xml).Should().BeEmpty();
    }
}
