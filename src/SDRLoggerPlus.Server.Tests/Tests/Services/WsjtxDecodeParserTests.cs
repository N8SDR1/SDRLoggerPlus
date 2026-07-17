using FluentAssertions;
using SDRLoggerPlus.Server.Services.Wsjtx;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

[Trait("Category", "Unit")]
public class WsjtxDecodeParserTests
{
    [Theory]
    // Plain CQ with grid
    [InlineData("CQ K1ABC FN42", "K1ABC", null, "FN42", true)]
    // Directional CQ (DX / continent / activity) — directional skipped, caller found
    [InlineData("CQ DX DL1XYZ JO31", "DL1XYZ", null, "JO31", true)]
    [InlineData("CQ NA VE3ABC FN25", "VE3ABC", null, "FN25", true)]
    [InlineData("CQ POTA W1AW FN31", "W1AW", null, "FN31", true)]
    // Frequency-directed CQ ("070" is not a callsign)
    [InlineData("CQ 070 K1ABC FN42", "K1ABC", null, "FN42", true)]
    // CQ without a grid
    [InlineData("CQ K1ABC", "K1ABC", null, null, true)]
    // Compound / portable caller
    [InlineData("CQ ZL/K1ABC RF80", "ZL/K1ABC", null, "RF80", true)]
    // Standard exchange: 2nd call is the transmitter, 1st is who they're calling
    [InlineData("K1ABC W9XYZ FN42", "W9XYZ", "K1ABC", "FN42", false)]
    [InlineData("K1ABC W9XYZ -15", "W9XYZ", "K1ABC", null, false)]
    [InlineData("K1ABC W9XYZ R-07", "W9XYZ", "K1ABC", null, false)]
    [InlineData("K1ABC W9XYZ RR73", "W9XYZ", "K1ABC", null, false)]
    // Portable transmitter
    [InlineData("W9XYZ K1ABC/P R+03", "K1ABC/P", "W9XYZ", null, false)]
    // Hashed callsign in angle brackets is stripped
    [InlineData("<K1ABC> W9XYZ FN42", "W9XYZ", "K1ABC", "FN42", false)]
    public void Parse_KnownForms(string message, string call, string? dxCall, string? grid, bool isCq)
    {
        var r = WsjtxDecodeParser.Parse(message);
        r.Should().NotBeNull();
        r!.Callsign.Should().Be(call);
        r.DxCall.Should().Be(dxCall);
        r.Grid.Should().Be(grid);
        r.IsCq.Should().Be(isCq);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData(null)]
    [InlineData("TNX 73 GL")]        // free text, no callsign
    [InlineData("CQ CQ CQ")]         // no caller
    public void Parse_Unparseable_ReturnsNull(string? message)
        => WsjtxDecodeParser.Parse(message).Should().BeNull();

    [Fact]
    public void Parse_GridNotMistakenForCallsign()
    {
        // FN42 has a letter and a digit but must be classified as a grid, not the caller.
        var r = WsjtxDecodeParser.Parse("CQ K1ABC FN42");
        r!.Callsign.Should().Be("K1ABC");
        r.Grid.Should().Be("FN42");
    }
}
