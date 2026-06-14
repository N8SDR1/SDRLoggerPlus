using FluentAssertions;
using SDRLoggerPlus.Server.Services.Wsjtx;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

[Trait("Category", "Unit")]
public class WsjtxQsoMapperTests
{
    private static WsjtxQsoLogged MakeQso(
        string? dxCall = "JA1ABC",
        ulong freqHz = 14_074_000,
        string? mode = "FT8")
        => new(
            Id: "WSJT-X",
            DateTimeOff: new DateTime(2026, 6, 10, 14, 30, 0, DateTimeKind.Utc),
            DxCall: dxCall,
            DxGrid: "PM95",
            TxFrequencyHz: freqHz,
            Mode: mode,
            ReportSent: "-10",
            ReportReceived: "-08",
            TxPower: "50",
            Comments: "tnx 73",
            Name: "Hiro",
            DateTimeOn: new DateTime(2026, 6, 10, 14, 28, 0, DateTimeKind.Utc),
            OperatorCall: "",
            MyCall: "W8XYZ",
            MyGrid: "EN82",
            ExchangeSent: "",
            ExchangeReceived: "",
            AdifPropagationMode: null);

    [Fact]
    public void ToCreateRequest_MapsCoreFields()
    {
        var req = WsjtxQsoMapper.ToCreateRequest(MakeQso())!;

        req.Callsign.Should().Be("JA1ABC");
        req.Band.Should().Be("20m");
        req.Mode.Should().Be("FT8");
        req.Frequency.Should().BeApproximately(14.074, 0.0001);
        req.TimeOn.Should().Be("1428");
        req.RstSent.Should().Be("-10");
        req.RstRcvd.Should().Be("-08");
        req.Grid.Should().Be("PM95");
        req.Name.Should().Be("Hiro");
        req.Country.Should().Be("Japan");   // cty enrichment
        req.Comment.Should().Be("tnx 73");
    }

    [Fact]
    public void ToCreateRequest_EmptyCall_ReturnsNull()
        => WsjtxQsoMapper.ToCreateRequest(MakeQso(dxCall: "  ")).Should().BeNull();

    [Fact]
    public void ToCreateRequest_EmptyMode_DefaultsToFt8()
        => WsjtxQsoMapper.ToCreateRequest(MakeQso(mode: ""))!.Mode.Should().Be("FT8");
}

[Trait("Category", "Unit")]
public class WsjtxDedupeTests
{
    private static readonly DateTime T0 = new(2026, 6, 10, 14, 30, 0, DateTimeKind.Utc);

    [Fact]
    public void TryAdd_FirstTime_True_RepeatWithinWindow_False()
    {
        var dedupe = new WsjtxDedupe();
        dedupe.TryAdd("JA1ABC", T0, T0).Should().BeTrue();
        dedupe.TryAdd("JA1ABC", T0, T0.AddSeconds(30)).Should().BeFalse();
        dedupe.TryAdd("ja1abc", T0, T0.AddSeconds(40)).Should().BeFalse(); // case-insensitive
    }

    [Fact]
    public void TryAdd_DifferentCallOrMinute_True()
    {
        var dedupe = new WsjtxDedupe();
        dedupe.TryAdd("JA1ABC", T0, T0).Should().BeTrue();
        dedupe.TryAdd("DL1ABC", T0, T0).Should().BeTrue();
        dedupe.TryAdd("JA1ABC", T0.AddMinutes(2), T0.AddMinutes(2)).Should().BeTrue();
    }

    [Fact]
    public void TryAdd_AfterWindowExpiry_TrueAgain()
    {
        var dedupe = new WsjtxDedupe(TimeSpan.FromMinutes(10));
        dedupe.TryAdd("JA1ABC", T0, T0).Should().BeTrue();
        dedupe.TryAdd("JA1ABC", T0, T0.AddMinutes(11)).Should().BeTrue();
    }
}
