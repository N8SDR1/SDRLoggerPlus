using System.Xml.Linq;
using FluentAssertions;
using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Contracts.Models.Contesting;
using SDRLoggerPlus.Server.Services.Contesting;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

[Trait("Category", "Unit")]
public class N1mmXmlBuilderTests
{
    private static ContestDefinition Def() => SeedContests.All.First(d => d.Id == "cq-ww-cw");
    private static ContestSession Session() => new() { DefinitionId = "cq-ww-cw" };

    private static Qso Qso() => new()
    {
        Callsign = "DL1ABC",
        Band = "20m",
        Mode = "CW",
        Frequency = 14042.0, // kHz
        QsoDate = new DateTime(2026, 5, 30, 0, 0, 0, DateTimeKind.Utc),
        TimeOn = "120130",
        RstSent = "599",
        RstRcvd = "599",
        Country = "Fed. Rep. of Germany",
        Continent = "EU",
        Contest = new ContestInfo { RcvdZone = "14", QsoPoints = 3, Mults = new() { "CqZone:14@20m" } },
    };

    [Fact]
    public void ContactInfo_CarriesKeyFields()
    {
        var xml = XElement.Parse(N1mmXmlBuilder.ContactInfo(Def(), Session(), Qso(), "N9BC"));

        xml.Name.LocalName.Should().Be("contactinfo");
        xml.Element("contestname")!.Value.Should().Be("CQ-WW-CW");
        xml.Element("mycall")!.Value.Should().Be("N9BC");
        xml.Element("call")!.Value.Should().Be("DL1ABC");
        xml.Element("band")!.Value.Should().Be("14");
        xml.Element("txfreq")!.Value.Should().Be("1404200"); // kHz × 100 (tens of Hz)
        xml.Element("mode")!.Value.Should().Be("CW");
        xml.Element("zone")!.Value.Should().Be("14");
        xml.Element("wpxprefix")!.Value.Should().Be("DL1");
        xml.Element("points")!.Value.Should().Be("3");
        xml.Element("ismultiplier1")!.Value.Should().Be("1");
        xml.Element("timestamp")!.Value.Should().Be("2026-05-30 12:01:30");
    }

    [Fact]
    public void DynamicResults_CarriesScore()
    {
        var state = new ContestStateDto("s", "cq-ww-cw", "CQ WW DX CW", "L", ContestRole.All, false, 0,
            Qsos: 4, Dupes: 1, Points: 12, Multipliers: 7, BonusPoints: 0, Score: 84,
            RateLastHour: 40, RateLast10: 0, MultsBySource: new(),
            IsStale: false, StartedAt: "2026-05-30T12:00:00Z");

        var xml = XElement.Parse(N1mmXmlBuilder.DynamicResults(Def(), state, "N9BC"));

        xml.Name.LocalName.Should().Be("dynamicresults");
        xml.Element("contest")!.Value.Should().Be("CQ-WW-CW");
        xml.Element("call")!.Value.Should().Be("N9BC");
        xml.Element("qsos")!.Value.Should().Be("4");
        xml.Element("points")!.Value.Should().Be("12");
        xml.Element("mults")!.Value.Should().Be("7");
        xml.Element("score")!.Value.Should().Be("84");
    }
}
