using FluentAssertions;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Contracts.Models.Contesting;
using SDRLoggerPlus.Server.Services.Contesting;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

[Trait("Category", "Unit")]
public class CabrilloExporterTests
{
    private static ContestDefinition Wpx() =>
        SeedContests.All.First(d => d.Id == "cq-wpx-cw");

    private static ContestSession Session() => new()
    {
        DefinitionId = "cq-wpx-cw",
        MyExchange = new MyExchange { Continent = "NA", Dxcc = 291, Power = "LOW" },
    };

    private static Qso WpxQso(string call, string serialSent, string serialRcvd) => new()
    {
        Callsign = call,
        Band = "20m",
        Mode = "CW",
        Frequency = 14042.0, // kHz
        QsoDate = new DateTime(2026, 5, 30, 0, 0, 0, DateTimeKind.Utc),
        TimeOn = "120130",
        RstSent = "599",
        RstRcvd = "599",
        Dxcc = 230,
        Continent = "EU",
        Contest = new ContestInfo { SerialSent = serialSent, SerialRcvd = serialRcvd },
    };

    [Fact]
    public void Generate_EmitsWellFormedHeaderAndTrailer()
    {
        var cbr = CabrilloExporter.Generate(Wpx(), Session(),
            new[] { WpxQso("DL1ABC", "001", "025") }, "N9BC", "EN54");

        var lines = cbr.Split('\n').Select(l => l.TrimEnd('\r')).ToList();
        lines.First().Should().Be("START-OF-LOG: 3.0");
        lines.Should().Contain("CONTEST: CQ-WPX-CW");
        lines.Should().Contain("CALLSIGN: N9BC");
        lines.Should().Contain("CATEGORY-MODE: CW");
        lines.Should().Contain("CATEGORY-POWER: LOW");
        lines.Should().Contain("GRID-LOCATOR: EN54");
        lines.Should().Contain(l => l.StartsWith("CLAIMED-SCORE:"));
        lines.Should().Contain(l => l == "END-OF-LOG:");
    }

    [Fact]
    public void Generate_QsoLine_HasCabrilloFieldsInOrder()
    {
        var cbr = CabrilloExporter.Generate(Wpx(), Session(),
            new[] { WpxQso("DL1ABC", "001", "025") }, "N9BC", null);

        var qsoLine = cbr.Split('\n').Select(l => l.TrimEnd('\r'))
            .First(l => l.StartsWith("QSO:"));
        var t = qsoLine.Split(' ', StringSplitOptions.RemoveEmptyEntries);

        t[0].Should().Be("QSO:");
        t[1].Should().Be("14042");          // freq kHz
        t[2].Should().Be("CW");             // mode token
        t[3].Should().Be("2026-05-30");     // date
        t[4].Should().Be("1201");           // time HHMM
        t[5].Should().Be("N9BC");           // my call
        t[6].Should().Be("599");            // sent RST
        t[7].Should().Be("001");            // sent serial
        t[8].Should().Be("DL1ABC");         // his call
        t[9].Should().Be("599");            // rcvd RST
        t[10].Should().Be("025");           // rcvd serial
    }

    [Fact]
    public void Generate_ClaimedScore_MatchesEngine()
    {
        // Two distinct WPX prefixes on 20m, both EU (3 pts each) = 6 pts × 2 mults = 12.
        var cbr = CabrilloExporter.Generate(Wpx(), Session(), new[]
        {
            WpxQso("DL1ABC", "001", "010"),
            WpxQso("G3XYZ", "002", "011"),
        }, "N9BC", null);

        cbr.Should().Contain("CLAIMED-SCORE: 12");
    }

    [Fact]
    public void Generate_UsesBandFrequencyFallback_WhenFreqMissing()
    {
        var qso = WpxQso("DL1ABC", "001", "025");
        qso.Frequency = null;
        var cbr = CabrilloExporter.Generate(Wpx(), Session(), new[] { qso }, "N9BC", null);

        var qsoLine = cbr.Split('\n').Select(l => l.TrimEnd('\r')).First(l => l.StartsWith("QSO:"));
        qsoLine.Split(' ', StringSplitOptions.RemoveEmptyEntries)[1].Should().Be("14030"); // 20m fallback
    }

    // -- role-split (QSO party) sent exchange --------------------------------

    // Ohio QSO Party-style: in-area ops send their county, out-of-area ops send
    // their state. The exchange fields are role overrides on the definition.
    private static ContestDefinition OhioQp() => new()
    {
        Id = "oh-qp", Name = "Ohio QSO Party", CabrilloName = "OH-QSO-PARTY",
        Bands = new() { "80m", "40m", "20m" }, Modes = new() { "CW", "SSB" },
        SentExchange = new()
        {
            new ContestField { Key = "rst", Type = ContestFieldType.Rst, Label = "RST" },
            new ContestField { Key = "state", Type = ContestFieldType.State, Label = "St" },
        },
        RcvdExchange = new()
        {
            new ContestField { Key = "rst", Type = ContestFieldType.Rst, Label = "RST" },
            new ContestField { Key = "state", Type = ContestFieldType.State, Label = "St/Co" },
        },
        HomeArea = new HomeArea { Kind = HomeAreaKind.StateCounty, States = new() { "OH" } },
        Roles = new()
        {
            [ContestRole.InArea] = new RoleRules
            {
                SentExchange = new()
                {
                    new ContestField { Key = "rst", Type = ContestFieldType.Rst, Label = "RST" },
                    new ContestField { Key = "county", Type = ContestFieldType.Text, Label = "Co" },
                },
            },
        },
    };

    private static Qso QpQso(string call, string rcvdLoc) => new()
    {
        Callsign = call, Band = "20m", Mode = "CW", Frequency = 14042.0,
        QsoDate = new DateTime(2026, 8, 22, 0, 0, 0, DateTimeKind.Utc), TimeOn = "120130",
        RstSent = "599", RstRcvd = "599",
        Contest = new ContestInfo { RcvdState = rcvdLoc },
    };

    [Fact]
    public void Generate_InAreaOp_SendsCounty()
    {
        var session = new ContestSession
        {
            DefinitionId = "oh-qp", Role = ContestRole.InArea,
            MyExchange = new MyExchange { State = "OH", County = "CUYA" },
        };

        var cbr = CabrilloExporter.Generate(OhioQp(), session, new[] { QpQso("W8XYZ", "GEAU") }, "N8SDR", null);
        var t = cbr.Split('\n').Select(l => l.TrimEnd('\r')).First(l => l.StartsWith("QSO:"))
            .Split(' ', StringSplitOptions.RemoveEmptyEntries);

        // ... N8SDR 599 <sent> W8XYZ 599 <rcvd>
        t[6].Should().Be("599");
        t[7].Should().Be("CUYA");   // in-area op sends its county, not its state
        t[8].Should().Be("W8XYZ");
    }

    [Fact]
    public void Generate_OutAreaOp_SendsState()
    {
        var session = new ContestSession
        {
            DefinitionId = "oh-qp", Role = ContestRole.OutArea,
            MyExchange = new MyExchange { State = "TX", County = null },
        };

        var cbr = CabrilloExporter.Generate(OhioQp(), session, new[] { QpQso("W8XYZ", "GEAU") }, "K5ABC", null);
        var t = cbr.Split('\n').Select(l => l.TrimEnd('\r')).First(l => l.StartsWith("QSO:"))
            .Split(' ', StringSplitOptions.RemoveEmptyEntries);

        // OutArea has no role override, so it falls back to the top-level state field.
        t[7].Should().Be("TX");
    }
}
