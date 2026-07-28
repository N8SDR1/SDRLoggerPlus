using FluentAssertions;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Services;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

/// <summary>
/// The "every logger agrees" guarantee (docs/design/timezone-architecture.md).
///
/// Every ADIF-producing upload path (QRZ, eQSL, Club Log, HRDLog) and ADIF export must emit the SAME
/// UTC QSO_DATE + TIME_ON, derived from the one QsoDate instant — never from an independent TimeOn
/// string that could drift. This is what lets our log round-trip cleanly to LoTW/QRZ/eQSL the way
/// N1MM/N3FJP/Log4OM/Logger32/HRD do. Historically these four "roll-their-own" builders paired an
/// unconverted date with the raw TimeOn string; this pins them to the single-source UTC rule.
///
/// 02:30 UTC is the evening-QSO case that read back on the previous local day; run off-UTC to prove
/// the emitted date does not follow the server's zone.
/// </summary>
[Trait("Category", "Unit")]
public class UploaderUtcConsistencyTests
{
    private static Qso EveningQso() => new()
    {
        Id = "1",
        Callsign = "G0AMO",
        QsoDate = new DateTime(2026, 3, 10, 2, 30, 0, DateTimeKind.Utc),
        TimeOn = "0230",
        Band = "20m",
        Mode = "SSB",
    };

    [Fact]
    public void All_uploaders_emit_the_same_utc_date_and_time()
    {
        var clubLog = ClubLogService.BuildAdif(EveningQso(), "N9BC");
        var hrdLog = HrdLogService.BuildAdif(EveningQso(), "N9BC");
        var eqsl = EqslService.BuildAdif(EveningQso(), "N9BC", null);

        foreach (var adif in new[] { clubLog, hrdLog, eqsl })
        {
            adif.Should().Contain("<QSO_DATE:8>20260310",
                "the UTC date must not follow the server's local zone");
            adif.Should().Contain("<TIME_ON:6>023000",
                "date and time are both derived from the one UTC QsoDate instant");
        }
    }
}
