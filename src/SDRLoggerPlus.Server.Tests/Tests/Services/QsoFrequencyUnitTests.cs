using System.Text.RegularExpressions;
using FluentAssertions;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Services;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

/// <summary>
/// Qso.Frequency is stored in kHz. ADIF's FREQ field is defined in MHz, so every
/// exporter that emits FREQ must divide by 1000.
///
/// These tests exist because each exporter previously had its own private notion of
/// the unit: QRZ/ADIF divided, while ClubLog/eQSL/HrdLog emitted the raw kHz value
/// into an MHz field — a silent 1000x error on every upload. Per-service tests all
/// passed because each asserted only its own assumption. The cross-service agreement
/// test below is what actually pins the contract.
/// </summary>
[Trait("Category", "Unit")]
public class QsoFrequencyUnitTests
{
    private static Qso KHzQso(double frequencyKhz, string band) => new()
    {
        Callsign = "W1AW",
        QsoDate = new DateTime(2026, 5, 30, 0, 0, 0, DateTimeKind.Utc),
        TimeOn = "1201",
        Band = band,
        Mode = "SSB",
        Frequency = frequencyKhz,
        RstSent = "59",
        RstRcvd = "59",
    };

    /// <summary>Pulls the FREQ value out of an ADIF record.</summary>
    private static string Freq(string adif)
    {
        var m = Regex.Match(adif, @"<FREQ:\d+>([^<\s]+)", RegexOptions.IgnoreCase);
        m.Success.Should().BeTrue("the record should contain a FREQ field: {0}", adif);
        return m.Groups[1].Value;
    }

    [Theory]
    [InlineData(14074.0, "20m", 14.074)]
    [InlineData(50125.0, "6m", 50.125)]     // the originally reported case
    [InlineData(7268.0, "40m", 7.268)]
    [InlineData(474.0, "630m", 0.474)]      // sub-1000 kHz — must not be mistaken for MHz
    [InlineData(144174.0, "2m", 144.174)]
    public void AllExporters_EmitFreqInMhz(double storedKhz, string band, double expectedMhz)
    {
        var qso = KHzQso(storedKhz, band);

        var emitted = new Dictionary<string, string>
        {
            ["ClubLog"] = Freq(ClubLogService.BuildAdif(qso, "N9BC")),
            ["eQSL"] = Freq(EqslService.BuildAdif(qso, "N9BC", null)),
            ["HrdLog"] = Freq(HrdLogService.BuildAdif(qso, "N9BC")),
        };

        foreach (var (service, value) in emitted)
        {
            double.Parse(value, System.Globalization.CultureInfo.InvariantCulture)
                .Should().BeApproximately(expectedMhz, 0.0000005,
                    "{0} must emit ADIF FREQ in MHz, not the raw kHz value", service);
        }
    }

    [Fact]
    public void AllExporters_AgreeWithEachOther()
    {
        var qso = KHzQso(14074.0, "20m");

        var values = new[]
        {
            Freq(ClubLogService.BuildAdif(qso, "N9BC")),
            Freq(EqslService.BuildAdif(qso, "N9BC", null)),
            Freq(HrdLogService.BuildAdif(qso, "N9BC")),
        };

        values.Distinct().Should().ContainSingle(
            "every exporter must emit the same FREQ for the same stored QSO, but got: {0}",
            string.Join(" | ", values));
    }

    [Fact]
    public void Exporters_OmitFreq_WhenFrequencyMissing()
    {
        var qso = KHzQso(0, "20m");
        qso.Frequency = null;

        ClubLogService.BuildAdif(qso, "N9BC").Should().NotContain("<FREQ:");
        EqslService.BuildAdif(qso, "N9BC", null).Should().NotContain("<FREQ:");
        HrdLogService.BuildAdif(qso, "N9BC").Should().NotContain("<FREQ:");
    }

    [Fact]
    public void Exporters_UseInvariantCulture_ForDecimalPoint()
    {
        var qso = KHzQso(14074.0, "20m");

        // A comma decimal separator would silently corrupt the ADIF field.
        Freq(ClubLogService.BuildAdif(qso, "N9BC")).Should().Contain(".").And.NotContain(",");
        Freq(EqslService.BuildAdif(qso, "N9BC", null)).Should().Contain(".").And.NotContain(",");
        Freq(HrdLogService.BuildAdif(qso, "N9BC")).Should().Contain(".").And.NotContain(",");
    }
}
