using FluentAssertions;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Services;
using SDRLoggerPlus.Server.Services.Ffma;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

/// <summary>
/// FFMA tallying, driven through the pure <see cref="AwardsService.ComputeFfma"/> so the
/// logic is tested against a controlled grid set rather than the shipped roster.
/// </summary>
[Trait("Category", "Unit")]
public class AwardsServiceFfmaTests
{
    // A small stand-in for the 488. ComputeFfma only counts grids in this set.
    private static readonly IReadOnlySet<string> Required =
        new HashSet<string>(StringComparer.Ordinal) { "EN82", "EN74", "FN31", "EM79" };

    private static bool LotwOrPaper(Qso q) =>
        string.Equals(q.Qsl?.Lotw?.Rcvd, "Y", StringComparison.OrdinalIgnoreCase) ||
        string.Equals(q.Qsl?.Rcvd, "Y", StringComparison.OrdinalIgnoreCase);

    private static Qso Q(string grid, string? lotw = null, string? paper = null,
        string? eqsl = null, DateTime? date = null)
        => new()
        {
            Id = Guid.NewGuid().ToString(),
            Callsign = "TEST",
            Grid = grid,
            Band = "6m",
            Mode = "FT8",
            QsoDate = date ?? new DateTime(2026, 6, 1, 0, 0, 0, DateTimeKind.Utc),
            Qsl = new QslStatus
            {
                Rcvd = paper,
                Lotw = new LotwStatus { Rcvd = lotw },
                Eqsl = new EqslStatus { Rcvd = eqsl },
            },
        };

    [Fact]
    public void EveryRequiredGridIsReported_WithNeededDefault()
    {
        var result = AwardsService.ComputeFfma([], Required, LotwOrPaper);

        result.TotalRequired.Should().Be(4);
        result.Worked.Should().Be(0);
        result.Confirmed.Should().Be(0);
        result.Grids.Should().HaveCount(4);
        result.Grids.Should().OnlyContain(g => g.Status == "needed");
    }

    [Fact]
    public void WorkedButUnconfirmedCountsAsWorkedNotConfirmed()
    {
        var result = AwardsService.ComputeFfma([Q("EN82")], Required, LotwOrPaper);

        result.Worked.Should().Be(1);
        result.Confirmed.Should().Be(0);
        result.Grids.Single(g => g.Grid == "EN82").Status.Should().Be("worked");
    }

    [Fact]
    public void LotwOrPaperConfirms_ButEqslDoesNot()
    {
        var result = AwardsService.ComputeFfma(
            [Q("EN82", lotw: "Y"), Q("EN74", paper: "Y"), Q("FN31", eqsl: "Y")],
            Required, LotwOrPaper);

        result.Confirmed.Should().Be(2); // EN82 (LoTW) + EN74 (paper)
        result.Grids.Single(g => g.Grid == "EN82").Status.Should().Be("confirmed");
        result.Grids.Single(g => g.Grid == "EN74").Status.Should().Be("confirmed");
        result.Grids.Single(g => g.Grid == "FN31").Status.Should().Be("worked"); // eQSL doesn't count
    }

    [Fact]
    public void GridsOutsideTheRequiredSetAreIgnored()
    {
        // JN58 is a valid grid but not required — must not inflate anything.
        var result = AwardsService.ComputeFfma([Q("JN58", lotw: "Y")], Required, LotwOrPaper);

        result.Worked.Should().Be(0);
        result.Grids.Should().NotContain(g => g.Grid == "JN58");
    }

    [Fact]
    public void SixCharGridsReduceToFourAndConfirmationIsSticky()
    {
        // Two QSOs to the same field+square, one confirmed — the grid is confirmed once.
        var result = AwardsService.ComputeFfma(
            [Q("EN82dk"), Q("EN82fm", lotw: "Y")], Required, LotwOrPaper);

        var en82 = result.Grids.Single(g => g.Grid == "EN82");
        en82.Status.Should().Be("confirmed");
        en82.QsoCount.Should().Be(2);
        result.Confirmed.Should().Be(1);
    }

    [Fact]
    public void ListCompleteOnlyWhenExactly488()
    {
        AwardsService.ComputeFfma([], Required, LotwOrPaper).ListComplete.Should().BeFalse();

        var full = new HashSet<string>(StringComparer.Ordinal);
        for (int i = 0; i < 488; i++) full.Add($"E{(char)('A' + i % 18)}{i / 100 % 10}{i % 10}");
        // (the generator can collide; just assert the boundary check keys off count == 488)
        AwardsService.ComputeFfma([], full.Count == 488 ? full : PadTo488(), LotwOrPaper)
            .ListComplete.Should().BeTrue();
    }

    private static IReadOnlySet<string> PadTo488()
    {
        var s = new HashSet<string>(StringComparer.Ordinal);
        int n = 0;
        for (char a = 'A'; a <= 'R' && s.Count < 488; a++)
            for (char b = 'A'; b <= 'R' && s.Count < 488; b++)
                for (int d = 0; d < 100 && s.Count < 488; d++)
                    s.Add($"{a}{b}{d / 10}{d % 10}");
        return s;
    }

    [Fact]
    public void ShippedRosterIsExactly488LegalGrids()
    {
        var grids = FfmaGridReference.RequiredGrids;

        grids.Should().HaveCount(488, "the shipped FFMA list must be the full official roster");
        FfmaGridReference.IsOfficial.Should().BeTrue();
        grids.Should().OnlyContain(g =>
            g.Length == 4 && g[0] >= 'A' && g[0] <= 'R' && g[1] >= 'A' && g[1] <= 'R'
            && g[2] >= '0' && g[2] <= '9' && g[3] >= '0' && g[3] <= '9');
        // Only the ten fields the award uses.
        var fields = new HashSet<string> { "CM", "CN", "DL", "DM", "DN", "EL", "EM", "EN", "FM", "FN" };
        grids.Should().OnlyContain(g => fields.Contains(g.Substring(0, 2)));
        // Two well-known anchors: the whole EM field (100) is required; EM79 (Detroit-ish) in it.
        grids.Should().Contain("EM79").And.Contain("EM00").And.Contain("FN31");
    }
}
