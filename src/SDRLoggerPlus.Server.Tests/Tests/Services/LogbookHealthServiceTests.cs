using FluentAssertions;
using Moq;
using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database;
using SDRLoggerPlus.Server.Services;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

/// <summary>
/// Verify QSO times — the read-only audit that finds rows whose QsoDate lost its time to the old
/// edit-modal flatten bug (docs/design/timezone-architecture.md §5a). It must reconstruct ONLY the
/// unambiguous flattened case and never mis-flag a consistent or genuinely-midnight QSO.
/// </summary>
[Trait("Category", "Unit")]
public class LogbookHealthServiceTests
{
    private static Qso Q(string id, DateTime qsoDate, string? timeOn) => new()
    {
        Id = id, Callsign = "W1AW", QsoDate = qsoDate, TimeOn = timeOn!, Band = "20m", Mode = "SSB",
    };

    private static async Task<QsoTimeAuditResult> Audit(params Qso[] qsos)
    {
        var repo = new Mock<IQsoRepository>();
        repo.Setup(r => r.GetAllAsync()).ReturnsAsync(qsos.ToList());
        return await new LogbookHealthService(repo.Object).AuditQsoTimesAsync();
    }

    [Fact]
    public async Task Consistent_qso_is_not_flagged()
    {
        var r = await Audit(Q("1", new DateTime(2026, 3, 10, 14, 30, 0, DateTimeKind.Utc), "1430"));
        r.Consistent.Should().Be(1);
        r.FixableLostTime.Should().Be(0);
        r.Ambiguous.Should().Be(0);
    }

    [Fact]
    public async Task Flattened_qso_is_fixable_with_reconstructed_instant()
    {
        // QsoDate lost its time (midnight) but TimeOn kept 14:30.
        var r = await Audit(Q("1", new DateTime(2026, 3, 10, 0, 0, 0, DateTimeKind.Utc), "1430"));

        r.FixableLostTime.Should().Be(1);
        r.FixableSamples.Should().ContainSingle();
        r.FixableSamples[0].ProposedQsoDate.Should()
            .Be(new DateTime(2026, 3, 10, 14, 30, 0, DateTimeKind.Utc));
        r.FixableSamples[0].ProposedQsoDate!.Value.Kind.Should().Be(DateTimeKind.Utc);
    }

    [Fact]
    public async Task Genuine_midnight_qso_is_consistent_not_fixable()
    {
        // A real 00:00 UTC contact: QsoDate midnight AND TimeOn 0000 — nothing lost.
        var r = await Audit(Q("1", new DateTime(2026, 3, 10, 0, 0, 0, DateTimeKind.Utc), "0000"));
        r.Consistent.Should().Be(1);
        r.FixableLostTime.Should().Be(0);
    }

    [Fact]
    public async Task Disagreement_that_is_not_a_flatten_is_ambiguous_only()
    {
        // Times disagree but QsoDate isn't midnight — no safe signal, report only.
        var r = await Audit(Q("1", new DateTime(2026, 3, 10, 14, 30, 0, DateTimeKind.Utc), "1200"));
        r.Ambiguous.Should().Be(1);
        r.FixableLostTime.Should().Be(0);
        r.AmbiguousSamples.Should().ContainSingle();
    }

    [Fact]
    public async Task HhmmssTimeOn_is_compared_at_minute_granularity()
    {
        var r = await Audit(Q("1", new DateTime(2026, 3, 10, 14, 30, 45, DateTimeKind.Utc), "143045"));
        r.Consistent.Should().Be(1);
    }

    [Fact]
    public async Task Local_kind_qsodate_is_normalized_to_utc_before_comparing()
    {
        // A Kind=Local QsoDate must be projected to UTC first (defensive — post-fix reads are Utc).
        var localMidnightUtcEquivalent = new DateTime(2026, 3, 10, 0, 0, 0, DateTimeKind.Utc).ToLocalTime();
        var r = await Audit(Q("1", localMidnightUtcEquivalent, "1430"));
        r.FixableLostTime.Should().Be(1);
        r.FixableSamples[0].ProposedQsoDate.Should()
            .Be(new DateTime(2026, 3, 10, 14, 30, 0, DateTimeKind.Utc));
    }
}
