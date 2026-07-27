using System.Text;
using FluentAssertions;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging;
using Moq;
using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database;
using SDRLoggerPlus.Server.Hubs;
using SDRLoggerPlus.Server.Services;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services.Adif;

/// <summary>
/// Confirmation-merge matching, LoTW-shaped. LoTW reports a submode QSO under its ADIF
/// parent mode — an MSK144 or FT4 contact comes back MODE=MFSK with the real mode in
/// SUBMODE — while the log stores what the operator (or WSJT-X) wrote: the submode name.
/// The old exact-only key ("…|MSK144" vs "…|MFSK") never matched, so every such
/// confirmation was silently counted as unmatched and dropped. The same defect class the
/// import dedupe key had (#38), pointing the other way.
/// </summary>
[Trait("Category", "Unit")]
public class AdifConfirmationMergeTests
{
    private readonly AdifService _service;
    private readonly List<Qso> _stored = new();

    public AdifConfirmationMergeTests()
    {
        var repo = new Mock<IQsoRepository>();
        repo.Setup(r => r.GetAllAsync()).ReturnsAsync(() => _stored.ToList());
        repo.Setup(r => r.UpdateAsync(It.IsAny<string>(), It.IsAny<Qso>())).ReturnsAsync(true);

        var hub = new Mock<IHubContext<LogHub, ILogHubClient>>();
        var clients = new Mock<IHubClients<ILogHubClient>>();
        clients.Setup(c => c.All).Returns(new Mock<ILogHubClient>().Object);
        hub.Setup(h => h.Clients).Returns(clients.Object);

        _service = new AdifService(
            repo.Object,
            new Mock<ISettingsRepository>().Object,
            hub.Object,
            new Mock<ILogger<AdifService>>().Object);
    }

    private static readonly DateTime Day = new(2026, 7, 27, 0, 0, 0, DateTimeKind.Utc);

    private Qso Logged(string mode, string call = "AA5HH", string band = "6m", DateTime? date = null)
    {
        var q = new Qso
        {
            Id = Guid.NewGuid().ToString(),
            Callsign = call,
            Band = band,
            Mode = mode,
            QsoDate = date ?? Day,
            TimeOn = "1600",
        };
        _stored.Add(q);
        return q;
    }

    /// <summary>A one-record LoTW-style confirmation report.</summary>
    private static Stream Report(string mode, string? submode = null,
        string call = "AA5HH", string band = "6m", string date = "20260727")
    {
        var sb = new StringBuilder()
            .Append($"<CALL:{call.Length}>{call} <QSO_DATE:8>{date} <TIME_ON:4>1600 ")
            .Append($"<BAND:{band.Length}>{band} <MODE:{mode.Length}>{mode} ");
        if (submode != null) sb.Append($"<SUBMODE:{submode.Length}>{submode} ");
        sb.Append("<QSL_RCVD:1>Y <EOR>");
        return new MemoryStream(Encoding.UTF8.GetBytes(sb.ToString()));
    }

    private static bool LotwConfirmed(Qso q) =>
        string.Equals(q.Qsl?.Lotw?.Rcvd, "Y", StringComparison.OrdinalIgnoreCase);

    [Theory]
    [InlineData("MSK144")] // the reporter of #42 is an MSK144 operator — this was his next bug
    [InlineData("FT4")]
    [InlineData("JS8")]
    [InlineData("Q65")]
    public async Task AnMfskReportConfirmsTheSubmodeQsoItDescribes(string loggedMode)
    {
        var qso = Logged(loggedMode);

        var result = await _service.MergeConfirmationsAsync(Report("MFSK"), ConfirmationSource.Lotw);

        LotwConfirmed(qso).Should().BeTrue(
            $"LoTW reports a {loggedMode} QSO as MODE=MFSK, and dropping it loses a real confirmation");
        result.Updated.Should().Be(1);
        result.Unmatched.Should().Be(0);
    }

    [Fact]
    public async Task AJt65ReportConfirmsAJt65bQso()
    {
        var qso = Logged("JT65B");

        await _service.MergeConfirmationsAsync(Report("JT65"), ConfirmationSource.Lotw);

        LotwConfirmed(qso).Should().BeTrue();
    }

    [Fact]
    public async Task TheSubmodePicksTheRightSiblingWhenTwoFamilyModesShareTheDay()
    {
        // Worked the same station on 6 m FT4 AND MSK144 the same day — plausible during
        // a contest. The report's SUBMODE says which one this confirmation belongs to.
        // The decoy is logged FIRST: a nearest-date tie keeps the first-inserted
        // candidate, so this fails — rather than passing vacuously — if the submode
        // level ever stops running.
        var msk = Logged("MSK144");
        var ft4 = Logged("FT4");

        await _service.MergeConfirmationsAsync(Report("MFSK", submode: "FT4"), ConfirmationSource.Lotw);

        LotwConfirmed(ft4).Should().BeTrue("SUBMODE=FT4 names this QSO");
        LotwConfirmed(msk).Should().BeFalse("the MSK144 QSO has its own confirmation coming");
    }

    [Fact]
    public async Task AnExactModeMatchAlwaysBeatsAFamilyMatch()
    {
        // A log can legitimately hold a literal "MFSK" QSO next to an MSK144 one.
        // Decoy first — see TheSubmodePicksTheRightSibling for why order matters.
        var msk = Logged("MSK144");
        var literal = Logged("MFSK");

        await _service.MergeConfirmationsAsync(Report("MFSK"), ConfirmationSource.Lotw);

        LotwConfirmed(literal).Should().BeTrue("exact key hit — the family fallback must not run");
        LotwConfirmed(msk).Should().BeFalse();
    }

    [Fact]
    public async Task AReportNamingASiblingSubmodeConfirmsNothing()
    {
        // The log holds only an FT4 QSO; the report says MODE=MFSK SUBMODE=MSK144 —
        // an explicit statement that the confirmed contact was MSK144. That QSO is
        // simply not in this log (uploaded from elsewhere, or deleted), and the
        // family fallback must not hand its confirmation to the FT4 sibling.
        var ft4 = Logged("FT4");

        var result = await _service.MergeConfirmationsAsync(
            Report("MFSK", submode: "MSK144"), ConfirmationSource.Lotw);

        LotwConfirmed(ft4).Should().BeFalse("the report names a different contact");
        result.Unmatched.Should().Be(1, "a missing QSO must be reported missing, not absorbed");
        result.Updated.Should().Be(0);
    }

    [Fact]
    public async Task AKnownSubmodeStillMatchesALiteralParentModeRow()
    {
        // The sibling guard must not overshoot: a log row stored as the literal
        // parent ("MFSK") is a legitimate target for any submode of that family.
        var literal = Logged("MFSK");

        await _service.MergeConfirmationsAsync(
            Report("MFSK", submode: "MSK144"), ConfirmationSource.Lotw);

        LotwConfirmed(literal).Should().BeTrue();
    }

    [Fact]
    public async Task AnUnknownSubmodeDoesNotBlockTheParentModeFallback()
    {
        // LoTW ships submodes this app hasn't heard of yet. The unknown submode's
        // miss must not eat the MODE=MFSK family lookup that still identifies the QSO.
        var qso = Logged("MSK144");

        await _service.MergeConfirmationsAsync(
            Report("MFSK", submode: "FUTUREMODE"), ConfirmationSource.Lotw);

        LotwConfirmed(qso).Should().BeTrue();
    }

    [Fact]
    public async Task AnMfskReportDoesNotConfirmAnFt8Qso()
    {
        // FT8 is its own ADIF mode, not an MFSK submode. Widening must stop at the
        // spec-defined families or the merge starts inventing confirmations.
        var qso = Logged("FT8");

        var result = await _service.MergeConfirmationsAsync(Report("MFSK"), ConfirmationSource.Lotw);

        LotwConfirmed(qso).Should().BeFalse();
        result.Unmatched.Should().Be(1);
    }

    [Fact]
    public async Task AUsbQsoStillMatchesAnSsbReport()
    {
        // The pre-existing sideband collapse, pinned so the family work can't regress it.
        var qso = Logged("USB", band: "20m");

        await _service.MergeConfirmationsAsync(Report("SSB", band: "20m"), ConfirmationSource.Lotw);

        LotwConfirmed(qso).Should().BeTrue();
    }

    [Fact]
    public async Task TheDateWindowStillGuardsFamilyMatches()
    {
        // Family matching widens the mode, never the date: a QSO days away is a
        // different contact no matter how well the mode lines up.
        var qso = Logged("MSK144", date: Day.AddDays(-5));

        var result = await _service.MergeConfirmationsAsync(Report("MFSK"), ConfirmationSource.Lotw);

        LotwConfirmed(qso).Should().BeFalse();
        result.Unmatched.Should().Be(1);
    }
}
