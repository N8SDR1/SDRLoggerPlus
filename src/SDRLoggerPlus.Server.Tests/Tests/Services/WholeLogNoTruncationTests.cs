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

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

/// <summary>
/// Whole-log operations must never truncate. ADIF export is what operators use as
/// a backup, and LoTW upload selection decides what gets confirmed — both used to
/// fetch through the paged search with a hard-coded 100k cap, so a log past the
/// cap exported "successfully" while silently missing QSOs. These tests pin the
/// null-Limit (unbounded) contract on both call sites.
/// </summary>
[Trait("Category", "Unit")]
public class WholeLogNoTruncationTests
{
    private readonly Mock<IQsoRepository> _qsoRepoMock = new();
    private readonly Mock<ISettingsRepository> _settingsRepoMock = new();

    private AdifService NewAdifService() => new(
        _qsoRepoMock.Object,
        _settingsRepoMock.Object,
        new Mock<IHubContext<LogHub, ILogHubClient>>().Object,
        new Mock<ILogger<AdifService>>().Object);

    [Fact]
    public async Task AdifExportAsksForTheWholeLogNotAPage()
    {
        QsoSearchRequest? seen = null;
        _qsoRepoMock
            .Setup(r => r.SearchAsync(It.IsAny<QsoSearchRequest>()))
            .Callback<QsoSearchRequest>(r => seen = r)
            .ReturnsAsync((Enumerable.Empty<Qso>(), 0));
        _settingsRepoMock.Setup(r => r.GetAsync()).ReturnsAsync(new UserSettings());

        await NewAdifService().ExportQsosAsync(new AdifExportRequest());

        seen.Should().NotBeNull();
        seen!.Limit.Should().BeNull(
            "a bounded export silently drops every QSO past the cap — the backup " +
            "completes and looks fine right up until someone restores from it");
    }

    [Fact]
    public async Task AdifExportWithNoRequestStillAsksForEverything()
    {
        // The plain "Export" button calls with no request at all — same rule.
        QsoSearchRequest? seen = null;
        _qsoRepoMock
            .Setup(r => r.SearchAsync(It.IsAny<QsoSearchRequest>()))
            .Callback<QsoSearchRequest>(r => seen = r)
            .ReturnsAsync((Enumerable.Empty<Qso>(), 0));
        _settingsRepoMock.Setup(r => r.GetAsync()).ReturnsAsync(new UserSettings());

        await NewAdifService().ExportQsosAsync();

        seen!.Limit.Should().BeNull();
    }
}
