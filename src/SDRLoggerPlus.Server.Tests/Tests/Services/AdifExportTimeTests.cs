using FluentAssertions;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging;
using Moq;
using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Contracts.Models.Contesting;
using SDRLoggerPlus.Server.Core.Database;
using SDRLoggerPlus.Server.Hubs;
using SDRLoggerPlus.Server.Services;
using SDRLoggerPlus.Server.Services.Contesting;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

/// <summary>
/// ADIF export derives BOTH QSO_DATE and TIME_ON from Qso.QsoDate — deliberately,
/// so the two cannot drift apart. The consequence is that any write path storing a
/// truncated or wrongly-framed QsoDate corrupts the exported time even when
/// Qso.TimeOn holds the right value. ContestService did exactly that.
/// </summary>
[Trait("Category", "Unit")]
public class AdifExportTimeTests
{
    private readonly AdifService _service;
    private readonly Mock<IQsoRepository> _qsoRepoMock = new();
    private readonly Mock<ISettingsRepository> _settingsRepoMock = new();
    private readonly Mock<IHubContext<LogHub, ILogHubClient>> _hubMock = new();

    public AdifExportTimeTests()
    {
        _service = new AdifService(
            _qsoRepoMock.Object,
            _settingsRepoMock.Object,
            _hubMock.Object,
            new Mock<ILogger<AdifService>>().Object);
    }

    private void ReturnsQso(Qso qso) =>
        _qsoRepoMock
            .Setup(r => r.SearchAsync(It.IsAny<QsoSearchRequest>()))
            .ReturnsAsync((new[] { qso }.AsEnumerable(), 1));

    private static string Field(string adif, string name)
    {
        var marker = $"<{name}:";
        var start = adif.IndexOf(marker, StringComparison.OrdinalIgnoreCase);
        if (start < 0) return string.Empty;
        var close = adif.IndexOf('>', start);
        var len = int.Parse(adif[(start + marker.Length)..close]);
        return adif.Substring(close + 1, len);
    }

    /// <summary>
    /// The regression guard for the real bug: a contest QSO built by the actual
    /// production code must carry its time of day, not midnight. Storing now.Date
    /// here exported every contest QSO as TIME_ON=000000.
    /// </summary>
    [Fact]
    public void ContestQso_CarriesTimeOfDay_NotMidnight()
    {
        var qso = ContestService.BuildQso(
            new ContestSession { DefinitionId = "cq-ww-cw" },
            new ContestDefinition { Id = "cq-ww-cw", Name = "CQ WW CW" },
            new LogContestQsoRequest("W1AW", "20m", "CW", null, null, null),
            enrich: false);

        qso.QsoDate.TimeOfDay.Should().NotBe(TimeSpan.Zero,
            "ADIF export derives TIME_ON from QsoDate, so a midnight-truncated date " +
            "exports the contact as 000000 regardless of TimeOn");
        qso.QsoDate.ToString("HHmmss").Should().Be(qso.TimeOn);
    }

    /// <summary>End-to-end: that QSO exports with its real time.</summary>
    [Fact]
    public async Task Export_ContestQso_WritesRealTimeOn()
    {
        var qso = ContestService.BuildQso(
            new ContestSession { DefinitionId = "cq-ww-cw" },
            new ContestDefinition { Id = "cq-ww-cw", Name = "CQ WW CW" },
            new LogContestQsoRequest("W1AW", "20m", "CW", null, null, null),
            enrich: false);
        ReturnsQso(qso);

        var adif = await _service.ExportQsosAsync();

        Field(adif, "TIME_ON").Should().Be(qso.QsoDate.ToUniversalTime().ToString("HHmmss"));
        Field(adif, "TIME_ON").Should().NotBe("000000");
        Field(adif, "QSO_DATE").Should().Be(qso.QsoDate.ToUniversalTime().ToString("yyyyMMdd"));
    }

    /// <summary>
    /// A local-frame QsoDate (what the in-app create path stores) must still export
    /// the UTC date and time — the LoTW-dropping case the export code guards against.
    /// </summary>
    [Fact]
    public async Task Export_LocalFrameQso_ExportsUtc()
    {
        // 2026-06-27 04:52:33Z expressed at -05:00 is 2026-06-26 23:52:33 local.
        var utc = new DateTime(2026, 6, 27, 4, 52, 33, DateTimeKind.Utc);
        ReturnsQso(new Qso
        {
            Callsign = "W1AW",
            QsoDate = utc.ToLocalTime(),
            TimeOn = utc.ToString("HHmmss"),
            Band = "20m",
            Mode = "CW",
        });

        var adif = await _service.ExportQsosAsync();

        Field(adif, "QSO_DATE").Should().Be("20260627");
        Field(adif, "TIME_ON").Should().Be("045233");
    }
}
