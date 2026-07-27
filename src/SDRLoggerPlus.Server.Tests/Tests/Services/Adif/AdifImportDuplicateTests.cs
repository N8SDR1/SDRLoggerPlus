using System.Text;
using FluentAssertions;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging;
using Moq;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database;
using SDRLoggerPlus.Server.Hubs;
using SDRLoggerPlus.Server.Services;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services.Adif;

/// <summary>
/// The defect these cover, measured on the live 24,544-record log: 1,027 groups of records
/// shared a callsign, date and time — the same contact stored more than once — and in 876 of
/// them the copies disagreed about the mode. 1,046 records of pure inflation, 4.26% of the log.
///
/// Cause: the duplicate key was built from callsign + band + mode. Export the same QSO from two
/// programs and the mode comes back spelled two ways ("DATA" and "MFSK", "PH" and "SSB"), so the
/// key differs, so the duplicate check misses, so it imports again. Mode is the least reliable
/// field in a real ADIF file and identity was hanging on it.
/// </summary>
[Trait("Category", "Unit")]
public class AdifImportDuplicateTests
{
    private readonly AdifService _service;
    private readonly List<Qso> _stored = new();

    public AdifImportDuplicateTests()
    {
        var repo = new Mock<IQsoRepository>();
        repo.Setup(r => r.GetAllAsync()).ReturnsAsync(() => _stored.ToList());
        repo.Setup(r => r.CreateBulkAsync(It.IsAny<IEnumerable<Qso>>()))
            .Callback<IEnumerable<Qso>>(q => _stored.AddRange(q))
            .ReturnsAsync((IEnumerable<Qso> q) => q);

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

    private static Stream Adif(string band, string mode) => new MemoryStream(Encoding.UTF8.GetBytes(
        $"<CALL:5>G0AMO <QSO_DATE:8>20260303 <TIME_ON:4>2113 <BAND:{band.Length}>{band} " +
        $"<MODE:{mode.Length}>{mode} <FREQ:5>14084 <EOR>"));

    [Theory]
    [InlineData("MFSK", "DATA")]   // 422 groups in the live log
    [InlineData("SSB", "PH")]      // 340 groups
    [InlineData("FT8", "DATA")]    // 41 groups
    [InlineData("FT2", "29")]      // 30 groups — two truncations of one field
    [InlineData("PSK31", "PSK3")]  // 5 groups
    public async Task SameContactUnderTwoModeSpellingsImportsOnce(string first, string second)
    {
        await _service.ImportAdifAsync(Adif("20M", first));
        var result = await _service.ImportAdifAsync(Adif("20M", second));

        _stored.Should().HaveCount(1, "it is one contact — the mode spelling changed, not the QSO");
        result.SkippedDuplicates.Should().Be(1);
        result.ImportedCount.Should().Be(0);
    }

    [Fact]
    public async Task SameContactUnderTwoBandSpellingsImportsOnce()
    {
        // 13,106 records carry an upper-case band. Before normalisation "40M" and "40m"
        // produced different keys, so this pair duplicated too.
        await _service.ImportAdifAsync(Adif("40M", "FT8"));
        await _service.ImportAdifAsync(Adif("40m", "FT8"));

        _stored.Should().HaveCount(1);
    }

    [Fact]
    public async Task TheSameStationOnADifferentBandIsStillTwoContacts()
    {
        // The guard against over-correcting: band remains part of identity. Working a station
        // on 20m and again on 40m is two QSOs, and dropping band from the key would merge them.
        await _service.ImportAdifAsync(Adif("20M", "FT8"));
        await _service.ImportAdifAsync(Adif("40M", "FT8"));

        _stored.Should().HaveCount(2);
    }

    [Fact]
    public async Task RepeatedImportOfTheSameFileAddsNothing()
    {
        await _service.ImportAdifAsync(Adif("20M", "FT8"));
        await _service.ImportAdifAsync(Adif("20M", "FT8"));
        await _service.ImportAdifAsync(Adif("20M", "FT8"));

        _stored.Should().HaveCount(1);
    }

    [Fact]
    public async Task ImportReportsWhatItCorrectedAndWhatItCouldNotUnderstand()
    {
        // The other half of the fix: the result used to be four integers, so a file full of
        // malformed modes looked exactly like a clean one.
        var result = await _service.ImportAdifAsync(Adif("40M", "FT2"));

        result.Issues.Should().NotBeNull();

        var band = result.Issues!.Single(i => i.Field == "band");
        band.OriginalValue.Should().Be("40M");
        band.StoredValue.Should().Be("40m");
        band.Action.Should().Be("Corrected");

        var mode = result.Issues!.Single(i => i.Field == "mode");
        mode.OriginalValue.Should().Be("FT2");
        mode.StoredValue.Should().Be("FT2", "an unrecognised mode is kept, not guessed at");
        mode.Action.Should().Be("Flagged");
    }

    [Fact]
    public async Task ACleanFileProducesNoIssues()
    {
        var result = await _service.ImportAdifAsync(Adif("20m", "FT8"));

        result.Issues.Should().BeEmpty("a clean import must not manufacture noise to report");
    }

    [Fact]
    public async Task RepeatedBadValuesAreGroupedWithACountRatherThanListedOnce_Each()
    {
        var adif = new StringBuilder();
        for (var i = 0; i < 5; i++)
        {
            adif.Append($"<CALL:5>G0AM{i} <QSO_DATE:8>20260303 <TIME_ON:4>211{i} ")
                .Append("<BAND:3>20M <MODE:3>FT2 <EOR>\n");
        }

        var result = await _service.ImportAdifAsync(
            new MemoryStream(Encoding.UTF8.GetBytes(adif.ToString())));

        result.ImportedCount.Should().Be(5);
        result.Issues!.Single(i => i.Field == "mode").Count.Should().Be(5);
        result.Issues!.Single(i => i.Field == "band").Count.Should().Be(5);
    }
}
