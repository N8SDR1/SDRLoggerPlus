using FluentAssertions;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging;
using Moq;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database;
using SDRLoggerPlus.Server.Hubs;
using SDRLoggerPlus.Server.Services;
using SDRLoggerPlus.Server.Services.Counties;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

/// <summary>
/// What happens to county data when someone imports a logbook. ADIF import
/// previously mapped STATE but not CNTY, so every county landed in the
/// unmapped-extras bucket instead of the field the award reads.
/// </summary>
[Trait("Category", "Unit")]
public class AdifCountyImportTests
{
    private readonly AdifService _service;

    public AdifCountyImportTests()
    {
        _service = new AdifService(
            new Mock<IQsoRepository>().Object,
            new Mock<ISettingsRepository>().Object,
            new Mock<IHubContext<LogHub, ILogHubClient>>().Object,
            new Mock<ILogger<AdifService>>().Object);
    }

    private static string Record(string extraFields) =>
        "<CALL:4>W1AW<QSO_DATE:8>20260722<TIME_ON:4>1234<BAND:3>20m<MODE:2>CW" +
        $"<COUNTRY:13>United States{extraFields}<EOR>";

    private Qso ParseOne(string adif) => _service.ParseAdif(adif).Single();

    [Fact]
    public void ImportPutsTheCountyInTheFieldTheAwardReads()
    {
        var qso = ParseOne(Record("<STATE:2>MN<CNTY:11>MN,Hennepin"));

        qso.Station!.County.Should().Be("Hennepin", "the ST, prefix is redundant — STATE already has it");
        qso.Station.State.Should().Be("MN");
    }

    [Theory]
    [InlineData("CNTY")]
    [InlineData("cnty")]
    [InlineData("Cnty")]
    public void ImportAcceptsAnyCasingOfTheFieldName(string fieldName)
    {
        var qso = ParseOne(Record($"<STATE:2>MN<{fieldName}:12>MN,Hennepin"));

        qso.Station!.County.Should().Be("Hennepin");
    }

    [Fact]
    public void ImportAcceptsACountyWithNoStatePrefix()
    {
        // The "ST," prefix is a convention, not a guarantee.
        var qso = ParseOne(Record("<STATE:2>MN<CNTY:8>Hennepin"));

        qso.Station!.County.Should().Be("Hennepin");
    }

    [Fact]
    public void ImportedCountyIsNoLongerDuplicatedIntoTheExtras()
    {
        // Before the mapping existed, CNTY fell through to AdifExtra. Now that
        // it is mapped, keeping a second copy would let the two drift apart.
        var qso = ParseOne(Record("<STATE:2>MN<CNTY:11>MN,Hennepin"));

        var hasExtra = qso.AdifExtra != null &&
            qso.AdifExtra.Elements.Any(e => string.Equals(e.Name, "cnty", StringComparison.OrdinalIgnoreCase));
        hasExtra.Should().BeFalse();
    }

    [Fact]
    public void AQsoWithNoCountyImportsWithoutOne()
    {
        ParseOne(Record("<STATE:2>MN")).Station!.County.Should().BeNull();
    }

    [Fact]
    public void AnImportedCountyCountsTowardTheAwardImmediately()
    {
        // The point of the whole exercise: import → resolvable → counted.
        var qso = ParseOne(Record("<STATE:2>MN<CNTY:11>MN,Hennepin"));

        var placement = CountyResolver.Resolve(qso, "MN");

        placement.Should().NotBeNull();
        placement!.Value.County.Should().Be("Hennepin");
    }

    [Fact]
    public void ExportReattachesTheStatePrefixSoTheAdifStaysConventional()
    {
        var qso = ParseOne(Record("<STATE:2>MN<CNTY:11>MN,Hennepin"));

        var adif = _service.ExportToAdif([qso]);

        adif.Should().Contain("<CNTY:11>MN,Hennepin");
    }

    [Fact]
    public void ExportOmitsCntyWhenThereIsNoStateToQualifyIt()
    {
        // CNTY without a state is ambiguous — two states can share a county
        // name — so emitting a bare one would produce an ADIF we could not
        // re-import correctly.
        var qso = ParseOne(Record("<CNTY:8>Hennepin"));
        qso.Station!.State = null;

        _service.ExportToAdif([qso]).Should().NotContain("<CNTY:");
    }

    [Fact]
    public void LegacyQsosExportTheirCountyFromTheExtras()
    {
        // QSOs imported before CNTY was mapped carry it only in AdifExtra —
        // 15,318 on the author's log. Extras export verbatim, so their county
        // still round-trips even though Station.County is empty.
        var qso = ParseOne(Record("<STATE:2>MN"));
        qso.AdifExtra = new MongoDB.Bson.BsonDocument { ["cnty"] = "MN,Hennepin" };

        _service.ExportToAdif([qso]).Should().Contain("<CNTY:11>MN,Hennepin");
    }

    [Fact]
    public void ExportNeverEmitsCntyTwice()
    {
        // A QSO can end up with both the canonical field and a legacy extras
        // copy (e.g. county edited after an old import). The canonical value
        // wins; the stale extras copy must not produce a second CNTY.
        var qso = ParseOne(Record("<STATE:2>MN"));
        qso.Station!.County = "Ramsey";
        qso.AdifExtra = new MongoDB.Bson.BsonDocument { ["cnty"] = "MN,Hennepin" };

        var adif = _service.ExportToAdif([qso]);

        adif.Should().Contain("MN,Ramsey");
        adif.Should().NotContain("Hennepin");
    }

    [Fact]
    public void CountySurvivesAFullExportImportRoundTrip()
    {
        var original = ParseOne(Record("<STATE:2>MN<CNTY:11>MN,Hennepin"));

        var reimported = _service.ParseAdif(_service.ExportToAdif([original])).Single();

        reimported.Station!.County.Should().Be(original.Station!.County);
        reimported.Station.State.Should().Be(original.Station.State);
        CountyResolver.Resolve(reimported, "MN").Should().NotBeNull("a round-tripped QSO must still count");
    }
}
