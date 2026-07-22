using FluentAssertions;
using MongoDB.Bson;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Services.Counties;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

[Trait("Category", "Unit")]
public class CountyNameNormalizerTests
{
    [Theory]
    [InlineData("MN,Hennepin", "MN", "Hennepin")]
    [InlineData("  TX , Burnet ", "TX", "Burnet")]
    [InlineData("Hennepin", null, "Hennepin")]
    [InlineData("", null, "")]
    [InlineData(null, null, "")]
    public void SplitsTheAdifStatePrefix(string? raw, string? expectedState, string expectedCounty)
    {
        var (state, county) = CountyNameNormalizer.SplitStatePrefix(raw);
        state.Should().Be(expectedState);
        county.Should().Be(expectedCounty);
    }

    [Fact]
    public void DoesNotTreatACommaInsideACountyNameAsAStatePrefix()
    {
        // Only a 2-letter prefix is a state. Anything else is part of the name.
        var (state, county) = CountyNameNormalizer.SplitStatePrefix("Saint Louis, City of");
        state.Should().BeNull();
        county.Should().Be("Saint Louis, City of");
    }

    [Theory]
    [InlineData("Los Angeles")]
    [InlineData("LOS ANGELES")]
    [InlineData("los angeles")]
    [InlineData("Los Angeles County")]
    [InlineData("  Los  Angeles  ")]
    public void CollapsesEverySpellingOfOneCountyOntoOneKey(string spelling)
    {
        CountyNameNormalizer.Normalize(spelling)
            .Should().Be(CountyNameNormalizer.Normalize("Los Angeles"));
    }

    [Theory]
    [InlineData("St. Louis", "St Louis")]
    [InlineData("Miami-Dade", "Miami Dade")]
    [InlineData("O'Brien", "OBrien")]
    [InlineData("Aleutians East Borough", "Aleutians East")]
    [InlineData("St. Martin Parish", "St Martin")]
    [InlineData("Anchorage Municipality", "Anchorage")]
    [InlineData("Aleutians West Census Area", "Aleutians West")]
    public void FoldsPunctuationAndGoverningTypeSuffixes(string a, string b)
    {
        CountyNameNormalizer.Normalize(a).Should().Be(CountyNameNormalizer.Normalize(b));
    }

    [Fact]
    public void KeepsGenuinelyDifferentCountiesApart()
    {
        CountyNameNormalizer.Normalize("Kent")
            .Should().NotBe(CountyNameNormalizer.Normalize("Kenton"));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("---")]
    public void ReturnsAnEmptyKeyForNothingUsable(string? input)
    {
        CountyNameNormalizer.Normalize(input).Should().BeEmpty();
    }
}

[Trait("Category", "Unit")]
public class CountyReferenceTests
{
    [Fact]
    public void ShipsAllFiftyStates()
    {
        CountyReference.States.Should().HaveCount(50);
    }

    [Fact]
    public void TotalIsInTheRightNeighbourhoodForUsaCa()
    {
        // Census-derived, so ~3,144 rather than MARAC's official ~3,077. The
        // point of the assertion is to catch a truncated or empty resource,
        // not to pin an exact figure.
        CountyReference.TotalCounties.Should().BeInRange(3000, 3200);
    }

    [Theory]
    [InlineData("TX", 254)]   // most counties of any state
    [InlineData("DE", 3)]     // fewest
    [InlineData("HI", 5)]
    public void CarriesTheKnownPerStateCounts(string state, int expected)
    {
        CountyReference.CountyCount(state).Should().Be(expected);
    }

    [Fact]
    public void RecognisesCountiesRegardlessOfSpelling()
    {
        CountyReference.IsKnownCounty("CA", "Los Angeles").Should().BeTrue();
        CountyReference.IsKnownCounty("CA", "LOS ANGELES COUNTY").Should().BeTrue();
        CountyReference.IsKnownCounty("AK", "Aleutians East Borough").Should().BeTrue();
    }

    [Fact]
    public void ConnecticutUsesTheHistoricalCountiesNotCensusPlanningRegions()
    {
        // The 2022 Census replaced CT's 8 counties with 9 "planning regions",
        // but ADIF CNTY, USA-CA and every logger still use the historical
        // counties. Found live: every CT county in a real log failed to match
        // until the reference was corrected.
        CountyReference.CountyCount("CT").Should().Be(8);
        CountyReference.IsKnownCounty("CT", "Hartford").Should().BeTrue();
        CountyReference.IsKnownCounty("CT", "Fairfield").Should().BeTrue();
        CountyReference.IsKnownCounty("CT", "Capitol").Should().BeFalse("planning regions are not counties");
    }

    [Fact]
    public void RejectsACountyFromTheWrongState()
    {
        // Kent exists in MI, DE and RI — but not in California.
        CountyReference.IsKnownCounty("CA", "Kent").Should().BeFalse();
        CountyReference.IsKnownCounty("MI", "Kent").Should().BeTrue();
    }

    [Theory]
    [InlineData("XX", "Anywhere")]
    [InlineData("CA", "Notacounty")]
    [InlineData(null, "Los Angeles")]
    [InlineData("CA", null)]
    public void RejectsUnknownStatesAndCounties(string? state, string? county)
    {
        CountyReference.IsKnownCounty(state, county).Should().BeFalse();
    }
}

[Trait("Category", "Unit")]
public class CountyResolverTests
{
    private static Qso UsQso(string? county = null, string? adifCnty = null, string state = "MN",
        string country = "United States", string? propMode = null)
    {
        var qso = new Qso
        {
            Id = Guid.NewGuid().ToString(),
            Callsign = "W1AW",
            QsoDate = new DateTime(2026, 7, 22),
            Band = "20m",
            Mode = "CW",
            Country = country,
            Station = new StationInfo { Country = country, State = state, County = county },
        };
        if (adifCnty != null || propMode != null)
        {
            qso.AdifExtra = new BsonDocument();
            if (adifCnty != null) qso.AdifExtra["cnty"] = adifCnty;
            if (propMode != null) qso.AdifExtra["PROP_MODE"] = propMode;
        }
        return qso;
    }

    [Fact]
    public void ReadsTheCanonicalCountyFieldWhenItIsPopulated()
    {
        var placement = CountyResolver.Resolve(UsQso(county: "Hennepin"), "MN");

        placement.Should().NotBeNull();
        placement!.Value.State.Should().Be("MN");
        placement.Value.County.Should().Be("Hennepin");
    }

    [Fact]
    public void FallsBackToTheAdifCntyWhereImportedDataActuallySits()
    {
        // The whole reason the fallback exists: ADIF import never mapped CNTY,
        // so 15,318 QSOs on the author's log carry it only in AdifExtra.
        var placement = CountyResolver.Resolve(UsQso(adifCnty: "MN,Hennepin"), "MN");

        placement.Should().NotBeNull();
        placement!.Value.County.Should().Be("Hennepin");
    }

    [Fact]
    public void PrefersTheCanonicalFieldOverTheAdifExtra()
    {
        var placement = CountyResolver.Resolve(UsQso(county: "Ramsey", adifCnty: "MN,Hennepin"), "MN");

        placement!.Value.County.Should().Be("Ramsey");
    }

    [Fact]
    public void ReportsTheReferenceSpellingSoOneCountyReadsIdenticallyEverywhere()
    {
        var a = CountyResolver.Resolve(UsQso(adifCnty: "CA,LOS ANGELES"), "CA");
        var b = CountyResolver.Resolve(UsQso(county: "Los Angeles County"), "CA");

        a!.Value.County.Should().Be(b!.Value.County);
        a.Value.County.Should().Be("Los Angeles");
    }

    [Fact]
    public void DoesNotCountAQsoWithNoStateVerdict()
    {
        // A null state is the caller saying "not an eligible US QSO".
        CountyResolver.Resolve(UsQso(adifCnty: "MN,Hennepin"), null).Should().BeNull();
    }

    [Fact]
    public void DoesNotCountACountyNameTheReferenceDoesNotRecognise()
    {
        // A typo must stay visible as "not worked", never be guessed into a
        // neighbouring county.
        CountyResolver.Resolve(UsQso(adifCnty: "MN,Hennipin"), "MN").Should().BeNull();
    }

    [Fact]
    public void DoesNotCountACountyThatBelongsToAnotherState()
    {
        // Prefix says MI, but the QSO resolved to MN. Kent is a Michigan county.
        CountyResolver.Resolve(UsQso(adifCnty: "MI,Kent", state: "MN"), "MN").Should().BeNull();
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void DoesNotCountAQsoWithNoCountyRecorded(string? county)
    {
        CountyResolver.Resolve(UsQso(county: county), "MN").Should().BeNull();
    }

    [Fact]
    public void ExcludesSatelliteQsos()
    {
        // Satellite is not a field on Qso — it is PROP_MODE=SAT in AdifExtra.
        var qso = UsQso(county: "Hennepin", propMode: "SAT");

        CountyResolver.IsSatellite(qso).Should().BeTrue();
        CountyResolver.Resolve(qso, "MN").Should().BeNull();
    }

    [Fact]
    public void DoesNotMistakeATerrestrialQsoForASatelliteOne()
    {
        var qso = UsQso(county: "Hennepin", propMode: "TROPO");

        CountyResolver.IsSatellite(qso).Should().BeFalse();
        CountyResolver.Resolve(qso, "MN").Should().NotBeNull();
    }

    [Fact]
    public void CountsQslCardAndEqslAsConfirmation()
    {
        var card = UsQso(county: "Hennepin");
        card.Qsl = new QslStatus { Rcvd = "Y" };
        CountyResolver.IsConfirmed(card).Should().BeTrue();

        var eqsl = UsQso(county: "Hennepin");
        eqsl.Qsl = new QslStatus { Eqsl = new EqslStatus { Rcvd = "Y" } };
        CountyResolver.IsConfirmed(eqsl).Should().BeTrue();
    }

    [Fact]
    public void DoesNotCountLotwAsACountyConfirmation()
    {
        // LoTW does not reliably carry CNTY, so it cannot vouch for the county
        // this QSO claims. Worked, yes; confirmed, no.
        var qso = UsQso(county: "Hennepin");
        qso.Qsl = new QslStatus { Lotw = new LotwStatus { Rcvd = "Y" } };

        CountyResolver.IsConfirmed(qso).Should().BeFalse();
        CountyResolver.Resolve(qso, "MN").Should().NotBeNull();
    }

    [Fact]
    public void TreatsAMissingQslBlockAsUnconfirmed()
    {
        CountyResolver.IsConfirmed(UsQso(county: "Hennepin")).Should().BeFalse();
    }
}
