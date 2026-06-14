using FluentAssertions;
using SDRLoggerPlus.Server.Services.Sat;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

[Trait("Category", "Unit")]
public class SatMessageParserTests
{
    [Fact]
    public void Parse_Boot()
        => SatMessageParser.Parse("SAT,BOOT,SN1234,2.45").Should()
            .Be(new SatBoot("SN1234", "2.45"));

    [Fact]
    public void Parse_StartTrack()
        => SatMessageParser.Parse("SAT,START TRACK,SO-50,27607").Should()
            .Be(new SatStartTrack("SO-50", "27607"));

    [Fact]
    public void Parse_AosLos()
    {
        SatMessageParser.Parse("SAT,AOS,212.5").Should().Be(new SatAos("212.5"));
        SatMessageParser.Parse("SAT,LOS,33.0").Should().Be(new SatLos("33.0"));
    }

    [Fact]
    public void Parse_Transponder()
        => SatMessageParser.Parse("SAT,TRANSPONDER,Mode V/U,145950000,FM,436795000,FM").Should()
            .Be(new SatTransponder("Mode V/U", "145950000", "FM", "436795000", "FM"));

    [Fact]
    public void Parse_Qso_FullMessage()
    {
        var msg = SatMessageParser.Parse(
            "SAT,QSO,SO-50,k5p,EM12,fm,nice pass,59,57,145850000,436795000,Bob");

        var qso = msg.Should().BeOfType<SatQso>().Subject;
        qso.SatName.Should().Be("SO-50");
        qso.Callsign.Should().Be("K5P");          // uppercased
        qso.Grid.Should().Be("EM12");
        qso.Mode.Should().Be("FM");
        qso.RstSent.Should().Be("59");
        qso.RstReceived.Should().Be("57");
        qso.UplinkHz.Should().Be("145850000");
        qso.DownlinkHz.Should().Be("436795000");
        qso.Name.Should().Be("Bob");
    }

    [Fact]
    public void Parse_Stop()
        => SatMessageParser.Parse("SAT,STOP").Should().Be(new SatStop());

    [Theory]
    [InlineData("")]
    [InlineData("garbage")]
    [InlineData("SAT")]
    [InlineData("SAT,BOOT")]            // too few fields
    [InlineData("SAT,UNKNOWN,1,2,3")]
    [InlineData("XYZ,QSO,a,b,c,d,e,f,g,h,i,j")]
    public void Parse_BadFrames_ReturnNull(string text)
        => SatMessageParser.Parse(text).Should().BeNull();
}

[Trait("Category", "Unit")]
public class SatGeometryTests
{
    [Fact]
    public void FootprintRadius_So50Case_MatchesControllerDiameter()
    {
        // Documented SDRLogger+ v1.10 validation: SO-50 at 605.9 km altitude →
        // radius ≈ 2 679 km, i.e. half the controller's reported 5 352.8 km diameter
        var radius = SatGeometry.FootprintRadiusKm(605.9);
        radius.Should().BeApproximately(5352.8 / 2, 15.0);
    }

    [Fact]
    public void FootprintRadius_ZeroOrNegativeAltitude_Zero()
    {
        SatGeometry.FootprintRadiusKm(0).Should().Be(0);
        SatGeometry.FootprintRadiusKm(-5).Should().Be(0);
    }

    [Fact]
    public void Subpoint_StraightUp_IsStationLocation()
    {
        // Satellite directly overhead: subpoint = station, altitude = range
        var result = SatGeometry.SubpointFromLook(40.0, -84.0, 0, 90, 500)!.Value;
        result.Lat.Should().BeApproximately(40.0, 0.01);
        result.Lon.Should().BeApproximately(-84.0, 0.01);
        result.AltKm.Should().BeApproximately(500, 1);
    }

    [Fact]
    public void Subpoint_NorthHorizon_MovesNorth()
    {
        // Low elevation looking due north → subpoint is north of station
        var result = SatGeometry.SubpointFromLook(40.0, -84.0, 0, 10, 1500)!.Value;
        result.Lat.Should().BeGreaterThan(40.0);
        result.Lon.Should().BeApproximately(-84.0, 1.0);
        result.AltKm.Should().BeGreaterThan(0);
    }

    [Fact]
    public void Subpoint_ZeroRange_Null()
        => SatGeometry.SubpointFromLook(40, -84, 0, 45, 0).Should().BeNull();
}
