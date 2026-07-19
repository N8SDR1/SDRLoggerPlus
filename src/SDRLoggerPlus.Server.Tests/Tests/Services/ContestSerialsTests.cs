using FluentAssertions;
using SDRLoggerPlus.Contracts.Models.Contesting;
using SDRLoggerPlus.Server.Services.Contesting;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

[Trait("Category", "Unit")]
public class ContestSerialsTests
{
    [Fact]
    public void AllBand_IncrementsAcrossBands()
    {
        var s = new ContestSession();

        ContestSerials.Allocate(s, SerialMode.AllBand, "20M").Should().Be(1);
        ContestSerials.Allocate(s, SerialMode.AllBand, "40M").Should().Be(2); // shared across bands
        ContestSerials.Peek(s, SerialMode.AllBand, "15M").Should().Be(3);
    }

    [Fact]
    public void PerBand_TracksEachBandIndependently()
    {
        var s = new ContestSession();

        ContestSerials.Allocate(s, SerialMode.PerBand, "20M").Should().Be(1);
        ContestSerials.Allocate(s, SerialMode.PerBand, "20M").Should().Be(2);
        ContestSerials.Allocate(s, SerialMode.PerBand, "40M").Should().Be(1); // independent
        ContestSerials.Peek(s, SerialMode.PerBand, "20M").Should().Be(3);
    }

    [Fact]
    public void None_AllocatesZeroAndDoesNotAdvance()
    {
        var s = new ContestSession();

        ContestSerials.Allocate(s, SerialMode.None, "20M").Should().Be(0);
        s.NextSerialAllBand.Should().Be(1); // untouched
        s.NextSerialPerBand.Should().BeEmpty();
    }

    [Theory]
    [InlineData(1, "001")]
    [InlineData(5, "005")]
    [InlineData(42, "042")]
    [InlineData(1234, "1234")]
    public void Format_ZeroPadsToThreeDigits(int serial, string expected)
    {
        ContestSerials.Format(serial).Should().Be(expected);
    }
}
