using FluentAssertions;
using SDRLoggerPlus.Server.Services;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

[Trait("Category", "Unit")]
public class WpxPrefixExtractorTests
{
    [Theory]
    [InlineData("N8ABC", "N8")]
    [InlineData("3DA0XYZ", "3DA0")]
    [InlineData("K5P", "K5")]
    [InlineData("W1AW/P", "W1")]      // portable suffix stripped
    [InlineData("N8SDR/QRP", "N8")]
    [InlineData("VK9/N8SDR", "VK9")]  // stroke call: shorter part wins
    [InlineData("N8SDR/VK9", "VK9")]
    [InlineData("F/ON4UN/P", "F0")]   // multi-slash → first part; no digit → letter + "0"
    [InlineData("RAEM", "R0")]        // no digit at all
    [InlineData("KH6ABC", "KH6")]
    [InlineData("kh6abc", "KH6")]     // lowercase input normalized
    public void Extract_ReturnsWpxPrefix(string call, string expected)
        => WpxPrefixExtractor.Extract(call).Should().Be(expected);

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void Extract_EmptyInput_ReturnsNull(string call)
        => WpxPrefixExtractor.Extract(call).Should().BeNull();
}
