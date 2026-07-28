using FluentAssertions;
using SDRLoggerPlus.Server.Services;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

/// <summary>
/// QRZ's lotw/eqsl/mqsl callbook fields are a genuine three-state — "1" (will
/// accept), "0" (won't), or blank/missing (the operator never said). Issue #39:
/// collapsing blank to false would misreport an unknown operator as a refusal.
/// </summary>
[Trait("Category", "Unit")]
public class QrzServiceParseTriStateTests
{
    [Fact]
    public void OneMeansTrue() => QrzService.ParseTriState("1").Should().BeTrue();

    [Fact]
    public void ZeroMeansFalse() => QrzService.ParseTriState("0").Should().BeFalse();

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("  ")]
    public void MissingOrBlankMeansUnknown_NotFalse(string? value)
    {
        QrzService.ParseTriState(value).Should().BeNull(
            "an unanswered field is not the same as a refusal");
    }

    [Fact]
    public void AnUnexpectedValueMeansUnknown_NeverGuesses()
    {
        QrzService.ParseTriState("Y").Should().BeNull();
    }
}
