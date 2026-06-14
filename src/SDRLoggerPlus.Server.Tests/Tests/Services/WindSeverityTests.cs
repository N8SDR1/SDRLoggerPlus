using FluentAssertions;
using SDRLoggerPlus.Server.Services.Weather;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

[Trait("Category", "Unit")]
public class WindSeverityTests
{
    // Default SDRLogger+ thresholds: sustained 30 mph, gust 45 mph
    private static string Classify(double? sust, double? gust, string? nws = null, bool extreme = false)
        => WindSeverity.Classify(sust, gust, nws, extreme, 30, 45);

    [Fact]
    public void CalmConditions_NoSeverity()
        => Classify(5, 10).Should().Be(WindSeverity.None);

    [Theory]
    [InlineData(20.0, null)]   // sustained >= max(10, 30-10) = 20
    [InlineData(null, 35.0)]   // gust >= max(15, 45-10) = 35
    public void NearThreshold_Elevated(double? sust, double? gust)
        => Classify(sust, gust).Should().Be(WindSeverity.Elevated);

    [Theory]
    [InlineData(30.0, null)]
    [InlineData(null, 45.0)]
    [InlineData(35.0, 20.0)]
    public void AtThreshold_High(double? sust, double? gust)
        => Classify(sust, gust).Should().Be(WindSeverity.High);

    [Theory]
    [InlineData(45.0, null)]   // sustained >= 30+15
    [InlineData(null, 60.0)]   // gust >= 45+15
    public void WellOverThreshold_Extreme(double? sust, double? gust)
        => Classify(sust, gust).Should().Be(WindSeverity.Extreme);

    [Fact]
    public void NwsExtremeFlag_AlwaysExtreme()
        => Classify(0, 0, "High Wind Warning", extreme: true).Should().Be(WindSeverity.Extreme);

    [Fact]
    public void NwsAdvisoryWithCalmReadings_High()
        => Classify(5, 10, "Wind Advisory").Should().Be(WindSeverity.High);

    [Fact]
    public void NullReadingsNoNws_None()
        => Classify(null, null).Should().Be(WindSeverity.None);

    [Fact]
    public void LowThresholds_FloorsApply()
        // thresh 15/20 → elevated floor is max(10, 5)=10 sustained, max(15, 10)=15 gust
        => WindSeverity.Classify(12, null, null, false, 15, 20).Should().Be(WindSeverity.Elevated);
}
