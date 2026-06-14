using FluentAssertions;
using SDRLoggerPlus.Server.Services;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

[Trait("Category", "Unit")]
public class UsStateResolverTests
{
    [Fact]
    public void Resolve_StateCode_WithUsCountry_ReturnsCode()
        => UsStateResolver.Resolve("OH", null, "United States").Should().Be("OH");

    [Fact]
    public void Resolve_FullStateName_ReturnsCode()
        => UsStateResolver.Resolve("Ohio", null, "United States").Should().Be("OH");

    [Fact]
    public void Resolve_StateFromQthToken_ReturnsCode()
        => UsStateResolver.Resolve(null, "Columbus, OH", "United States").Should().Be("OH");

    [Fact]
    public void Resolve_StateFromQthFullName_ReturnsCode()
        => UsStateResolver.Resolve(null, "Albany, New York", "United States").Should().Be("NY");

    [Fact]
    public void Resolve_QthState_NonUsCountry_ReturnsNull()
        => UsStateResolver.Resolve(null, "TX", "Japan").Should().BeNull();

    [Fact]
    public void Resolve_StateField_NonUsCountry_ReturnsNull()
        => UsStateResolver.Resolve("TX", null, "Japan").Should().BeNull();

    [Fact]
    public void Resolve_AlaskaEntity_NoState_ReturnsAK()
        => UsStateResolver.Resolve(null, null, "Alaska").Should().Be("AK");

    [Fact]
    public void Resolve_HawaiiEntity_NoState_ReturnsHI()
        => UsStateResolver.Resolve(null, null, "Hawaii").Should().Be("HI");

    [Fact]
    public void Resolve_UsCountry_NothingResolvable_ReturnsNull()
        => UsStateResolver.Resolve(null, "somewhere nice", "United States").Should().BeNull();

    [Fact]
    public void Resolve_InvalidStateCode_ReturnsNull()
        => UsStateResolver.Resolve("ZZ", null, "United States").Should().BeNull();

    [Fact]
    public void Resolve_NullCountry_ReturnsNull()
        => UsStateResolver.Resolve("OH", null, null).Should().BeNull();

    [Fact]
    public void Resolve_CaseInsensitive()
        => UsStateResolver.Resolve("oh", null, "UNITED STATES").Should().Be("OH");
}
