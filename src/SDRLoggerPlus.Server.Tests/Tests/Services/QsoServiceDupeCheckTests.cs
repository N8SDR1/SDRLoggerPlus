using FluentAssertions;
using Microsoft.AspNetCore.SignalR;
using Moq;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database;
using SDRLoggerPlus.Server.Hubs;
using SDRLoggerPlus.Server.Services;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

[Trait("Category", "Unit")]
public class QsoServiceDupeCheckTests
{
    private readonly Mock<IQsoRepository> _repository = new();
    private readonly QsoService _service;

    public QsoServiceDupeCheckTests()
    {
        var hub = new Mock<IHubContext<LogHub, ILogHubClient>>();
        _service = new QsoService(_repository.Object, hub.Object);
    }

    private static Qso MakeQso() => new()
    {
        Id = "abc123",
        Callsign = "K1ABC",
        Band = "20m",
        Mode = "FT8",
        QsoDate = DateTime.UtcNow.Date,
        TimeOn = "0142",
        CreatedAt = DateTime.UtcNow.AddMinutes(-12),
    };

    [Fact]
    public async Task CheckRecentDupe_PassesDupeWindowToRepository()
    {
        DateTime? seenSince = null;
        _repository.Setup(r => r.FindRecentDuplicateAsync("K1ABC", "20m", "FT8", It.IsAny<DateTime>()))
            .Callback<string, string, string, DateTime>((_, _, _, since) => seenSince = since)
            .ReturnsAsync((Qso?)null);

        await _service.CheckRecentDupeAsync("K1ABC", "20m", "FT8");

        seenSince.Should().NotBeNull();
        seenSince!.Value.Should().BeCloseTo(
            DateTime.UtcNow - QsoService.DupeWindow, TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task CheckRecentDupe_MapsMatchToResponse()
    {
        _repository.Setup(r => r.FindRecentDuplicateAsync("K1ABC", "20m", "FT8", It.IsAny<DateTime>()))
            .ReturnsAsync(MakeQso());

        var result = await _service.CheckRecentDupeAsync("K1ABC", "20m", "FT8");

        result.Should().NotBeNull();
        result!.Callsign.Should().Be("K1ABC");
        result.Band.Should().Be("20m");
        result.CreatedAt.Should().BeCloseTo(DateTime.UtcNow.AddMinutes(-12), TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task CheckRecentDupe_NoMatch_ReturnsNull()
    {
        _repository.Setup(r => r.FindRecentDuplicateAsync(It.IsAny<string>(), It.IsAny<string>(),
                It.IsAny<string>(), It.IsAny<DateTime>()))
            .ReturnsAsync((Qso?)null);

        (await _service.CheckRecentDupeAsync("K1ABC", "20m", "FT8")).Should().BeNull();
    }

    [Theory]
    [InlineData("", "20m", "FT8")]
    [InlineData("K1ABC", "", "FT8")]
    [InlineData("K1ABC", "20m", "")]
    [InlineData("   ", "20m", "FT8")]
    public async Task CheckRecentDupe_BlankInputs_ReturnNullWithoutQuerying(
        string callsign, string band, string mode)
    {
        (await _service.CheckRecentDupeAsync(callsign, band, mode)).Should().BeNull();

        _repository.Verify(r => r.FindRecentDuplicateAsync(It.IsAny<string>(), It.IsAny<string>(),
            It.IsAny<string>(), It.IsAny<DateTime>()), Times.Never);
    }
}
