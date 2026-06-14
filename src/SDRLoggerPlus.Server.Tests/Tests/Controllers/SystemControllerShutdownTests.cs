using FluentAssertions;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Moq;
using SDRLoggerPlus.Server.Controllers;
using SDRLoggerPlus.Server.Core.Database;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Controllers;

/// <summary>
/// The shutdown endpoint exists so the Electron shell can stop the backend
/// gracefully (reaching BackupService.StopAsync's on-exit backup) instead of
/// TerminateProcess-ing it on Windows. It must be inert unless the launcher
/// configured a token, and must reject anything but an exact token match —
/// otherwise any local webpage could POST a shutdown to a dev instance.
/// </summary>
[Trait("Category", "Unit")]
public class SystemControllerShutdownTests
{
    private readonly Mock<IHostApplicationLifetime> _lifetime = new();

    private SystemController CreateController(string? configuredToken)
    {
        var values = new Dictionary<string, string?>();
        if (configuredToken != null)
            values["SDRLOGGERPLUS_SHUTDOWN_TOKEN"] = configuredToken;
        var config = new ConfigurationBuilder().AddInMemoryCollection(values).Build();
        return new SystemController(
            Mock.Of<IDbContext>(), config, _lifetime.Object);
    }

    [Fact]
    public void Shutdown_NoTokenConfigured_Returns404AndDoesNotStop()
    {
        var result = CreateController(null).Shutdown("anything");

        result.Should().BeOfType<NotFoundResult>();
        _lifetime.Verify(l => l.StopApplication(), Times.Never);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("wrong-token")]
    [InlineData("secret-token-extra")]
    public void Shutdown_TokenMismatch_Returns401AndDoesNotStop(string? presented)
    {
        var result = CreateController("secret-token").Shutdown(presented);

        result.Should().BeOfType<UnauthorizedResult>();
        _lifetime.Verify(l => l.StopApplication(), Times.Never);
    }

    [Fact]
    public void Shutdown_CorrectToken_Returns202AndStopsApplication()
    {
        var result = CreateController("secret-token").Shutdown("secret-token");

        result.Should().BeOfType<AcceptedResult>();
        _lifetime.Verify(l => l.StopApplication(), Times.Once);
    }
}
