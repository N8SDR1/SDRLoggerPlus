using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Services;
using SDRLoggerPlus.Server.Services.Backup;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

[Trait("Category", "Unit")]
public class BackupServiceTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "sdrloggerplus-bks-" + Guid.NewGuid());
    private readonly Mock<ISettingsService> _settings = new();
    private readonly Mock<IAdifService> _adif = new();
    private readonly UserSettings _userSettings = new();

    public BackupServiceTests()
    {
        Directory.CreateDirectory(_root);
        _settings.Setup(s => s.GetSettingsAsync(It.IsAny<string>())).ReturnsAsync(_userSettings);
        _adif.Setup(a => a.ExportQsosAsync(null)).ReturnsAsync("<EOH>\n<EOR>");
    }

    public void Dispose() => Directory.Delete(_root, recursive: true);

    private BackupService CreateService() => new(
        _settings.Object, _adif.Object,
        configDir: _root, liteDbPath: Path.Combine(_root, "sdrloggerplus.db"),
        NullLoggerFactory.Instance);

    [Fact]
    public async Task GetStatus_Defaults_DisabledDailyDefaultDest()
    {
        var status = await CreateService().GetStatusAsync();
        status.Enabled.Should().BeFalse();
        status.Interval.Should().Be("daily");
        status.Destination.Should().Be(Path.Combine(_root, "backups"));
        status.LastRunUtc.Should().BeNull();
    }

    [Fact]
    public async Task RunNow_WritesBackup_AndStatusReflectsIt()
    {
        File.WriteAllText(Path.Combine(_root, "sdrloggerplus.db"), "X");
        var svc = CreateService();

        var result = await svc.RunNowAsync();

        result.Ok.Should().BeTrue();
        var status = await svc.GetStatusAsync();
        status.LastRunUtc.Should().NotBeNull();
        status.Ok.Should().BeTrue();
        status.NextDueUtc.Should().NotBeNull(); // daily → lastRun+1d
    }

    [Fact]
    public async Task TickAsync_Disabled_DoesNotRun()
    {
        _userSettings.Backup.Enabled = false;
        var svc = CreateService();
        await svc.TickAsync();
        (await svc.GetStatusAsync()).LastRunUtc.Should().BeNull();
    }

    [Fact]
    public async Task TickAsync_EnabledNoHistory_FiresImmediately()
    {
        File.WriteAllText(Path.Combine(_root, "sdrloggerplus.db"), "X");
        _userSettings.Backup.Enabled = true;
        var svc = CreateService();
        await svc.TickAsync();
        (await svc.GetStatusAsync()).LastRunUtc.Should().NotBeNull();
    }

    [Fact]
    public async Task TickAsync_OnExitInterval_NeverFiresFromTimer()
    {
        _userSettings.Backup.Enabled = true;
        _userSettings.Backup.Interval = "on_exit";
        var svc = CreateService();
        await svc.TickAsync();
        (await svc.GetStatusAsync()).LastRunUtc.Should().BeNull();
    }

    [Fact]
    public async Task CustomDestination_IsUsed()
    {
        File.WriteAllText(Path.Combine(_root, "sdrloggerplus.db"), "X");
        var custom = Path.Combine(_root, "elsewhere");
        _userSettings.Backup.DestinationPath = custom;
        var result = await CreateService().RunNowAsync();
        result.Path.Should().StartWith(custom);
    }

    [Fact]
    public void Dispose_Twice_DoesNotThrow()
    {
        // The instance is registered both as a singleton and as a hosted
        // service (Program.cs), so the host AND the DI container each dispose
        // it on graceful shutdown. The second call must be a no-op, not
        // _cts.Cancel() on a disposed CancellationTokenSource.
        var service = CreateService();
        service.Dispose();
        var second = () => service.Dispose();
        second.Should().NotThrow();
    }
}
