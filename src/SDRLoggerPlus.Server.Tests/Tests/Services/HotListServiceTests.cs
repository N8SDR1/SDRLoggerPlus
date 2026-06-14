using FluentAssertions;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using SDRLoggerPlus.Contracts.Events;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Hubs;
using SDRLoggerPlus.Server.Services;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

[Trait("Category", "Unit")]
public class HotListServiceTests
{
    private readonly Mock<ISettingsService> _settings = new();
    private readonly Mock<IHubContext<LogHub, ILogHubClient>> _hub = new();
    private readonly Mock<ILogHubClient> _hubClient = new();
    private readonly UserSettings _userSettings = new();
    private readonly HotListService _service;

    public HotListServiceTests()
    {
        _settings.Setup(s => s.GetSettingsAsync(It.IsAny<string>())).ReturnsAsync(_userSettings);
        _settings.Setup(s => s.SaveSettingsAsync(It.IsAny<UserSettings>()))
            .ReturnsAsync((UserSettings u) => u);
        _hub.Setup(h => h.Clients.All).Returns(_hubClient.Object);
        _hubClient.Setup(c => c.OnHotListChanged(It.IsAny<HotListChangedEvent>()))
            .Returns(Task.CompletedTask);

        var services = new ServiceCollection();
        services.AddSingleton(_settings.Object);
        var provider = services.BuildServiceProvider();

        _service = new HotListService(provider, _hub.Object, NullLogger<HotListService>.Instance);
    }

    [Fact]
    public async Task IsHot_EnabledAndListed_True()
    {
        _userSettings.HotList.Enabled = true;
        _userSettings.HotList.Callsigns = ["K5P", "VP8XYZ"];
        await _service.ReloadAsync();

        _service.IsHot("K5P").Should().BeTrue();
        _service.IsHot("k5p").Should().BeTrue();      // case-insensitive
        _service.IsHot(" K5P ").Should().BeTrue();    // trimmed
        _service.IsHot("W1AW").Should().BeFalse();
    }

    [Fact]
    public async Task IsHot_Disabled_AlwaysFalse()
    {
        _userSettings.HotList.Enabled = false;
        _userSettings.HotList.Callsigns = ["K5P"];
        await _service.ReloadAsync();

        _service.IsHot("K5P").Should().BeFalse();
    }

    [Fact]
    public void IsHot_NullOrEmpty_False()
    {
        _service.IsHot(null).Should().BeFalse();
        _service.IsHot("").Should().BeFalse();
    }

    [Fact]
    public async Task Add_PersistsUppercasedDeduped_AndBroadcasts()
    {
        _userSettings.HotList.Enabled = true;
        await _service.AddAsync(["k5p", "K5P", " vp8xyz ", ""]);

        _userSettings.HotList.Callsigns.Should().BeEquivalentTo("K5P", "VP8XYZ");
        _settings.Verify(s => s.SaveSettingsAsync(It.IsAny<UserSettings>()), Times.Once);
        _hubClient.Verify(c => c.OnHotListChanged(It.IsAny<HotListChangedEvent>()), Times.Once);
        _service.IsHot("K5P").Should().BeTrue();      // in-memory set reloaded
    }

    [Fact]
    public async Task Remove_TakesCallOut()
    {
        _userSettings.HotList.Enabled = true;
        _userSettings.HotList.Callsigns = ["K5P", "VP8XYZ"];
        await _service.ReloadAsync();

        await _service.RemoveAsync("k5p");

        _userSettings.HotList.Callsigns.Should().BeEquivalentTo("VP8XYZ");
        _service.IsHot("K5P").Should().BeFalse();
    }

    [Fact]
    public async Task Clear_EmptiesList()
    {
        _userSettings.HotList.Callsigns = ["K5P", "VP8XYZ"];
        await _service.ClearAsync();
        _userSettings.HotList.Callsigns.Should().BeEmpty();
    }

    [Fact]
    public async Task SetFlags_UpdatesOnlyProvided()
    {
        await _service.SetFlagsAsync(enabled: true, ttsEnabled: null);
        _userSettings.HotList.Enabled.Should().BeTrue();
        _userSettings.HotList.TtsEnabled.Should().BeFalse();

        await _service.SetFlagsAsync(enabled: null, ttsEnabled: true);
        _userSettings.HotList.Enabled.Should().BeTrue();
        _userSettings.HotList.TtsEnabled.Should().BeTrue();
    }
}
