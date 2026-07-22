using System.Net;
using FluentAssertions;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Moq.Protected;
using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Controllers;
using SDRLoggerPlus.Server.Core.Database;
using SDRLoggerPlus.Server.Core.Database.LiteDb;
using SDRLoggerPlus.Server.Hubs;
using SDRLoggerPlus.Server.Services;
using SDRLoggerPlus.Server.Services.Qsl;
using SDRLoggerPlus.Server.Tests.Fixtures;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services.Qsl;

internal static class QslTestDoubles
{
    public static HttpClient RespondingWith(HttpStatusCode status, string body)
    {
        var handler = new Mock<HttpMessageHandler>();
        handler.Protected()
            .Setup<Task<HttpResponseMessage>>("SendAsync",
                ItExpr.IsAny<HttpRequestMessage>(), ItExpr.IsAny<CancellationToken>())
            .ReturnsAsync(new HttpResponseMessage(status) { Content = new StringContent(body) });
        return new HttpClient(handler.Object);
    }

    public static ISettingsService EqslSettings()
    {
        var settings = new UserSettings
        {
            Station = { Callsign = "N9BC" },
            Eqsl = { Enabled = true, Username = "N9BC", Password = "pw" },
        };
        var mock = new Mock<ISettingsService>();
        mock.Setup(s => s.GetSettingsAsync(It.IsAny<string>())).ReturnsAsync(settings);
        return mock.Object;
    }
}

/// <summary>
/// eQSL failure classification. PostAsync does not throw on 5xx, so without
/// an explicit status check an eQSL outage fell through the body sniffing to
/// Rejected — permanently needs-attention for QSOs logged during an outage.
/// </summary>
[Trait("Category", "Unit")]
public class EqslFailureClassificationTests
{
    private static Task<QslUploadResult> Upload(HttpStatusCode status, string body) =>
        new EqslService(QslTestDoubles.EqslSettings(),
                QslTestDoubles.RespondingWith(status, body),
                NullLogger<EqslService>.Instance)
            .UploadQsoAsync(new Qso
            {
                Id = "x",
                Callsign = "W1AW",
                Band = "20m",
                Mode = "FT8",
                QsoDate = DateTime.UtcNow.Date,
                TimeOn = "1200",
            });

    [Theory]
    [InlineData(HttpStatusCode.InternalServerError)]
    [InlineData(HttpStatusCode.ServiceUnavailable)]
    [InlineData(HttpStatusCode.TooManyRequests)]
    public async Task ServerFaultsAreTemporary(HttpStatusCode status)
    {
        var result = await Upload(status, "<html>maintenance</html>");

        result.Ok.Should().BeFalse();
        result.Kind.Should().Be(QslFailureKind.Temporary,
            "an eQSL outage must leave the QSO retryable, not permanently needs-attention");
    }

    [Fact]
    public async Task OtherClientErrorsAreRejected()
    {
        (await Upload(HttpStatusCode.BadRequest, "nope")).Kind.Should().Be(QslFailureKind.Rejected);
    }

    [Fact]
    public async Task A200WithSuccessBodyStillSucceeds()
    {
        (await Upload(HttpStatusCode.OK, "Result: 1 out of 1 records added")).Ok.Should().BeTrue();
    }

    [Fact]
    public async Task A200WithBadCredentialsBodyIsStillAuth()
    {
        (await Upload(HttpStatusCode.OK, "Error: Bad Callsign/Password"))
            .Kind.Should().Be(QslFailureKind.Auth);
    }
}

/// <summary>
/// Saving corrected Club Log credentials must clear the persisted one-strike
/// block. The block survives restarts by design, so if a settings save cannot
/// clear it, an operator who fixes their password but never presses "Test"
/// stays blocked forever — while the log claims re-saving will fix it.
/// </summary>
[Trait("Category", "Unit")]
public class SettingsSaveClearsClubLogBlockTests : IDisposable
{
    private readonly string _dir;
    private readonly QslBlockStateStore _store;

    public SettingsSaveClearsClubLogBlockTests()
    {
        _dir = Path.Combine(Path.GetTempPath(), $"slp_reset_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_dir);
        _store = new QslBlockStateStore(Path.Combine(_dir, "qsl-block-state.json"));
    }

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { }
    }

    private static UserSettings Settings(string clubLogPassword) => new()
    {
        Station = { Callsign = "N9BC" },
        ClubLog = { Enabled = true, ApiKey = "key", Email = "a@b.c", Password = clubLogPassword },
    };

    private async Task<(ClubLogService ClubLog, SettingsController Controller, Mock<ISettingsService> Service)>
        BlockedSetup(string existingPassword)
    {
        var clubLog = new ClubLogService(
            new Mock<ISettingsService>().Object,
            QslTestDoubles.RespondingWith(HttpStatusCode.OK, "OK"),
            NullLogger<ClubLogService>.Instance,
            _store);

        // Trip the block the way production does.
        _store.Save(new QslBlockState { ClubLogBlockedUtc = DateTime.UtcNow, ClubLogReason = "HTTP 403" });
        var rearmed = new ClubLogService(
            new Mock<ISettingsService>().Object,
            QslTestDoubles.RespondingWith(HttpStatusCode.OK, "OK"),
            NullLogger<ClubLogService>.Instance,
            _store);
        rearmed.IsBlocked.Should().BeTrue("precondition: the persisted block must load");

        var settingsService = new Mock<ISettingsService>();
        settingsService.Setup(s => s.GetSettingsAsync(It.IsAny<string>()))
            .ReturnsAsync(Settings(existingPassword));
        settingsService.Setup(s => s.SaveSettingsAsync(It.IsAny<UserSettings>()))
            .ReturnsAsync((UserSettings s) => s);

        var controller = new SettingsController(
            settingsService.Object,
            new Mock<IHotListService>().Object,
            NullLogger<SettingsController>.Instance,
            rearmed);

        await Task.CompletedTask;
        return (rearmed, controller, settingsService);
    }

    [Fact]
    public async Task SavingChangedCredentialsClearsTheBlock()
    {
        var (clubLog, controller, _) = await BlockedSetup(existingPassword: "old-pw");

        await controller.SaveSettings(Settings("new-pw"));

        clubLog.IsBlocked.Should().BeFalse("corrected credentials must re-arm uploads");
        _store.Load().ClubLogBlockedUtc.Should().BeNull("the persisted state must clear too");
    }

    [Fact]
    public async Task SavingUnrelatedSettingsLeavesTheBlockInPlace()
    {
        // Re-saving while still blocked with the SAME credentials must not
        // re-arm uploads against a password Club Log already rejected —
        // repetition is what triggers its IP firewall.
        var (clubLog, controller, _) = await BlockedSetup(existingPassword: "same-pw");

        var unrelated = Settings("same-pw");
        unrelated.Station.Callsign = "N9BC/P";
        await controller.SaveSettings(unrelated);

        clubLog.IsBlocked.Should().BeTrue();
        _store.Load().ClubLogBlockedUtc.Should().NotBeNull();
    }

    [Fact]
    public async Task WorksWithoutAClubLogService()
    {
        var settingsService = new Mock<ISettingsService>();
        settingsService.Setup(s => s.GetSettingsAsync(It.IsAny<string>())).ReturnsAsync(new UserSettings());
        settingsService.Setup(s => s.SaveSettingsAsync(It.IsAny<UserSettings>()))
            .ReturnsAsync((UserSettings s) => s);
        var controller = new SettingsController(
            settingsService.Object, new Mock<IHotListService>().Object,
            NullLogger<SettingsController>.Instance);

        var result = await controller.SaveSettings(new UserSettings());

        result.Result.Should().BeOfType<Microsoft.AspNetCore.Mvc.OkObjectResult>();
    }
}

/// <summary>
/// Pins that the REAL container wiring fills QsoService's optional
/// scope-factory parameter. Production registers it as
/// AddScoped&lt;IQsoService, QsoService&gt; and relies on MS DI resolving the
/// optional constructor arguments — if that assumption broke, uploads would
/// still fire but nothing would ever be recorded, silently.
/// </summary>
[Trait("Category", "Integration")]
[Collection("LiteDbMapper")]
public class QsoServiceContainerWiringTests : IDisposable
{
    private readonly LiteDbTestFixture _fixture = new();

    public void Dispose() => _fixture.Dispose();

    [Fact]
    public async Task ContainerResolvedQsoServiceRecordsToTheLedger()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddScoped<IQsoRepository>(_ => new LiteQsoRepository(_fixture.Context));
        services.AddScoped<QslSyncRecorder>();
        services.AddSingleton(new ClubLogService(
            ClubLogSettings(), QslTestDoubles.RespondingWith(HttpStatusCode.OK, "OK"),
            NullLogger<ClubLogService>.Instance));
        services.AddSingleton(Hub());
        // The registration under test — identical shape to Program.cs.
        services.AddScoped<IQsoService, QsoService>();

        using var provider = services.BuildServiceProvider();
        using var scope = provider.CreateScope();
        var qsoService = scope.ServiceProvider.GetRequiredService<IQsoService>();

        var created = await qsoService.CreateAsync(
            new CreateQsoRequest("W1AW", DateTime.UtcNow.Date, "1200", "20m", "FT8", Frequency: 14075));

        var repo = new LiteQsoRepository(_fixture.Context);
        QslServiceSync? entry = null;
        for (var i = 0; i < 100 && entry == null; i++)
        {
            entry = (await repo.GetByIdAsync(created.Id))?.QslSync?.For(QslSyncLedger.ClubLogKey);
            if (entry == null) await Task.Delay(20);
        }

        entry.Should().NotBeNull(
            "the container must inject the optional scope factory, or results are silently dropped");
        entry!.Status.Should().Be(SyncStatus.Synced);
    }

    private static ISettingsService ClubLogSettings()
    {
        var settings = new UserSettings
        {
            Station = { Callsign = "N9BC" },
            ClubLog = { Enabled = true, ApiKey = "key", Email = "a@b.c", Password = "pw" },
        };
        var mock = new Mock<ISettingsService>();
        mock.Setup(s => s.GetSettingsAsync(It.IsAny<string>())).ReturnsAsync(settings);
        return mock.Object;
    }

    private static IHubContext<LogHub, ILogHubClient> Hub()
    {
        var clients = new Mock<IHubClients<ILogHubClient>>();
        clients.Setup(c => c.All).Returns(new Mock<ILogHubClient>().Object);
        var hub = new Mock<IHubContext<LogHub, ILogHubClient>>();
        hub.Setup(h => h.Clients).Returns(clients.Object);
        return hub.Object;
    }
}
