using System.Net;
using FluentAssertions;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Moq.Protected;
using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database;
using SDRLoggerPlus.Server.Core.Database.LiteDb;
using SDRLoggerPlus.Server.Hubs;
using SDRLoggerPlus.Server.Services;
using SDRLoggerPlus.Server.Services.Qsl;
using SDRLoggerPlus.Server.Tests.Fixtures;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services.Qsl;

/// <summary>
/// Covers the seam the unit tests cannot: QsoService fires QSL uploads on a
/// background task that OUTLIVES the HTTP request, so it must resolve its own
/// DI scope. Capturing the request's scoped repository instead would be a
/// use-after-dispose that only shows up under real hosting — the classic
/// version of this bug.
///
/// No network traffic: the Club Log service is real but its HttpClient is
/// backed by a stub handler.
/// </summary>
[Trait("Category", "Integration")]
[Collection("LiteDbMapper")]
public class QsoServiceLedgerWiringTests : IDisposable
{
    private readonly LiteDbTestFixture _fixture = new();
    private readonly ServiceProvider _provider;
    private readonly LiteQsoRepository _repository;

    public QsoServiceLedgerWiringTests()
    {
        _repository = new LiteQsoRepository(_fixture.Context);

        var services = new ServiceCollection();
        services.AddLogging();
        // Scoped, exactly as production registers it.
        services.AddScoped<IQsoRepository>(_ => new LiteQsoRepository(_fixture.Context));
        services.AddScoped<QslSyncRecorder>();
        _provider = services.BuildServiceProvider();
    }

    public void Dispose()
    {
        _provider.Dispose();
        _fixture.Dispose();
    }

    /// <summary>
    /// A hub whose Clients.All resolves — CreateAsync broadcasts the new QSO,
    /// and a bare mock returns null there.
    /// </summary>
    private static IHubContext<LogHub, ILogHubClient> Hub()
    {
        var clients = new Mock<IHubClients<ILogHubClient>>();
        clients.Setup(c => c.All).Returns(new Mock<ILogHubClient>().Object);

        var hub = new Mock<IHubContext<LogHub, ILogHubClient>>();
        hub.Setup(h => h.Clients).Returns(clients.Object);
        return hub.Object;
    }

    private static HttpClient RespondingWith(HttpStatusCode status, string body)
    {
        var handler = new Mock<HttpMessageHandler>();
        handler.Protected()
            .Setup<Task<HttpResponseMessage>>("SendAsync",
                ItExpr.IsAny<HttpRequestMessage>(), ItExpr.IsAny<CancellationToken>())
            .ReturnsAsync(new HttpResponseMessage(status) { Content = new StringContent(body) });
        return new HttpClient(handler.Object);
    }

    private static ISettingsService SettingsWith(bool clubLogEnabled)
    {
        var settings = new UserSettings
        {
            Station = { Callsign = "N9BC" },
            ClubLog = { Enabled = clubLogEnabled, ApiKey = "key", Email = "a@b.c", Password = "pw" },
        };
        var mock = new Mock<ISettingsService>();
        mock.Setup(s => s.GetSettingsAsync(It.IsAny<string>())).ReturnsAsync(settings);
        return mock.Object;
    }

    private QsoService BuildService(bool clubLogEnabled, HttpStatusCode status, string body)
    {
        var clubLog = new ClubLogService(
            SettingsWith(clubLogEnabled), RespondingWith(status, body),
            NullLogger<ClubLogService>.Instance);

        return new QsoService(
            _repository,
            Hub(),
            spotStatusService: null,
            clubLog: clubLog,
            hrdLog: null,
            eqsl: null,
            scopeFactory: _provider.GetRequiredService<IServiceScopeFactory>(),
            logger: NullLogger<QsoService>.Instance);
    }

    private static CreateQsoRequest NewQso() =>
        new("w1aw", DateTime.UtcNow.Date, "1200", "20m", "FT8", Frequency: 14075);

    /// <summary>The upload runs in the background, so poll rather than assume.</summary>
    private async Task<QslServiceSync?> WaitForLedger(string qsoId, string service)
    {
        for (var i = 0; i < 100; i++)
        {
            var entry = (await _repository.GetByIdAsync(qsoId))?.QslSync?.For(service);
            if (entry != null) return entry;
            await Task.Delay(20);
        }
        return null;
    }

    [Fact]
    public async Task ASuccessfulUploadReachesTheLedgerThroughItsOwnScope()
    {
        var service = BuildService(clubLogEnabled: true, HttpStatusCode.OK, "OK");

        var created = await service.CreateAsync(NewQso());

        var entry = await WaitForLedger(created.Id, QslSyncLedger.ClubLogKey);
        entry.Should().NotBeNull("the background task must resolve a fresh scope and record the result");
        entry!.Status.Should().Be(SyncStatus.Synced);
        entry.SyncedAt.Should().NotBeNull();
    }

    [Fact]
    public async Task AFailedUploadIsRecordedWithItsKind()
    {
        var service = BuildService(clubLogEnabled: true, HttpStatusCode.ServiceUnavailable, "busy");

        var created = await service.CreateAsync(NewQso());

        var entry = await WaitForLedger(created.Id, QslSyncLedger.ClubLogKey);
        entry.Should().NotBeNull();
        entry!.Status.Should().Be(SyncStatus.NotSynced);
        entry.FailureKind.Should().Be(QslFailureKind.Temporary);
        entry.IsRetryable.Should().BeTrue();
    }

    [Fact]
    public async Task ADisabledServiceLeavesTheQsoUntracked()
    {
        var service = BuildService(clubLogEnabled: false, HttpStatusCode.OK, "OK");

        var created = await service.CreateAsync(NewQso());

        // Give the background task room to do the wrong thing if it were going to.
        await Task.Delay(300);
        (await _repository.GetByIdAsync(created.Id))!.QslSync.Should().BeNull(
            "a service the operator has not enabled must not mark QSOs as failed");
    }

    [Fact]
    public async Task LoggingStillSucceedsWhenTheUploadThrows()
    {
        // Logging a QSO must never fail because an external service did.
        var handler = new Mock<HttpMessageHandler>();
        handler.Protected()
            .Setup<Task<HttpResponseMessage>>("SendAsync",
                ItExpr.IsAny<HttpRequestMessage>(), ItExpr.IsAny<CancellationToken>())
            .ThrowsAsync(new HttpRequestException("network down"));
        var clubLog = new ClubLogService(SettingsWith(true), new HttpClient(handler.Object),
            NullLogger<ClubLogService>.Instance);

        var service = new QsoService(
            _repository, Hub(),
            null, clubLog, null, null,
            _provider.GetRequiredService<IServiceScopeFactory>(), NullLogger<QsoService>.Instance);

        var created = await service.CreateAsync(NewQso());

        created.Callsign.Should().Be("W1AW");
        var entry = await WaitForLedger(created.Id, QslSyncLedger.ClubLogKey);
        entry!.FailureKind.Should().Be(QslFailureKind.Temporary,
            "a transport failure is the one kind worth retrying later");
    }

    [Fact]
    public async Task WithoutAScopeFactoryLoggingStillWorks()
    {
        // The scope factory is optional so existing construction sites (and
        // older tests) keep compiling; the QSO must still be created.
        var clubLog = new ClubLogService(SettingsWith(true), RespondingWith(HttpStatusCode.OK, "OK"),
            NullLogger<ClubLogService>.Instance);
        var service = new QsoService(
            _repository, Hub(),
            null, clubLog, null, null);

        var created = await service.CreateAsync(NewQso());

        created.Id.Should().NotBeNullOrEmpty();
    }
}
