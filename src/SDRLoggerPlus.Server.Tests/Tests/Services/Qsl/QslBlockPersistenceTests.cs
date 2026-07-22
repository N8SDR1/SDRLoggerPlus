using System.Net;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Moq.Protected;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Services;
using SDRLoggerPlus.Server.Services.Qsl;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services.Qsl;

[Trait("Category", "Unit")]
public class QslBlockStateStoreTests : IDisposable
{
    private readonly string _dir;
    private readonly string _path;

    public QslBlockStateStoreTests()
    {
        _dir = Path.Combine(Path.GetTempPath(), $"slp_block_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_dir);
        _path = Path.Combine(_dir, "qsl-block-state.json");
    }

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { }
    }

    [Fact]
    public void RoundTripsTheBlock()
    {
        var store = new QslBlockStateStore(_path);
        var when = new DateTime(2026, 7, 21, 12, 0, 0, DateTimeKind.Utc);

        store.Save(new QslBlockState { ClubLogBlockedUtc = when, ClubLogReason = "HTTP 403" });

        var loaded = new QslBlockStateStore(_path).Load();
        loaded.ClubLogBlockedUtc.Should().Be(when);
        loaded.ClubLogReason.Should().Be("HTTP 403");
    }

    [Fact]
    public void MissingFileIsNotBlocked()
    {
        new QslBlockStateStore(_path).Load().ClubLogBlockedUtc.Should().BeNull();
    }

    [Fact]
    public void MalformedFileFailsOpen()
    {
        // Failing closed would mean refusing to upload forever because a JSON
        // file got truncated.
        File.WriteAllText(_path, "{ this is not json");

        new QslBlockStateStore(_path).Load().ClubLogBlockedUtc.Should().BeNull();
    }

    [Fact]
    public void SavingAnEmptyStateClearsTheBlock()
    {
        var store = new QslBlockStateStore(_path);
        store.Save(new QslBlockState { ClubLogBlockedUtc = DateTime.UtcNow });

        store.Save(new QslBlockState());

        store.Load().ClubLogBlockedUtc.Should().BeNull();
    }
}

[Trait("Category", "Unit")]
public class ClubLogBlockPersistenceTests : IDisposable
{
    private readonly string _dir;
    private readonly QslBlockStateStore _store;

    public ClubLogBlockPersistenceTests()
    {
        _dir = Path.Combine(Path.GetTempPath(), $"slp_cl_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_dir);
        _store = new QslBlockStateStore(Path.Combine(_dir, "qsl-block-state.json"));
    }

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { }
    }

    private static Mock<ISettingsService> Settings()
    {
        var settings = new UserSettings
        {
            Station = { Callsign = "N9BC" },
            ClubLog = { Enabled = true, ApiKey = "key", Email = "a@b.c", Password = "pw" },
        };
        var mock = new Mock<ISettingsService>();
        mock.Setup(s => s.GetSettingsAsync(It.IsAny<string>())).ReturnsAsync(settings);
        return mock;
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

    private ClubLogService Service(HttpClient http) =>
        new(Settings().Object, http, NullLogger<ClubLogService>.Instance, _store);

    private static Qso MakeQso() => new()
    {
        Id = Guid.NewGuid().ToString(),
        Callsign = "W1AW",
        Band = "20m",
        Mode = "FT8",
        QsoDate = DateTime.UtcNow.Date,
        TimeOn = "1200",
    };

    [Fact]
    public async Task A403BlocksAndPersists()
    {
        var service = Service(RespondingWith(HttpStatusCode.Forbidden, "Login rejected"));

        var result = await service.UploadQsoAsync(MakeQso());

        result.Ok.Should().BeFalse();
        result.Kind.Should().Be(QslFailureKind.Auth);
        service.IsBlocked.Should().BeTrue();
        _store.Load().ClubLogBlockedUtc.Should().NotBeNull("the block must outlive the process");
    }

    [Fact]
    public async Task ARestartStaysBlocked()
    {
        // The bug this fixes: the block was process state, so restarting the
        // app re-armed uploads against credentials Club Log had already
        // rejected — and repeating rejected POSTs is what gets an IP banned.
        await Service(RespondingWith(HttpStatusCode.Forbidden, "Login rejected")).UploadQsoAsync(MakeQso());

        var afterRestart = Service(RespondingWith(HttpStatusCode.OK, "OK"));

        afterRestart.IsBlocked.Should().BeTrue();
        var result = await afterRestart.UploadQsoAsync(MakeQso());
        result.Ok.Should().BeFalse();
        result.Kind.Should().Be(QslFailureKind.Auth);
    }

    [Fact]
    public async Task ResetBlockClearsThePersistedState()
    {
        var service = Service(RespondingWith(HttpStatusCode.Forbidden, "Login rejected"));
        await service.UploadQsoAsync(MakeQso());

        service.ResetBlock();

        service.IsBlocked.Should().BeFalse();
        _store.Load().ClubLogBlockedUtc.Should().BeNull();
        Service(RespondingWith(HttpStatusCode.OK, "OK")).IsBlocked.Should().BeFalse(
            "a fresh start after a reset must not resurrect the block");
    }

    [Fact]
    public async Task AServerErrorIsTemporaryAndDoesNotBlock()
    {
        var service = Service(RespondingWith(HttpStatusCode.ServiceUnavailable, "busy"));

        var result = await service.UploadQsoAsync(MakeQso());

        result.Kind.Should().Be(QslFailureKind.Temporary);
        service.IsBlocked.Should().BeFalse("a 503 is Club Log's problem, not a credential problem");
        _store.Load().ClubLogBlockedUtc.Should().BeNull();
    }

    [Fact]
    public async Task ADisabledServiceReportsNotConfiguredRatherThanFailure()
    {
        var settings = new UserSettings { ClubLog = { Enabled = false } };
        var mock = new Mock<ISettingsService>();
        mock.Setup(s => s.GetSettingsAsync(It.IsAny<string>())).ReturnsAsync(settings);
        var service = new ClubLogService(mock.Object, RespondingWith(HttpStatusCode.OK, "OK"),
            NullLogger<ClubLogService>.Instance, _store);

        var result = await service.UploadQsoAsync(MakeQso());

        result.Kind.Should().Be(QslFailureKind.NotConfigured);
        result.IsRecordable.Should().BeFalse("a service that is off must not mark QSOs as failed");
    }

    [Fact]
    public async Task AnUnparseableBodyIsRejectedNotTemporary()
    {
        // Club Log may already have accepted the POST; re-sending is exactly
        // the repetition its firewall watches for.
        var service = Service(RespondingWith(HttpStatusCode.OK, "<html>maintenance</html>"));

        var result = await service.UploadQsoAsync(MakeQso());

        result.Kind.Should().Be(QslFailureKind.Rejected);
    }

    [Fact]
    public async Task WorksWithoutAStore()
    {
        // The store is optional so existing construction sites keep working.
        var service = new ClubLogService(Settings().Object, RespondingWith(HttpStatusCode.OK, "OK"),
            NullLogger<ClubLogService>.Instance);

        (await service.UploadQsoAsync(MakeQso())).Ok.Should().BeTrue();
    }
}
