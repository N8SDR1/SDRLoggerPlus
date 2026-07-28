using FluentAssertions;
using Microsoft.AspNetCore.SignalR;
using Moq;
using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database;
using SDRLoggerPlus.Server.Hubs;
using SDRLoggerPlus.Server.Services;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

/// <summary>
/// PHASE 2 — manual write path pins QsoDate to canonical UTC.
/// See docs/design/timezone-architecture.md.
///
/// The API contract is UTC. The frontend submits a Z instant, but the server must not
/// trust the client's Kind: an offset value converts, and a NAIVE (Unspecified) value is
/// ASSUMED UTC (ham/ADIF convention) rather than shifted as if local — the latter is the
/// historical double-shift that stored the wrong instant.
/// </summary>
[Trait("Category", "Unit")]
public class QsoServiceUtcWriteTests
{
    private readonly Mock<IQsoRepository> _repo = new();
    private readonly QsoService _service;
    private Qso? _captured;

    public QsoServiceUtcWriteTests()
    {
        _repo.Setup(r => r.CreateAsync(It.IsAny<Qso>()))
            .Callback<Qso>(q => _captured = q)
            .ReturnsAsync((Qso q) => { q.Id = "qso-1"; return q; });

        var hub = new Mock<IHubContext<LogHub, ILogHubClient>>();
        var clients = new Mock<IHubClients<ILogHubClient>>();
        clients.Setup(c => c.All).Returns(new Mock<ILogHubClient>().Object);
        hub.Setup(h => h.Clients).Returns(clients.Object);

        _service = new QsoService(_repo.Object, hub.Object);
    }

    private static CreateQsoRequest Request(DateTime qsoDate) =>
        new("W1AW", qsoDate, "0230", "20m", "SSB");

    [Fact]
    public async Task Utc_instant_is_stored_unchanged()
    {
        var utc = new DateTime(2026, 3, 10, 2, 30, 0, DateTimeKind.Utc);
        await _service.CreateAsync(Request(utc));

        _captured!.QsoDate.Should().Be(utc);
        _captured.QsoDate.Kind.Should().Be(DateTimeKind.Utc);
    }

    [Fact]
    public async Task Offset_local_value_is_converted_to_the_same_instant()
    {
        // 2026-03-09 21:30 -05:00 == 2026-03-10 02:30 UTC.
        var local = new DateTimeOffset(2026, 3, 9, 21, 30, 0, TimeSpan.FromHours(-5)).LocalDateTime;
        await _service.CreateAsync(Request(local));

        _captured!.QsoDate.ToUniversalTime()
            .Should().Be(new DateTime(2026, 3, 10, 2, 30, 0, DateTimeKind.Utc));
        _captured.QsoDate.Kind.Should().Be(DateTimeKind.Utc);
    }

    [Fact]
    public async Task Naive_value_is_assumed_utc_not_shifted()
    {
        // A client that sends "2026-03-10T02:30:00" with no Z/offset means 02:30 UTC.
        var naive = new DateTime(2026, 3, 10, 2, 30, 0, DateTimeKind.Unspecified);
        await _service.CreateAsync(Request(naive));

        // Must NOT move by the server's offset — same wall-clock, now labelled UTC.
        _captured!.QsoDate.Should().Be(new DateTime(2026, 3, 10, 2, 30, 0, DateTimeKind.Utc));
        _captured.QsoDate.Kind.Should().Be(DateTimeKind.Utc);
    }
}
