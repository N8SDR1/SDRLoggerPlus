using System.Collections.Concurrent;
using System.Net;
using System.Net.WebSockets;
using System.Text;
using FluentAssertions;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using SDRLoggerPlus.Contracts.Events;
using SDRLoggerPlus.Server.Hubs;
using SDRLoggerPlus.Server.Services;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

/// <summary>
/// End-to-end proof of the TCI meters pipeline without touching user data:
/// a real Kestrel WebSocket endpoint plays Thetis (protocol/device/ready
/// handshake, then sensor frames), the actual TciRadioConnection consumes it,
/// and a mocked hub captures the OnTciMeters broadcasts. Verifies the
/// handshake order (start; then both sensor enables), value fidelity, and
/// that 100 ms coalescing holds under a frame burst.
/// </summary>
[Trait("Category", "Integration")]
public class TciMetersPipelineTests : IAsyncLifetime
{
    private WebApplication? _mockThetis;
    private int _port;

    private readonly ConcurrentQueue<string> _receivedByThetis = new();
    private readonly ConcurrentQueue<TciMetersEvent> _metersEvents = new();
    private readonly TaskCompletionSource _bothEnablesSeen =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    public async Task InitializeAsync()
    {
        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.ConfigureKestrel(o => o.Listen(IPAddress.Loopback, 0));
        _mockThetis = builder.Build();
        _mockThetis.UseWebSockets();

        _mockThetis.Map("/", async context =>
        {
            if (!context.WebSockets.IsWebSocketRequest)
            {
                context.Response.StatusCode = 400;
                return;
            }

            using var ws = await context.WebSockets.AcceptWebSocketAsync();

            async Task SendAsync(string s) =>
                await ws.SendAsync(Encoding.UTF8.GetBytes(s), WebSocketMessageType.Text, true, CancellationToken.None);

            await SendAsync("protocol:ExpertSDR3,1.9;");
            await SendAsync("device:Thetis;");
            await SendAsync("vfo:0,0,14074000;");
            await SendAsync("ready;");

            var sawRxEnable = false;
            var sawTxEnable = false;
            var buffer = new byte[4096];

            // Read until both enables arrive, then burst sensor frames.
            while (ws.State == WebSocketState.Open)
            {
                var result = await ws.ReceiveAsync(buffer, CancellationToken.None);
                if (result.MessageType == WebSocketMessageType.Close) break;

                var text = Encoding.UTF8.GetString(buffer, 0, result.Count);
                _receivedByThetis.Enqueue(text);

                if (text.Contains("rx_sensors_enable:true")) sawRxEnable = true;
                if (text.Contains("tx_sensors_enable:true")) sawTxEnable = true;

                if (sawRxEnable && sawTxEnable)
                {
                    _bothEnablesSeen.TrySetResult();

                    // 10 RX frames + 10 TX frames back-to-back (no pacing, so
                    // the burst deterministically lands in at most 2 throttle
                    // windows regardless of host speed). Real Thetis sends
                    // tx_sensors for BOTH trx indices and rx frames for both
                    // receivers — the off-instance frames (rx/trx 1) carry
                    // poison values that must never surface.
                    for (var i = 0; i < 10; i++)
                    {
                        await SendAsync($"rx_channel_sensors_ex:0,0,{(-97.4 + i).ToString("F1", System.Globalization.CultureInfo.InvariantCulture)},-99.1,-85.0;");
                        await SendAsync("rx_sensors:0,-96.0;");
                        await SendAsync("rx_sensors:1,-30.0;");
                        await SendAsync("rx_channel_sensors_ex:1,0,-31.0,-31.0,-31.0;");
                        await SendAsync("rx_channel_sensors_ex:0,1,-32.0,-32.0,-32.0;");
                        await SendAsync("tx_sensors:0,-12.3,4.8,5.0,1.42;");
                        await SendAsync("tx_sensors:1,-1.0,99.0,99.0,9.9;");
                    }

                    // Like real Thetis, the stream continues on its timer; the
                    // next frame after the throttle window flushes the
                    // coalesced TX readings (the aggregator emits only when a
                    // frame triggers it — no standalone flush timer).
                    await Task.Delay(150);
                    await SendAsync("tx_sensors:0,-12.3,4.8,5.0,1.42;");

                    // Hold the socket open so the client loop doesn't error.
                    await Task.Delay(500);
                    break;
                }
            }
        });

        await _mockThetis.StartAsync();
        _port = new Uri(_mockThetis.Urls.First()).Port;
    }

    public async Task DisposeAsync()
    {
        if (_mockThetis != null) await _mockThetis.DisposeAsync();
    }

    [Fact]
    public async Task SensorFrames_FlowFromSocketToHubBroadcast_WithCoalescing()
    {
        // Hub mock capturing OnTciMeters
        var hubClient = new Mock<ILogHubClient>();
        hubClient.Setup(c => c.OnTciMeters(It.IsAny<TciMetersEvent>()))
            .Callback<TciMetersEvent>(_metersEvents.Enqueue)
            .Returns(Task.CompletedTask);
        var clients = new Mock<IHubClients<ILogHubClient>>();
        clients.SetupGet(c => c.All).Returns(hubClient.Object);
        var hubContext = new Mock<IHubContext<LogHub, ILogHubClient>>();
        hubContext.SetupGet(h => h.Clients).Returns(clients.Object);

        var device = new TciRadioDevice
        {
            Id = "tci-test",
            Model = "MockThetis",
            IpAddress = "127.0.0.1",
            TciPort = _port,
            LastSeen = DateTime.UtcNow,
        };

        var connection = new TciRadioConnection(device, NullLogger.Instance, hubContext.Object);
        var connectTask = connection.ConnectAsync();

        // Handshake must complete: start; + both enables observed by "Thetis"
        var handshake = await Task.WhenAny(_bothEnablesSeen.Task, Task.Delay(10_000));
        handshake.Should().Be(_bothEnablesSeen.Task, "client must enable sensors after ready");

        var allReceived = string.Join("", _receivedByThetis);
        allReceived.Should().Contain("start;")
            .And.Contain("rx_sensors_enable:true,100;")
            .And.Contain("tx_sensors_enable:true,100;");
        allReceived.IndexOf("start;", StringComparison.Ordinal).Should().BeLessThan(
            allReceived.IndexOf("rx_sensors_enable", StringComparison.Ordinal),
            "start; precedes sensor enables");

        // Wait until a coalesced event carrying the TX readings has emitted —
        // the first event may close its 100 ms window before any tx_sensors
        // frame arrived.
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (!_metersEvents.Any(e => e.TxSwr.HasValue) && DateTime.UtcNow < deadline)
            await Task.Delay(25);

        await connection.DisconnectAsync();
        await Task.WhenAny(connectTask, Task.Delay(2000));

        _metersEvents.Should().NotBeEmpty("sensor frames must surface as OnTciMeters broadcasts");

        var evt = _metersEvents.First();
        evt.RadioId.Should().Be("tci-test");
        evt.RxSignalDbm.Should().NotBeNull();
        evt.RxSignalDbm!.Value.Should().BeInRange(-98, -87);
        evt.RxAvgSignalDbm.Should().BeApproximately(-99.1, 0.001);

        // SWR may land in the first or a later coalesced event depending on
        // frame interleaving; across all events it must appear with full fidelity.
        _metersEvents.Should().Contain(e => e.TxSwr == 1.42);

        // Off-instance poison values (rx 1, channel 1, trx 1) must never surface.
        _metersEvents.Should().NotContain(e => e.RxSignalDbm > -35,
            "frames for the wrong receiver/channel must be filtered");
        _metersEvents.Should().NotContain(e => e.TxSwr == 9.9,
            "tx_sensors for the non-selected trx must be filtered");

        // The back-to-back burst lands in at most 2 throttle windows, plus the
        // trailing flush frame => never more than 3 broadcasts.
        _metersEvents.Count.Should().BeLessThanOrEqualTo(3,
            "coalescing must hold under burst");
    }
}
