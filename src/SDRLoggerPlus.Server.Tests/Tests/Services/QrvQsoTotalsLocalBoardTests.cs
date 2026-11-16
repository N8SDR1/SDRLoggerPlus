using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using FluentAssertions;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database;
using SDRLoggerPlus.Server.Hubs;
using SDRLoggerPlus.Server.Services;
using SDRLoggerPlus.Server.Services.Qrv;
using Xunit;
using Xunit.Abstractions;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

/// <summary>
/// The other totals tests swap in an <see cref="HttpMessageHandler"/>. This one
/// opens a real socket on 127.0.0.1, obtains the install key from heartbeat.php,
/// stamps QSOs the way <see cref="QsoService"/> does, and lets
/// <see cref="QrvQsoTotalsService"/> post qso.php.
/// </summary>
[Trait("Category", "Unit")]
public class QrvQsoTotalsLocalBoardTests
{
    private const string InstallKey = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    private readonly ITestOutputHelper _output;

    public QrvQsoTotalsLocalBoardTests(ITestOutputHelper output) => _output = output;

    [Fact]
    public async Task Service_posts_the_snapshot_to_a_local_board_and_zeros_when_switched_off()
    {
        using var board = LocalQrvBoard.Start(InstallKey);
        using var http = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
        var presence = new QrvPresenceClient(http);
        var totals = new QrvQsoTotalsClient(http);
        var signal = new QrvQsoTotalsSignal();
        var settings = new UserSettings
        {
            Station = new StationSettings { Callsign = "n8sdr" },
            Qrv = new QrvSettings
            {
                Enabled = false,
                QsoTotalsEnabled = false,
                BaseUrl = board.BaseUrl,
                InstallKey = null,
            },
        };

        presence.ApplySettings(true, board.BaseUrl, settings.Station.Callsign, null, null);
        presence.SetRadio(true, 14_200_000, "USB");
        await presence.TickAsync(CancellationToken.None);

        var heartbeat = board.Snapshot().Should().ContainSingle(p => p.Path.EndsWith("/v1/heartbeat.php")).Subject;
        heartbeat.Url.Should().StartWith("http://127.0.0.1:");
        heartbeat.Body.Should().Contain("\"app\":\"Logger\"");
        heartbeat.Body.Should().Contain("\"call\":\"N8SDR\"");
        presence.Credentials().InstallKey.Should().Be(InstallKey);
        board.Snapshot().Should().NotContain(p => p.Path.Contains("qso.php", StringComparison.Ordinal));

        var saved = new List<Qso>();
        var gate = new object();
        var repo = new Mock<IQsoRepository>();
        repo.Setup(r => r.CreateAsync(It.IsAny<Qso>()))
            .Returns((Qso q) =>
            {
                q.Id ??= Guid.NewGuid().ToString("N");
                lock (gate) saved.Add(q);
                return Task.FromResult(q);
            });
        repo.Setup(r => r.GetAllAsync()).Returns(() =>
        {
            lock (gate) return Task.FromResult<IEnumerable<Qso>>(saved.ToList());
        });

        var services = new ServiceCollection();
        var settingsService = new Mock<ISettingsService>();
        settingsService.Setup(s => s.GetSettingsAsync(It.IsAny<string>())).ReturnsAsync(settings);
        services.AddSingleton(settingsService.Object);
        services.AddSingleton(repo.Object);
        await using var provider = services.BuildServiceProvider();

        var hosted = new QrvQsoTotalsService(
            totals,
            presence,
            signal,
            provider.GetRequiredService<IServiceScopeFactory>(),
            NullLogger<QrvQsoTotalsService>.Instance);

        try
        {
            await hosted.StartAsync(CancellationToken.None);
            await Task.Delay(TimeSpan.FromSeconds(2));
            board.Snapshot().Should().NotContain(p => p.Path.Contains("qso.php", StringComparison.Ordinal));
            totals.Status().Phase.Should().Be("off");

            var now = DateTime.UtcNow;
            var weekStart = QrvQsoTotals.WeekStartUtc(now);
            var monthStart = QrvQsoTotals.MonthStartUtc(now);
            var yearStart = QrvQsoTotals.YearStartUtc(now);
            // Inside the current week, month, and year, including a run that starts
            // a few seconds after a UTC boundary.
            var intersection = new[] { weekStart, monthStart, yearStart }.Max();
            var recent = now.AddSeconds(-30);
            if (recent < intersection) recent = intersection;
            var monthOnly = monthStart < weekStart;
            var yearOnly = yearStart < monthStart;

            var tracker = new TciRadioSourceTracker();
            tracker.Connected("lyra-1");
            tracker.NoteProtocol("lyra-1", "Lyra");
            var logging = QsoLogger(repo.Object, tracker, presence, signal);

            await logging.CreateAsync(Request("W1AW", recent, "USB", frequency: 14200, comment: "secret-note", myPota: "K-1234"));
            await logging.CreateAsync(Request("W1AW", recent, "CW"));
            tracker.NoteStd("lyra-1");
            using (QsoLoggingOrigin.Use("STD"))
                await logging.CreateAsync(Request("K2DIG", recent, "FT8"));
            await logging.CreateAsync(Request("N0CON", recent, "USB", contest: "CQ-WW-SSB"));
            if (monthOnly)
                await logging.CreateAsync(Request("W1AW", monthStart, "AM"));
            if (yearOnly)
                await logging.CreateAsync(Request("W1AW", yearStart, "DIGU"));
            tracker.Disconnected("lyra-1");
            await logging.CreateAsync(Request("K1JT", recent, "USB"));

            lock (gate)
            {
                saved.Should().Contain(q => q.RadioSource == "Lyra" && q.Mode == "USB" && q.Callsign == "W1AW");
                saved.Should().Contain(q => q.RadioSource == "Lyra" && q.Mode == "CW");
                saved.Should().Contain(q => q.RadioSource == "STD" && q.Mode == "FT8");
                saved.Should().Contain(q => q.RadioSource == "Lyra" && q.Contest != null && q.Contest.ContestId == "CQ-WW-SSB");
                saved.Should().Contain(q => q.RadioSource == null && q.Callsign == "K1JT");
            }

            settings.Qrv.QsoTotalsEnabled = true;

            var posted = await board.WaitForAsync(
                posts => posts.Any(p => p.Path.EndsWith("/v1/qso.php")),
                TimeSpan.FromSeconds(8));
            var snapshot = posted.Where(p => p.Path.EndsWith("/v1/qso.php")).Should().ContainSingle().Subject;
            _output.WriteLine(snapshot.Body);
            Console.WriteLine("QRV_QSO_CAPTURE " + snapshot.Body);

            snapshot.Url.Should().Be(board.QsoUrl);
            snapshot.ContentType.Should().Contain("application/json");
            AssertSnapshot(snapshot.Body, voice: 1, cw: 1, digital: 1,
                monthVoice: 1 + (monthOnly ? 1 : 0), monthCw: 1, monthDigital: 1,
                yearVoice: 1 + (monthOnly ? 1 : 0), yearCw: 1, yearDigital: 1 + (yearOnly ? 1 : 0));
            await WaitUntilAsync(() => totals.Status().Phase == "posted", TimeSpan.FromSeconds(2));
            totals.Status().Phase.Should().Be("posted");
            totals.Status().Detail.Should().Contain("2026-W47");

            settings.Qrv.QsoTotalsEnabled = false;
            var afterOff = await board.WaitForAsync(
                posts => posts.Count(p => p.Path.EndsWith("/v1/qso.php")) >= 2,
                TimeSpan.FromSeconds(16));
            var qsoPosts = afterOff.Where(p => p.Path.EndsWith("/v1/qso.php")).ToList();
            qsoPosts.Should().HaveCount(2);
            _output.WriteLine(qsoPosts[1].Body);
            Console.WriteLine("QRV_QSO_ZEROS " + qsoPosts[1].Body);
            AssertSnapshot(qsoPosts[1].Body, 0, 0, 0, 0, 0, 0, 0, 0, 0);
            qsoPosts[1].Url.Should().Be(board.QsoUrl);

            await Task.Delay(TimeSpan.FromSeconds(2));
            board.Snapshot().Count(p => p.Path.EndsWith("/v1/qso.php")).Should().Be(2);
            totals.Status().Phase.Should().Be("off");
            totals.Status().Detail.Should().Contain("Nothing is sent");
            board.Snapshot().Should().OnlyContain(p => p.Url.StartsWith("http://127.0.0.1:"));
        }
        finally
        {
            using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(3));
            await hosted.StopAsync(stop.Token);
            using var qrt = new CancellationTokenSource(TimeSpan.FromSeconds(3));
            await presence.ShutdownAsync(qrt.Token);
        }
    }

    private static async Task WaitUntilAsync(Func<bool> ready, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!ready() && DateTime.UtcNow < deadline)
            await Task.Delay(50);
    }

    private static void AssertSnapshot(
        string json,
        int voice, int cw, int digital,
        int monthVoice, int monthCw, int monthDigital,
        int yearVoice, int yearCw, int yearDigital)
    {
        json.Length.Should().BeLessThan(500);
        json.Should().NotContain("[");
        foreach (var leak in new[] { "W1AW", "K2DIG", "K1JT", "N0CON", "secret-note", "K-1234", "CQ-WW", "14200", "FT8", "USB", "20m", "adif", "freq", "hz" })
            json.Should().NotContain(leak);

        using var doc = JsonDocument.Parse(json);
        doc.RootElement.EnumerateObject().Select(p => p.Name).Should().Equal(
            "call", "app", "install_key", "week", "month", "year");
        doc.RootElement.GetProperty("call").GetString().Should().Be("N8SDR");
        doc.RootElement.GetProperty("app").GetString().Should().Be("Logger");
        doc.RootElement.GetProperty("install_key").GetString().Should().Be(InstallKey);
        Bucket(doc.RootElement.GetProperty("week")).Should().Be((voice, cw, digital));
        Bucket(doc.RootElement.GetProperty("month")).Should().Be((monthVoice, monthCw, monthDigital));
        Bucket(doc.RootElement.GetProperty("year")).Should().Be((yearVoice, yearCw, yearDigital));
    }

    private static (int Voice, int Cw, int Digital) Bucket(JsonElement counts)
    {
        counts.EnumerateObject().Select(p => p.Name).Should().Equal("voice", "cw", "digital");
        return (counts.GetProperty("voice").GetInt32(), counts.GetProperty("cw").GetInt32(), counts.GetProperty("digital").GetInt32());
    }

    private static QsoService QsoLogger(
        IQsoRepository repo, TciRadioSourceTracker tracker, QrvPresenceClient presence, QrvQsoTotalsSignal signal)
    {
        var hub = new Mock<IHubContext<LogHub, ILogHubClient>>();
        var clients = new Mock<IHubClients<ILogHubClient>>();
        clients.Setup(c => c.All).Returns(new Mock<ILogHubClient>().Object);
        hub.Setup(h => h.Clients).Returns(clients.Object);
        return new QsoService(repo, hub.Object, radioSources: tracker, qrvPresence: presence, qrvTotals: signal);
    }

    private static CreateQsoRequest Request(
        string call, DateTime when, string mode,
        double? frequency = null, string? comment = null, string? contest = null, string? myPota = null) =>
        new(call, when, "120000", "20m", mode, Frequency: frequency, Comment: comment, Contest: contest, MyPotaRef: myPota);

    private sealed class LocalQrvBoard : IDisposable
    {
        private readonly HttpListener _listener = new();
        private readonly CancellationTokenSource _stop = new();
        private readonly List<CapturedPost> _posts = new();
        private readonly object _gate = new();
        private readonly string _installKey;
        private readonly Task _loop;

        public string BaseUrl { get; }
        public string QsoUrl { get; }

        private LocalQrvBoard(int port, string installKey)
        {
            _installKey = installKey;
            BaseUrl = $"http://127.0.0.1:{port}/v1";
            QsoUrl = $"http://127.0.0.1:{port}/v1/qso.php";
            _listener.Prefixes.Add($"http://127.0.0.1:{port}/");
            _listener.Start();
            _loop = Task.Run(ListenAsync);
        }

        public static LocalQrvBoard Start(string installKey)
        {
            for (var attempt = 0; attempt < 5; attempt++)
            {
                var probe = new TcpListener(IPAddress.Loopback, 0);
                probe.Start();
                var port = ((IPEndPoint)probe.LocalEndpoint).Port;
                probe.Stop();
                try
                {
                    return new LocalQrvBoard(port, installKey);
                }
                catch (HttpListenerException)
                {
                }
            }

            throw new InvalidOperationException("Could not bind a local QRV board.");
        }

        public IReadOnlyList<CapturedPost> Snapshot()
        {
            lock (_gate) return _posts.ToList();
        }

        public async Task<IReadOnlyList<CapturedPost>> WaitForAsync(Func<IReadOnlyList<CapturedPost>, bool> ready, TimeSpan timeout)
        {
            var deadline = DateTime.UtcNow + timeout;
            while (true)
            {
                var snapshot = Snapshot();
                if (ready(snapshot) || DateTime.UtcNow >= deadline)
                    return snapshot;
                await Task.Delay(50);
            }
        }

        public void Dispose()
        {
            _stop.Cancel();
            _listener.Stop();
            _listener.Close();
            try { _loop.Wait(TimeSpan.FromSeconds(2)); } catch { /* listener closed */ }
            _stop.Dispose();
        }

        private async Task ListenAsync()
        {
            while (!_stop.IsCancellationRequested)
            {
                HttpListenerContext ctx;
                try
                {
                    ctx = await _listener.GetContextAsync().WaitAsync(_stop.Token);
                }
                catch (Exception) when (_stop.IsCancellationRequested)
                {
                    break;
                }
                catch (HttpListenerException)
                {
                    break;
                }
                catch (ObjectDisposedException)
                {
                    break;
                }

                try
                {
                    await HandleAsync(ctx);
                }
                catch
                {
                    try { ctx.Response.Abort(); } catch { /* already closed */ }
                }
            }
        }

        private async Task HandleAsync(HttpListenerContext ctx)
        {
            using var buffer = new MemoryStream();
            await ctx.Request.InputStream.CopyToAsync(buffer);
            var body = Encoding.UTF8.GetString(buffer.ToArray());
            var path = ctx.Request.Url?.AbsolutePath ?? "";
            lock (_gate)
            {
                _posts.Add(new CapturedPost(
                    ctx.Request.HttpMethod,
                    ctx.Request.Url?.ToString() ?? path,
                    path,
                    ctx.Request.ContentType,
                    body));
            }

            var json = path.EndsWith("/v1/heartbeat.php", StringComparison.Ordinal)
                ? "{\"ok\":true,\"install_key\":\"" + _installKey + "\",\"stale_s\":180}"
                : path.EndsWith("/v1/qso.php", StringComparison.Ordinal)
                    ? "{\"ok\":true,\"periods\":{\"week\":\"2026-W47\",\"month\":\"2026-11\",\"year\":\"2026\"}}"
                    : "{\"ok\":true}";
            var payload = Encoding.UTF8.GetBytes(json);
            ctx.Response.StatusCode = 200;
            ctx.Response.ContentType = "application/json";
            ctx.Response.ContentLength64 = payload.Length;
            await ctx.Response.OutputStream.WriteAsync(payload);
            ctx.Response.Close();
        }
    }

    private sealed record CapturedPost(string Method, string Url, string Path, string? ContentType, string Body);
}
