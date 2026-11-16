using System.Net;
using System.Text.Json;
using FluentAssertions;
using SDRLoggerPlus.Server.Services.Qrv;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

[Trait("Category", "Unit")]
public class QrvPresenceClientTests
{
    private const string InstallKey = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    private static readonly string RosterJson = """
        {"ok":true,"stale_s":180,"entries":[
          {"call":"N8SDR","app":"Logger","band":"20m","mode":"FT8","last_seen":"2026-11-15T23:04:05Z"},
          {"call":"KC8TYK","app":"Lyra","band":"40m","mode":"LSB","last_seen":"2026-11-15T23:03:44Z","hz":7150000}
        ]}
        """;

    private sealed class FakeHandler : HttpMessageHandler
    {
        public List<(string Method, string Url, string Body)> Requests { get; } = new();
        public Func<HttpRequestMessage, string, HttpResponseMessage>? Responder;

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var body = request.Content == null ? "" : await request.Content.ReadAsStringAsync(cancellationToken);
            Requests.Add((request.Method.Method, request.RequestUri!.ToString(), body));
            return Responder?.Invoke(request, body) ?? Json(HttpStatusCode.OK, """{"ok":true}""");
        }
    }

    private static HttpResponseMessage Json(HttpStatusCode status, string body) =>
        new(status) { Content = new StringContent(body) };

    private static (QrvPresenceClient Client, FakeHandler Handler, Func<DateTimeOffset> Advance) Create()
    {
        var now = new DateTimeOffset(2026, 11, 16, 12, 0, 0, TimeSpan.Zero);
        var handler = new FakeHandler();
        var client = new QrvPresenceClient(new HttpClient(handler), clock: () => now);
        return (client, handler, () => now);
    }

    private static void Advance(ref DateTimeOffset now, TimeSpan by) => now = now.Add(by);

    [Fact]
    public async Task Disabled_SendsNoRequests()
    {
        var now = new DateTimeOffset(2026, 11, 16, 12, 0, 0, TimeSpan.Zero);
        var handler = new FakeHandler();
        var client = new QrvPresenceClient(new HttpClient(handler), clock: () => now);

        client.ApplySettings(false, "https://qrvsdr.cc/v1", "N8SDR", null, null);
        client.ReportLoggingMode("FT8", "radio-1");
        client.SetRadio(true, 14_074_000, "DIGU");
        await client.TickAsync(CancellationToken.None);
        now = now.AddSeconds(30);
        await client.TickAsync(CancellationToken.None);
        await client.ShutdownAsync(CancellationToken.None);

        handler.Requests.Should().BeEmpty();
        client.Status().Phase.Should().Be("off");
        client.Status().Enabled.Should().BeFalse();
    }

    [Fact]
    public async Task Heartbeat_UsesLoggerBadgeAndSection11Fields_WithoutHzOrNote()
    {
        var now = new DateTimeOffset(2026, 11, 16, 12, 0, 0, TimeSpan.Zero);
        var handler = new FakeHandler();
        handler.Responder = (_, _) => Json(HttpStatusCode.OK,
            "{\"ok\":true,\"stale_s\":180,\"install_key\":\"" + InstallKey + "\"}");
        var client = new QrvPresenceClient(new HttpClient(handler), clock: () => now);

        client.ApplySettings(true, "https://qrvsdr.cc/v1", "n8sdr", null, null);
        client.ReportLoggingMode("FT8", null);
        client.SetRadio(true, 14_074_000, "DIGU");
        await client.TickAsync(CancellationToken.None);

        handler.Requests.Should().ContainSingle();
        var posted = handler.Requests[0];
        posted.Method.Should().Be("POST");
        posted.Url.Should().Be("https://qrvsdr.cc/v1/heartbeat.php");
        using (var doc = JsonDocument.Parse(posted.Body))
        {
            var root = doc.RootElement;
            root.GetProperty("call").GetString().Should().Be("N8SDR");
            root.GetProperty("app").GetString().Should().Be("Logger");
            root.GetProperty("band").GetString().Should().Be("20m");
            root.GetProperty("mode").GetString().Should().Be("FT8");
            root.TryGetProperty("install_key", out _).Should().BeFalse();
            root.TryGetProperty("hz", out _).Should().BeFalse();
            root.TryGetProperty("note", out _).Should().BeFalse();
        }

        client.Status().Phase.Should().Be("on-air");
        client.Status().InstallKeyPrefix.Should().Be("01234567...");

        now = now.AddSeconds(25);
        await client.TickAsync(CancellationToken.None);
        handler.Requests.Should().HaveCount(2);
        using (var doc = JsonDocument.Parse(handler.Requests[1].Body))
        {
            doc.RootElement.GetProperty("app").GetString().Should().Be("Logger");
            doc.RootElement.GetProperty("install_key").GetString().Should().Be(InstallKey);
            doc.RootElement.TryGetProperty("hz", out _).Should().BeFalse();
        }
    }

    [Fact]
    public async Task Disable_SendsQrtForLogger()
    {
        var (client, handler) = await GoOnAir();
        client.ApplySettings(false, "https://qrvsdr.cc/v1", "N8SDR", null, null);
        await client.TickAsync(CancellationToken.None);

        var qrt = handler.Requests.Should().ContainSingle(r => r.Url.EndsWith("/v1/qrt.php")).Subject;
        qrt.Method.Should().Be("POST");
        using var doc = JsonDocument.Parse(qrt.Body);
        doc.RootElement.GetProperty("call").GetString().Should().Be("N8SDR");
        doc.RootElement.GetProperty("app").GetString().Should().Be("Logger");
        doc.RootElement.GetProperty("install_key").GetString().Should().Be(InstallKey);
        doc.RootElement.TryGetProperty("band", out _).Should().BeFalse();
        doc.RootElement.TryGetProperty("hz", out _).Should().BeFalse();
        client.Status().Phase.Should().Be("off");
        client.Status().OnAir.Should().BeFalse();
    }

    [Fact]
    public async Task Shutdown_SendsQrt()
    {
        var (client, handler) = await GoOnAir();
        await client.ShutdownAsync(CancellationToken.None);

        handler.Requests.Should().Contain(r => r.Url.EndsWith("/v1/qrt.php") && r.Body.Contains("\"app\":\"Logger\""));
    }

    [Fact]
    public async Task RadioDisconnect_SendsQrt_AndDoesNotInventAFrequency()
    {
        var (client, handler) = await GoOnAir();
        var before = handler.Requests.Count;
        client.SetRadio(false, 0, null);
        await client.TickAsync(CancellationToken.None);

        handler.Requests.Skip(before).Should().ContainSingle(r => r.Url.EndsWith("/v1/qrt.php"));
        handler.Requests.Skip(before).Should().NotContain(r => r.Url.Contains("heartbeat"));
        client.Status().OnAir.Should().BeFalse();
        client.Status().Detail.Should().Contain("not connected");
    }

    [Fact]
    public async Task Roster_ParsesEntries_AndKeepsThemWhenTheNextGetFails()
    {
        var now = new DateTimeOffset(2026, 11, 16, 12, 0, 0, TimeSpan.Zero);
        var handler = new FakeHandler();
        var gets = 0;
        handler.Responder = (req, _) =>
        {
            if (req.Method == HttpMethod.Get)
            {
                gets++;
                if (gets == 1) return Json(HttpStatusCode.OK, RosterJson);
                return Json(HttpStatusCode.BadGateway, """{"ok":false,"error":"http_502"}""");
            }
            return Json(HttpStatusCode.OK, "{\"ok\":true,\"install_key\":\"" + InstallKey + "\"}");
        };
        var client = new QrvPresenceClient(new HttpClient(handler), clock: () => now);
        client.ApplySettings(false, "https://qrvsdr.cc/v1", "N8SDR", null, null);
        client.SetRosterWatching(true);
        await client.TickAsync(CancellationToken.None);

        var status = client.Status();
        status.Roster.Should().HaveCount(2);
        status.Roster[0].Call.Should().Be("N8SDR");
        status.Roster[0].App.Should().Be("Logger");
        status.Roster[0].Mode.Should().Be("FT8");
        status.Roster[1].Hz.Should().Be(7_150_000);
        status.Error.Should().BeNull();

        now = now.AddSeconds(25);
        await client.TickAsync(CancellationToken.None);

        var after = client.Status();
        after.Roster.Should().HaveCount(2);
        after.Roster[0].Call.Should().Be("N8SDR");
        after.Roster[1].Mode.Should().Be("LSB");
        after.Error.Should().NotBeNullOrEmpty();
        after.Phase.Should().Be("off");
    }

    [Fact]
    public async Task InvalidFields_SurfacesTheError_AndKeepsTheLastGoodRosterAndKey()
    {
        var now = new DateTimeOffset(2026, 11, 16, 12, 0, 0, TimeSpan.Zero);
        var handler = new FakeHandler();
        handler.Responder = (req, _) =>
        {
            if (req.Method == HttpMethod.Get)
                return Json(HttpStatusCode.OK, RosterJson);
            var heartbeats = handler.Requests.Count(r => r.Url.Contains("heartbeat.php", StringComparison.Ordinal));
            if (heartbeats >= 2)
                return Json(HttpStatusCode.BadRequest, """{"ok":false,"error":"invalid_fields"}""");
            return Json(HttpStatusCode.OK, "{\"ok\":true,\"stale_s\":180,\"install_key\":\"" + InstallKey + "\"}");
        };
        var client = new QrvPresenceClient(new HttpClient(handler), clock: () => now);
        client.ApplySettings(true, "https://qrvsdr.cc/v1", "N8SDR", null, null);
        client.ReportLoggingMode("FT8", null);
        client.SetRadio(true, 14_074_000, "USB");
        client.SetRosterWatching(true);

        await client.TickAsync(CancellationToken.None);
        client.Status().Roster.Should().HaveCount(2);
        client.Status().InstallKeyPrefix.Should().Be("01234567...");

        now = now.AddSeconds(25);
        var act = async () => await client.TickAsync(CancellationToken.None);
        await act.Should().NotThrowAsync();

        var failed = client.Status();
        failed.Error.Should().Be("invalid_fields");
        failed.Phase.Should().Be("error");
        failed.Roster.Should().HaveCount(2);
        failed.Roster[0].Mode.Should().Be("FT8");
        failed.InstallKeyPrefix.Should().Be("01234567...");
        failed.Call.Should().Be("N8SDR");
        failed.Band.Should().Be("20m");
        failed.Mode.Should().Be("FT8");

        var count = handler.Requests.Count;
        await client.TickAsync(CancellationToken.None);
        handler.Requests.Should().HaveCount(count);
    }

    [Theory]
    [InlineData("USB", "USB")]
    [InlineData("LSB", "LSB")]
    [InlineData("SSB", "SSB")]
    [InlineData("CWU", "CWU")]
    [InlineData("CWL", "CWL")]
    [InlineData("CW", "CW")]
    [InlineData("CW-R", "CWL")]
    [InlineData("AM", "AM")]
    [InlineData("SAM", "SAM")]
    [InlineData("FM", "FM")]
    [InlineData("NFM", "NFM")]
    [InlineData("DIGU", "DIGU")]
    [InlineData("DIGL", "DIGL")]
    [InlineData("DATA-U", "DIGU")]
    [InlineData("FT8", "FT8")]
    [InlineData("ft8", "FT8")]
    [InlineData("FT4", "FT4")]
    [InlineData("JS8", "JS8")]
    [InlineData("RTTY", "RTTY")]
    [InlineData("RTTY-R", "RTTYR")]
    [InlineData("RTTYR", "RTTYR")]
    [InlineData("PSK31", "PSK31")]
    [InlineData("WSPR", "WSPR")]
    [InlineData("JT65", "JT65")]
    [InlineData("JT9", "JT9")]
    [InlineData("DIGI", "DIGI")]
    [InlineData("DIG", "DIG")]
    [InlineData("DATA", "DATA")]
    [InlineData("VARAC", "VARAC")]
    [InlineData("PACKET", "PACKET")]
    [InlineData("MSK144", "MSK144")]
    [InlineData("Q65", "Q65")]
    [InlineData("FST4", "FST4")]
    [InlineData("FST4W", "FST4W")]
    [InlineData("OLIVIA", "OLIVIA")]
    [InlineData("HELL", "HELL")]
    [InlineData("DSB", "DSB")]
    [InlineData("SPEC", "SPEC")]
    [InlineData("DRM", "DRM")]
    [InlineData("FSK", "RTTY")]
    [InlineData("SSTV", null)]
    [InlineData("", null)]
    public void ModeForLogger_MatchesTheContract(string input, string? expected)
    {
        QrvProtocol.ModeForLogger(input).Should().Be(expected);
    }

    [Theory]
    [InlineData(14_074_000, "20m")]
    [InlineData(7_250_000, "40m")]
    [InlineData(1_000_000, "GEN")]
    [InlineData(50_000_000, "6m")]
    [InlineData(0, null)]
    [InlineData(222_000_000, null)]
    public void BandForHz_PrefersAHamBandAndDoesNotInventOne(long hz, string? expected)
    {
        QrvProtocol.BandForHz(hz).Should().Be(expected);
    }

    [Fact]
    public void NormalizeApiBase_AcceptsTheConfiguredV1Root()
    {
        QrvProtocol.EndpointUrl("https://qrvsdr.cc/v1", "heartbeat.php")
            .Should().Be("https://qrvsdr.cc/v1/heartbeat.php");
        QrvProtocol.EndpointUrl("https://qrvsdr.cc", "qrt.php")
            .Should().Be("https://qrvsdr.cc/v1/qrt.php");
    }

    private static async Task<(QrvPresenceClient Client, FakeHandler Handler)> GoOnAir()
    {
        var now = new DateTimeOffset(2026, 11, 16, 12, 0, 0, TimeSpan.Zero);
        var handler = new FakeHandler();
        handler.Responder = (_, _) => Json(HttpStatusCode.OK,
            "{\"ok\":true,\"stale_s\":180,\"install_key\":\"" + InstallKey + "\"}");
        var client = new QrvPresenceClient(new HttpClient(handler), clock: () => now);
        client.ApplySettings(true, "https://qrvsdr.cc/v1", "N8SDR", null, null);
        client.ReportLoggingMode("FT8", null);
        client.SetRadio(true, 14_074_000, "USB");
        await client.TickAsync(CancellationToken.None);
        return (client, handler);
    }
}
