using System.Net;
using System.Text;
using FluentAssertions;
using SDRLoggerPlus.Server.Services.Contesting;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

/// <summary>
/// S5b piece 1 — the multi-op client draws its contest serial from the HOST's atomic allocator.
/// Verifies RemoteHostSerialClient hits the right endpoint with {contestId, band} and parses the
/// serial. (The hard-locked reservation — GitHub #52 — is still outstanding; this is the routing.)
/// </summary>
[Trait("Category", "Unit")]
public class HostSerialClientTests
{
    private sealed class StubHandler : HttpMessageHandler
    {
        public string? LastPath;
        public string? LastBody;
        public required string ResponseJson;
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            LastPath = request.RequestUri!.AbsolutePath;
            LastBody = request.Content is null ? null : await request.Content.ReadAsStringAsync(ct);
            return new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(ResponseJson, Encoding.UTF8, "application/json"),
            };
        }
    }

    private static (RemoteHostSerialClient client, StubHandler handler) Make(string json)
    {
        var handler = new StubHandler { ResponseJson = json };
        var http = new HttpClient(handler) { BaseAddress = new Uri("http://host.local/") };
        return (new RemoteHostSerialClient(http), handler);
    }

    [Fact]
    public void LocalHostSerialClient_is_not_remote()
    {
        new LocalHostSerialClient().IsRemote.Should().BeFalse();
    }

    [Fact]
    public async Task Next_posts_to_serial_next_with_contest_and_band_and_returns_serial()
    {
        var (client, handler) = Make("{\"serial\":42,\"formatted\":\"042\"}");

        var serial = await client.NextAsync("cq-wpx-cw", "20m");

        client.IsRemote.Should().BeTrue();
        serial.Should().Be(42);
        handler.LastPath.Should().Be("/api/data/serial/next");
        handler.LastBody.Should().Contain("cq-wpx-cw").And.Contain("20m");
    }

    [Fact]
    public async Task Peek_posts_to_serial_peek()
    {
        var (client, handler) = Make("{\"serial\":7,\"formatted\":\"007\"}");

        var serial = await client.PeekAsync("arrl-ss-cw", null);

        serial.Should().Be(7);
        handler.LastPath.Should().Be("/api/data/serial/peek");
    }
}
