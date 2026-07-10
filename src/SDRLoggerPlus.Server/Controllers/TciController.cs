using System.Net.WebSockets;
using Microsoft.AspNetCore.Mvc;

namespace SDRLoggerPlus.Server.Controllers;

[ApiController]
[Route("api/tci")]
public class TciController : ControllerBase
{
    public record TestTciRequest(string Host, int Port);
    public record TestTciResponse(bool Ok, string? Error);

    /// <summary>
    /// Probe a TCI server before the operator saves the rig. TCI runs over a
    /// WebSocket (ws://host:port), so we attempt the handshake with a short
    /// timeout and close immediately. A wrong port fails fast here instead of
    /// leaving a saved rig that can never connect.
    /// </summary>
    [HttpPost("test-connection")]
    public async Task<ActionResult<TestTciResponse>> TestConnection([FromBody] TestTciRequest req)
    {
        if (string.IsNullOrWhiteSpace(req.Host))
            return Ok(new TestTciResponse(false, "Enter a host or IP address."));
        if (req.Port is <= 0 or > 65535)
            return Ok(new TestTciResponse(false, "Port must be between 1 and 65535."));

        using var ws = new ClientWebSocket();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(4));
        try
        {
            await ws.ConnectAsync(new Uri($"ws://{req.Host}:{req.Port}"), cts.Token);
            // A TCI (WebSocket) server answered the handshake. Close politely.
            try
            {
                await ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "probe", CancellationToken.None);
            }
            catch { /* the probe already succeeded; closing is best-effort */ }
            return Ok(new TestTciResponse(true, null));
        }
        catch (OperationCanceledException)
        {
            return Ok(new TestTciResponse(false,
                $"No response from {req.Host}:{req.Port} within 4 s — is the radio running, and is this the right TCI port? (Setup → CAT/TCI)"));
        }
        catch (Exception ex)
        {
            var detail = ex is WebSocketException we ? we.Message : ex.Message;
            return Ok(new TestTciResponse(false,
                $"Nothing responded at {req.Host}:{req.Port} — check the radio's TCI port (Setup → CAT/TCI). [{detail}]"));
        }
    }
}
