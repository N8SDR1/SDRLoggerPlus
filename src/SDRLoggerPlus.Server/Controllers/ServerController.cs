using System.Net.Http.Headers;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Mvc;
using SDRLoggerPlus.Server.Core.Database;
using SDRLoggerPlus.Server.Core.Security;
using SDRLoggerPlus.Server.Services;

namespace SDRLoggerPlus.Server.Controllers;

/// <summary>
/// Powers Settings → Server — the multi-op "easy connect" front door. Two roles:
///   • HOST this log: bind to the LAN, hand out per-device tokens, show the address clients dial.
///   • CONNECT to a host: point this station's shared log at a host URL + token.
/// Provider/host changes are persisted to UserConfig and take effect on the next backend start
/// (the launcher restarts it), so responses flag <c>RestartRequired</c>.
/// </summary>
[ApiController]
[Route("api/server")]
public class ServerController : ControllerBase
{
    private readonly IUserConfigService _config;
    private readonly AuthTokenStore _tokens;
    private readonly IHttpClientFactory _httpFactory;
    private readonly IServer _server;
    private readonly ILogger<ServerController> _log;

    public ServerController(IUserConfigService config, AuthTokenStore tokens,
        IHttpClientFactory httpFactory, IServer server, ILogger<ServerController> log)
    {
        _config = config;
        _tokens = tokens;
        _httpFactory = httpFactory;
        _server = server;
        _log = log;
    }

    public record ServerConfigDto(
        string Mode, string? HostUrl, bool RemotelyBound,
        IReadOnlyList<string> LanAddresses, int Port, int DeviceCount);

    public record SaveConfigRequest(string Mode, string? HostUrl, string? Token);
    public record TestRequest(string HostUrl, string? Token);
    public record TestResult(bool Ok, string Detail);
    public record AddDeviceRequest(string Name);

    [HttpGet("config")]
    public async Task<ActionResult<ServerConfigDto>> GetConfig()
    {
        var cfg = await _config.GetConfigAsync();
        var addrs = BoundAddresses();
        return Ok(new ServerConfigDto(
            Mode: cfg.Provider == DatabaseProvider.RemoteHost ? "client" : "host",
            HostUrl: cfg.HostUrl,
            RemotelyBound: RemoteAccessGuard.IsRemotelyBound(addrs),
            LanAddresses: LanIPv4(),
            Port: PortOf(addrs),
            DeviceCount: _tokens.List().Count));
    }

    [HttpPost("config")]
    public async Task<ActionResult> SaveConfig([FromBody] SaveConfigRequest req)
    {
        var cfg = await _config.GetConfigAsync();
        if (string.Equals(req.Mode, "client", StringComparison.OrdinalIgnoreCase))
        {
            if (string.IsNullOrWhiteSpace(req.HostUrl))
                return BadRequest(new { error = "A host URL is required to connect as a client." });
            cfg.Provider = DatabaseProvider.RemoteHost;
            cfg.HostUrl = req.HostUrl.Trim();
            if (!string.IsNullOrWhiteSpace(req.Token)) cfg.HostToken = req.Token.Trim();
        }
        else
        {
            // Back to hosting our own log; keep the stored host URL/token for easy re-connect later.
            cfg.Provider = DatabaseProvider.Local;
        }
        await _config.SaveConfigAsync(cfg);
        return Ok(new { restartRequired = true });
    }

    /// <summary>Reach a candidate host and validate the token against a token-gated endpoint.</summary>
    [HttpPost("test")]
    public async Task<ActionResult<TestResult>> Test([FromBody] TestRequest req)
    {
        if (string.IsNullOrWhiteSpace(req.HostUrl))
            return Ok(new TestResult(false, "Enter the host address first."));
        var baseUrl = req.HostUrl.Trim().TrimEnd('/');
        try
        {
            var client = _httpFactory.CreateClient();
            client.Timeout = TimeSpan.FromSeconds(8);
            // /api/data/qsos/count is token-gated when the host enforces auth, so a 200 proves BOTH
            // reachability and a valid token; 401 = reachable but wrong/missing token.
            using var msg = new HttpRequestMessage(HttpMethod.Get, $"{baseUrl}/api/data/qsos/count");
            if (!string.IsNullOrWhiteSpace(req.Token))
                msg.Headers.Authorization = new AuthenticationHeaderValue("Bearer", req.Token.Trim());
            var res = await client.SendAsync(msg);
            return res.StatusCode switch
            {
                System.Net.HttpStatusCode.OK => Ok(new TestResult(true, "Connected — the host answered and the token is valid.")),
                System.Net.HttpStatusCode.Unauthorized => Ok(new TestResult(false, "Reached the host, but the token was rejected. Check the token.")),
                var s => Ok(new TestResult(false, $"Reached the host but it returned {(int)s} {s}."))
            };
        }
        catch (Exception ex)
        {
            return Ok(new TestResult(false, $"Couldn't reach the host: {ex.Message}"));
        }
    }

    // -- host-mode device tokens (who may connect to this host) ---------------------------------

    [HttpGet("devices")]
    public ActionResult<IReadOnlyList<AuthDeviceInfo>> Devices() => Ok(_tokens.List());

    [HttpPost("devices")]
    public ActionResult AddDevice([FromBody] AddDeviceRequest req)
    {
        var (token, device) = _tokens.Issue(string.IsNullOrWhiteSpace(req.Name) ? "device" : req.Name);
        // The plaintext token is returned ONCE — the client copies it now; only its hash is stored.
        return Ok(new { token, device });
    }

    [HttpDelete("devices/{id}")]
    public ActionResult RevokeDevice(string id) => Ok(new { removed = _tokens.Revoke(id) });

    // -- helpers --------------------------------------------------------------------------------

    private string[] BoundAddresses() =>
        _server.Features.Get<IServerAddressesFeature>()?.Addresses.ToArray() ?? Array.Empty<string>();

    private static int PortOf(IEnumerable<string> addresses)
    {
        foreach (var a in addresses)
            if (Uri.TryCreate(a.Replace("0.0.0.0", "localhost").Replace("+", "localhost").Replace("*", "localhost"),
                    UriKind.Absolute, out var u) && u.Port > 0)
                return u.Port;
        return 0;
    }

    /// <summary>This machine's LAN IPv4 addresses — what a client on the same network dials.</summary>
    private static List<string> LanIPv4()
    {
        var result = new List<string>();
        foreach (var ni in NetworkInterface.GetAllNetworkInterfaces())
        {
            if (ni.OperationalStatus != OperationalStatus.Up) continue;
            if (ni.NetworkInterfaceType is NetworkInterfaceType.Loopback or NetworkInterfaceType.Tunnel) continue;
            foreach (var ip in ni.GetIPProperties().UnicastAddresses)
                if (ip.Address.AddressFamily == AddressFamily.InterNetwork
                    && !System.Net.IPAddress.IsLoopback(ip.Address))
                    result.Add(ip.Address.ToString());
        }
        return result.Distinct().ToList();
    }
}
