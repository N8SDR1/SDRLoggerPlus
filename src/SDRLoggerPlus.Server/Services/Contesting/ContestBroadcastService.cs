using System.Net.Sockets;
using System.Text;
using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Contracts.Models.Contesting;

namespace SDRLoggerPlus.Server.Services.Contesting;

/// <summary>
/// Fire-and-forget outbound interop: N1MM-compatible UDP datagrams and online
/// score HTTP posts. All sends are best-effort — a failure is logged and never
/// propagates into the logging path. Opt-in via <see cref="ContestSettings"/>.
/// </summary>
public class ContestBroadcastService
{
    private readonly IHttpClientFactory _httpFactory;
    private readonly ILogger<ContestBroadcastService> _logger;

    public ContestBroadcastService(
        IHttpClientFactory httpFactory,
        ILogger<ContestBroadcastService> logger)
    {
        _httpFactory = httpFactory;
        _logger = logger;
    }

    // The caller passes a settings snapshot (fetched in-request) so this singleton
    // holds no scoped dependency and is safe to run after the request scope ends.

    /// <summary>Broadcast a logged QSO (contactinfo) + the fresh score, per settings.</summary>
    public async Task OnQsoLoggedAsync(ContestSettings cfg, ContestDefinition def, ContestSession session, Qso qso, ContestStateDto state, string myCall)
    {
        if (cfg.N1mmUdpEnabled)
        {
            SendUdp(cfg, N1mmXmlBuilder.ContactInfo(def, session, qso, myCall));
            SendUdp(cfg, N1mmXmlBuilder.DynamicResults(def, state, myCall));
        }
        if (cfg.OnlineScoreEnabled)
            await PostScoreAsync(cfg, N1mmXmlBuilder.DynamicResults(def, state, myCall));
    }

    /// <summary>Broadcast just the score (e.g. on periodic update / session change).</summary>
    public async Task OnScoreAsync(ContestSettings cfg, ContestDefinition def, ContestStateDto state, string myCall)
    {
        var xml = N1mmXmlBuilder.DynamicResults(def, state, myCall);
        if (cfg.N1mmUdpEnabled) SendUdp(cfg, xml);
        if (cfg.OnlineScoreEnabled) await PostScoreAsync(cfg, xml);
    }

    private void SendUdp(ContestSettings cfg, string xml)
    {
        try
        {
            using var client = new UdpClient();
            var bytes = Encoding.UTF8.GetBytes(xml);
            client.Send(bytes, bytes.Length, cfg.N1mmUdpHost, cfg.N1mmUdpPort);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "N1MM UDP broadcast to {Host}:{Port} failed", cfg.N1mmUdpHost, cfg.N1mmUdpPort);
        }
    }

    private async Task PostScoreAsync(ContestSettings cfg, string xml)
    {
        try
        {
            var http = _httpFactory.CreateClient();
            http.Timeout = TimeSpan.FromSeconds(10);
            using var content = new StringContent(xml, Encoding.UTF8, "text/xml");
            var resp = await http.PostAsync(cfg.OnlineScoreUrl, content);
            if (!resp.IsSuccessStatusCode)
                _logger.LogWarning("Online score post to {Url} returned {Status}", cfg.OnlineScoreUrl, resp.StatusCode);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Online score post to {Url} failed", cfg.OnlineScoreUrl);
        }
    }
}
