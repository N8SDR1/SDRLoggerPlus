using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Caching.Memory;
using System.Xml.Linq;

namespace SDRLoggerPlus.Server.Controllers;

/// <summary>
/// Proxies the PSK Reporter retrieve API for "who is hearing this callsign" reception
/// reports, used by the map overlay. PSK Reporter asks clients not to poll more often
/// than every few minutes per query, so results are cached per-callsign for 5 minutes —
/// the cache doubles as the rate-limit guard.
/// </summary>
[ApiController]
[Route("api/[controller]")]
[Produces("application/json")]
public class PskReporterController : ControllerBase
{
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ILogger<PskReporterController> _logger;
    private readonly IMemoryCache _cache;

    private static readonly TimeSpan CacheDuration = TimeSpan.FromMinutes(5);

    public PskReporterController(IHttpClientFactory httpClientFactory, ILogger<PskReporterController> logger, IMemoryCache cache)
    {
        _httpClientFactory = httpClientFactory;
        _logger = logger;
        _cache = cache;
    }

    /// <summary>
    /// Get reception reports for stations currently hearing <paramref name="callsign"/>
    /// (i.e. <paramref name="callsign"/> as the transmitter) over the last hour.
    /// </summary>
    [HttpGet("reports")]
    [ProducesResponseType(typeof(IEnumerable<PskReceptionReport>), StatusCodes.Status200OK)]
    public async Task<ActionResult<IEnumerable<PskReceptionReport>>> GetReports([FromQuery] string callsign)
    {
        if (string.IsNullOrWhiteSpace(callsign))
            return BadRequest("callsign is required");

        callsign = callsign.Trim().ToUpperInvariant();
        var cacheKey = $"pskreporter_{callsign}";

        if (_cache.TryGetValue(cacheKey, out List<PskReceptionReport>? cached) && cached is not null)
            return Ok(cached);

        try
        {
            var httpClient = _httpClientFactory.CreateClient();
            httpClient.Timeout = TimeSpan.FromSeconds(15);
            // PSK Reporter asks for a distinctive User-Agent so they can identify clients.
            httpClient.DefaultRequestHeaders.UserAgent.ParseAdd("SDRLoggerPlus/1.0");

            // senderCallsign => reports where this callsign was the transmitter (who heard it).
            // flowStartSeconds=-3600 => last hour. rronly=1 / noactive=1 trim the payload.
            var url = $"https://retrieve.pskreporter.info/query?senderCallsign={Uri.EscapeDataString(callsign)}" +
                      "&flowStartSeconds=-3600&rronly=1&noactive=1";

            var response = await httpClient.GetAsync(url);
            response.EnsureSuccessStatusCode();
            var xml = await response.Content.ReadAsStringAsync();

            var reports = ParseReports(xml);
            _cache.Set(cacheKey, reports, CacheDuration);
            return Ok(reports);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to fetch PSK Reporter data for {Callsign}", callsign);
            // Cache an empty result briefly so a failing/rate-limited upstream isn't hammered.
            var empty = new List<PskReceptionReport>();
            _cache.Set(cacheKey, empty, TimeSpan.FromMinutes(1));
            return Ok(empty);
        }
    }

    internal static List<PskReceptionReport> ParseReports(string xml)
    {
        var result = new List<PskReceptionReport>();
        if (string.IsNullOrWhiteSpace(xml)) return result;

        XDocument doc;
        try
        {
            doc = XDocument.Parse(xml);
        }
        catch
        {
            // Upstream occasionally returns truncated/non-XML (e.g. when rate-limited).
            return result;
        }

        foreach (var el in doc.Descendants().Where(e => e.Name.LocalName == "receptionReport"))
        {
            var senderLocator = Attr(el, "senderLocator");
            var receiverLocator = Attr(el, "receiverLocator");
            // Without both grids there's no path to draw on the map.
            if (string.IsNullOrEmpty(senderLocator) || string.IsNullOrEmpty(receiverLocator))
                continue;

            long.TryParse(Attr(el, "frequency"), out var freqHz);
            int.TryParse(Attr(el, "sNR"), out var snr);
            long.TryParse(Attr(el, "flowStartSeconds"), out var flowStart);

            result.Add(new PskReceptionReport
            {
                SenderCallsign = Attr(el, "senderCallsign"),
                SenderLocator = senderLocator,
                ReceiverCallsign = Attr(el, "receiverCallsign"),
                ReceiverLocator = receiverLocator,
                FrequencyHz = freqHz,
                Mode = Attr(el, "mode"),
                Snr = snr,
                FlowStartSeconds = flowStart,
            });
        }

        return result;
    }

    private static string Attr(XElement el, string name)
    {
        // PSK Reporter attribute names are case-sensitive but vary in casing across fields;
        // match on the local name case-insensitively to be safe.
        var attr = el.Attributes().FirstOrDefault(a =>
            string.Equals(a.Name.LocalName, name, StringComparison.OrdinalIgnoreCase));
        return attr?.Value ?? string.Empty;
    }
}

public class PskReceptionReport
{
    public string SenderCallsign { get; set; } = string.Empty;
    public string SenderLocator { get; set; } = string.Empty;
    public string ReceiverCallsign { get; set; } = string.Empty;
    public string ReceiverLocator { get; set; } = string.Empty;
    /// <summary>Reception frequency in Hz.</summary>
    public long FrequencyHz { get; set; }
    public string Mode { get; set; } = string.Empty;
    /// <summary>Signal-to-noise ratio in dB as reported by the receiver.</summary>
    public int Snr { get; set; }
    /// <summary>Unix epoch seconds of the reception.</summary>
    public long FlowStartSeconds { get; set; }
}
