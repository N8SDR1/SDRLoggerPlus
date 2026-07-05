using System.Xml.Linq;
using SDRLoggerPlus.Server.Core.Database;

namespace SDRLoggerPlus.Server.Services;

/// <summary>
/// HamQTH.com XML API client. Session-based:
///   1. GET https://www.hamqth.com/xml.php?u=USER&p=PASS → session_id
///   2. GET https://www.hamqth.com/xml.php?id=SESSION&callsign=CALL&prg=SDRLoggerPlus
///
/// The session_id lasts about an hour of idle; when we see the
/// "Session does not exist or is not valid" error we invalidate the
/// cached id and retry once.
/// </summary>
public class HamQthService : IHamQthService
{
    private const string BaseUrl = "https://www.hamqth.com/xml.php";
    private const string XmlNs = "https://www.hamqth.com";
    private const string ProgramName = "SDRLoggerPlus"; // sent with every lookup for HamQTH's usage stats

    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ISettingsRepository _settingsRepository;
    private readonly ILogger<HamQthService> _logger;

    // Cached session — protected by _sessionLock so a burst of concurrent
    // lookups on a cold cache all reuse the same login round-trip.
    private string? _sessionId;
    private string? _sessionForUsername;
    private DateTime _sessionAcquiredUtc;
    private readonly SemaphoreSlim _sessionLock = new(1, 1);

    // HamQTH sessions are documented as ~1 hour idle timeout; we refresh
    // proactively at 45 min to avoid the invalidation-retry round trip.
    private static readonly TimeSpan SessionMaxAge = TimeSpan.FromMinutes(45);

    public HamQthService(
        IHttpClientFactory httpClientFactory,
        ISettingsRepository settingsRepository,
        ILogger<HamQthService> logger)
    {
        _httpClientFactory = httpClientFactory;
        _settingsRepository = settingsRepository;
        _logger = logger;
    }

    public async Task<HamQthCallsignInfo?> LookupCallsignAsync(string callsign, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(callsign)) return null;

        var settings = await _settingsRepository.GetAsync();
        var hq = settings?.HamQth;
        if (hq is null || !hq.Enabled) return null;
        if (string.IsNullOrWhiteSpace(hq.Username) || string.IsNullOrWhiteSpace(hq.Password))
        {
            _logger.LogDebug("HamQTH lookup skipped: credentials not set");
            return null;
        }

        try
        {
            // First attempt with whatever session we have cached.
            var sessionId = await GetOrCreateSessionAsync(hq.Username!, hq.Password!, cancellationToken);
            if (sessionId is null) return null;

            var result = await DoLookupAsync(sessionId, callsign, cancellationToken);
            if (result.SessionExpired)
            {
                _logger.LogDebug("HamQTH session expired for {Callsign}; refreshing", callsign);
                InvalidateSession();
                sessionId = await GetOrCreateSessionAsync(hq.Username!, hq.Password!, cancellationToken);
                if (sessionId is null) return null;
                result = await DoLookupAsync(sessionId, callsign, cancellationToken);
            }
            return result.Info;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "HamQTH lookup failed for {Callsign}", callsign);
            return null;
        }
    }

    private async Task<string?> GetOrCreateSessionAsync(string username, string password, CancellationToken cancellationToken)
    {
        // Fast path: reuse a still-fresh session for the same user.
        if (_sessionId is not null
            && _sessionForUsername == username
            && DateTime.UtcNow - _sessionAcquiredUtc < SessionMaxAge)
        {
            return _sessionId;
        }

        await _sessionLock.WaitAsync(cancellationToken);
        try
        {
            // Recheck inside the lock — another waiter may have refreshed while we blocked.
            if (_sessionId is not null
                && _sessionForUsername == username
                && DateTime.UtcNow - _sessionAcquiredUtc < SessionMaxAge)
            {
                return _sessionId;
            }

            var http = _httpClientFactory.CreateClient();
            http.Timeout = TimeSpan.FromSeconds(15);

            var loginUrl = $"{BaseUrl}?u={Uri.EscapeDataString(username)}&p={Uri.EscapeDataString(password)}";
            using var resp = await http.GetAsync(loginUrl, cancellationToken);
            if (!resp.IsSuccessStatusCode)
            {
                _logger.LogWarning("HamQTH login failed: HTTP {Status}", (int)resp.StatusCode);
                return null;
            }

            var xml = await resp.Content.ReadAsStringAsync(cancellationToken);
            var doc = XDocument.Parse(xml);
            XNamespace ns = XmlNs;

            var session = doc.Root?.Element(ns + "session");
            if (session is null)
            {
                _logger.LogWarning("HamQTH login: no <session> element");
                return null;
            }

            var error = session.Element(ns + "error")?.Value;
            if (!string.IsNullOrEmpty(error))
            {
                // Login errors are usually bad credentials — log once at Warning
                // level so operators can spot them, then bail out.
                _logger.LogWarning("HamQTH login rejected: {Error}", error);
                return null;
            }

            var id = session.Element(ns + "session_id")?.Value;
            if (string.IsNullOrEmpty(id))
            {
                _logger.LogWarning("HamQTH login: no session_id in response");
                return null;
            }

            _sessionId = id;
            _sessionForUsername = username;
            _sessionAcquiredUtc = DateTime.UtcNow;
            _logger.LogInformation("HamQTH session established for {Username}", username);
            return id;
        }
        finally
        {
            _sessionLock.Release();
        }
    }

    private void InvalidateSession()
    {
        _sessionId = null;
        _sessionForUsername = null;
    }

    private async Task<(HamQthCallsignInfo? Info, bool SessionExpired)> DoLookupAsync(
        string sessionId, string callsign, CancellationToken cancellationToken)
    {
        var http = _httpClientFactory.CreateClient();
        http.Timeout = TimeSpan.FromSeconds(15);

        var url = $"{BaseUrl}?id={Uri.EscapeDataString(sessionId)}"
                  + $"&callsign={Uri.EscapeDataString(callsign)}"
                  + $"&prg={Uri.EscapeDataString(ProgramName)}";

        using var resp = await http.GetAsync(url, cancellationToken);
        if (!resp.IsSuccessStatusCode)
        {
            _logger.LogDebug("HamQTH lookup HTTP {Status} for {Callsign}", (int)resp.StatusCode, callsign);
            return (null, false);
        }

        var xml = await resp.Content.ReadAsStringAsync(cancellationToken);
        var doc = XDocument.Parse(xml);
        XNamespace ns = XmlNs;

        // Session-expiry response has a <session><error>...</error></session>
        // subtree — same shape as login errors, but at query time it means we
        // need to re-login and try again.
        var sessionElem = doc.Root?.Element(ns + "session");
        if (sessionElem?.Element(ns + "error") is { } sessionError)
        {
            var msg = sessionError.Value ?? string.Empty;
            var expired = msg.Contains("session", StringComparison.OrdinalIgnoreCase);
            if (!expired)
                _logger.LogDebug("HamQTH lookup error for {Callsign}: {Error}", callsign, msg);
            return (null, expired);
        }

        var search = doc.Root?.Element(ns + "search");
        if (search is null) return (null, false);

        string? Get(string name) => search.Element(ns + name)?.Value?.Trim();
        static double? ParseDouble(string? s)
            => double.TryParse(s, System.Globalization.NumberStyles.Float,
                System.Globalization.CultureInfo.InvariantCulture, out var v) ? v : null;
        static int? ParseInt(string? s) => int.TryParse(s, out var v) ? v : null;

        var lat = ParseDouble(Get("latitude"));
        var lon = ParseDouble(Get("longitude"));

        // HamQTH splits the name into <adr_name> (full) and <nick>. We surface
        // the full-name value as Name, and use <nick> as FirstName so
        // BuildFullName in LogHub composes the display string the same way it
        // does for QRZ results.
        var fullName = Get("adr_name");
        var nick = Get("nick");

        var info = new HamQthCallsignInfo(
            Callsign: Get("callsign") ?? callsign.ToUpperInvariant(),
            Name: fullName ?? nick,
            FirstName: nick,
            Country: Get("country"),
            Grid: Get("grid"),
            Latitude: lat,
            Longitude: lon,
            CqZone: ParseInt(Get("cq")),
            ItuZone: ParseInt(Get("itu")),
            State: Get("us_state") ?? Get("qsl_via"), // us_state is the usual HamQTH tag
            ImageUrl: Get("picture")
        );
        return (info, false);
    }
}
