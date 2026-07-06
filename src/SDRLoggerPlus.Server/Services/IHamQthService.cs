namespace SDRLoggerPlus.Server.Services;

/// <summary>
/// HamQTH.com XML-API callbook lookup. Used as a secondary lookup source in
/// the LogHub callsign lookup chain (QRZ → HamQTH → cty.dat centroid).
///
/// HamQTH is a free account service. The XML API is session-based: an initial
/// login gets a session_id which is cached in memory and reused across
/// lookups. On session expiry (~1h idle) we re-login transparently.
/// See https://www.hamqth.com/developers.php for API details.
/// </summary>
public interface IHamQthService
{
    /// <summary>
    /// Lookup a callsign via HamQTH. Returns null when: HamQTH is disabled
    /// or unconfigured, the callsign isn't in HamQTH, or the request fails.
    /// </summary>
    Task<HamQthCallsignInfo?> LookupCallsignAsync(string callsign, CancellationToken cancellationToken = default);

    /// <summary>
    /// Test credentials by attempting a fresh XML-API login. Bypasses the
    /// cached session so the test truly proves the supplied username/
    /// password work right now (not "worked an hour ago"). Powers the
    /// "Test Credentials" button in Settings → HamQTH.
    /// </summary>
    Task<HamQthTestResult> TestCredentialsAsync(string username, string password, CancellationToken cancellationToken = default);
}

/// <summary>
/// Outcome of a HamQTH credentials-test round trip.
/// </summary>
public record HamQthTestResult(bool Success, string Message);

/// <summary>
/// Subset of the HamQTH XML response used by the callsign lookup chain.
/// Field names mirror QrzCallsignInfo so LogHub can treat both sources
/// uniformly. HamQTH always exposes lat/lon on hit — that's the whole
/// reason it's in the fallback chain.
/// </summary>
public record HamQthCallsignInfo(
    string Callsign,
    string? Name,
    string? FirstName,
    string? Country,
    string? Grid,
    double? Latitude,
    double? Longitude,
    int? CqZone,
    int? ItuZone,
    string? State,
    string? ImageUrl
);
