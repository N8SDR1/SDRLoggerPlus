using System.Net;

namespace SDRLoggerPlus.Server.Core.Security;

/// <summary>
/// Decides WHEN token auth is enforced, and refuses an unsafe bind.
///
/// The rule that keeps the normal desktop zero-friction while making a remote
/// backend safe:
///   • Bound only to loopback (127.0.0.1 / ::1 / localhost) → auth is NOT enforced.
///     This is today's Electron desktop: no token, no login, nothing changes.
///   • Bound to any other address (0.0.0.0, a LAN IP, a hostname) → the backend is
///     reachable by other machines, so auth IS enforced — and because that backend
///     can key the transmitter over SignalR, we DEFAULT-DENY: refuse to start unless
///     at least one device token has been minted.
///
/// An operator can also force auth on a loopback bind with
/// <c>SDRLOGGERPLUS_REQUIRE_AUTH=true</c> (useful behind a reverse proxy that
/// forwards from localhost).
/// </summary>
public static class RemoteAccessGuard
{
    /// <summary>
    /// Whether the given bind URLs expose the backend beyond this machine.
    /// Empty/unparseable falls back to "remote" (fail safe — enforce), except the
    /// truly-empty case which means the host will use its default loopback binding.
    /// </summary>
    public static bool IsRemotelyBound(IEnumerable<string?> urls)
    {
        foreach (var raw in urls)
        {
            if (string.IsNullOrWhiteSpace(raw)) continue;
            foreach (var part in raw.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
                if (!IsLoopbackUrl(part)) return true;
        }
        // All configured URLs are loopback, or none are configured (Kestrel's default
        // bind is localhost) → not remotely reachable.
        return false;
    }

    private static bool IsLoopbackUrl(string url)
    {
        // Extract the host from forms like http://0.0.0.0:5050, https://host:443, http://[::1]:5050.
        var host = url;
        var scheme = url.IndexOf("://", StringComparison.Ordinal);
        if (scheme >= 0) host = host[(scheme + 3)..];
        // strip path
        var slash = host.IndexOf('/');
        if (slash >= 0) host = host[..slash];
        // IPv6 bracket form
        if (host.StartsWith('['))
        {
            var close = host.IndexOf(']');
            if (close > 0) host = host[1..close];
        }
        else
        {
            var colon = host.LastIndexOf(':');
            if (colon >= 0) host = host[..colon];
        }
        host = host.Trim();

        if (host.Equals("localhost", StringComparison.OrdinalIgnoreCase)) return true;
        // 0.0.0.0 / :: / a specific LAN IP or hostname are all "reachable by others".
        if (host is "0.0.0.0" or "*" or "+" or "::" ) return false;
        if (IPAddress.TryParse(host, out var ip)) return IPAddress.IsLoopback(ip);
        // A DNS hostname (not "localhost") is remote.
        return false;
    }

    /// <summary>Auth is enforced when bound remotely, or forced via env var.</summary>
    public static bool ShouldEnforce(IEnumerable<string?> bindUrls, bool forceRequire) =>
        forceRequire || IsRemotelyBound(bindUrls);

    /// <summary>
    /// Returns a fatal message if the configured bind is unsafe (remote + no tokens),
    /// otherwise null. The caller logs it and aborts startup.
    /// </summary>
    public static string? UnsafeBindReason(IEnumerable<string?> bindUrls, bool forceRequire, bool anyDevices)
    {
        if (!ShouldEnforce(bindUrls, forceRequire)) return null;
        if (anyDevices) return null;
        return "Refusing to start: the server is bound to a non-localhost address but no access " +
               "tokens exist, which would expose the API and radio control to the network without " +
               "authentication. Mint one first:  SDRLoggerPlus --auth-add-device \"my phone\"  " +
               "(or bind to localhost only). Override for a trusted reverse-proxy setup with care.";
    }
}
