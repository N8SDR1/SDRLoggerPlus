namespace SDRLoggerPlus.Server.Core.Security;

/// <summary>Whether token auth is enforced this run — decided once at startup by <see cref="RemoteAccessGuard"/>.</summary>
public sealed record AuthOptions(bool Enforce);

/// <summary>
/// Bearer-token gate for the API and SignalR hub when the backend is reachable
/// remotely. Sits between CORS and endpoint routing.
///
/// Enforcement is decided ONCE at startup (<see cref="RemoteAccessGuard"/>): on a
/// loopback-only desktop this middleware is registered with <c>enforce = false</c>
/// and every request passes straight through, so nothing changes for the local user.
/// When enforced, protected requests must carry a valid device token:
///   • HTTP:      <c>Authorization: Bearer &lt;token&gt;</c>
///   • SignalR/WS: <c>?access_token=&lt;token&gt;</c> (the standard hub pattern — a
///     browser WebSocket can't set the Authorization header).
///
/// Always exempt: the SPA static assets + fallback (harmless files; the app shell must
/// load so it can supply a token) and <c>/api/health</c> (for reverse-proxy health
/// checks). Everything under <c>/api</c> and <c>/hubs</c> otherwise requires a token —
/// which is what stops an unauthenticated stranger from keying the transmitter.
/// </summary>
public sealed class TokenAuthMiddleware
{
    private readonly RequestDelegate _next;
    private readonly AuthTokenStore _store;
    private readonly bool _enforce;

    public TokenAuthMiddleware(RequestDelegate next, AuthTokenStore store, AuthOptions options)
    {
        _next = next;
        _store = store;
        _enforce = options.Enforce;
    }

    public async Task Invoke(HttpContext context)
    {
        if (!_enforce || !RequiresAuth(context.Request.Path))
        {
            await _next(context);
            return;
        }

        if (_store.Validate(ExtractToken(context)))
        {
            await _next(context);
            return;
        }

        context.Response.StatusCode = StatusCodes.Status401Unauthorized;
        context.Response.Headers["WWW-Authenticate"] = "Bearer";
        await context.Response.WriteAsJsonAsync(new { error = "Unauthorized", detail = "A valid device access token is required." });
    }

    /// <summary>Only the API and hub are gated; static assets and the health check are open.</summary>
    internal static bool RequiresAuth(PathString path)
    {
        if (path.StartsWithSegments("/api/health", StringComparison.OrdinalIgnoreCase)) return false;
        return path.StartsWithSegments("/api", StringComparison.OrdinalIgnoreCase)
            || path.StartsWithSegments("/hubs", StringComparison.OrdinalIgnoreCase);
    }

    private static string? ExtractToken(HttpContext context)
    {
        var header = context.Request.Headers.Authorization.ToString();
        if (!string.IsNullOrEmpty(header) && header.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase))
            return header["Bearer ".Length..].Trim();

        // SignalR / WebSocket connections carry the token in the query string.
        var q = context.Request.Query["access_token"].ToString();
        return string.IsNullOrEmpty(q) ? null : q;
    }
}
