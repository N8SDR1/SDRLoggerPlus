using System.Security.Cryptography;
using System.Text.Json;

namespace SDRLoggerPlus.Server.Core.Security;

/// <summary>
/// A registered client device with its own bearer token. Only the token's HASH
/// is stored — never the token itself — so reading the file (or a copied/backed-up
/// server disk) does not reveal a working credential. The hash is a plain SHA-256
/// of the token: that is sufficient because the token is 256 bits of cryptographic
/// randomness, not a low-entropy human password, so there is nothing for a slow KDF
/// (bcrypt/argon2) to protect against. Unlike DPAPI/<c>ISecretProtector</c>, a hash
/// is not machine-bound, so it survives a move to a Linux/NAS host — exactly what a
/// hostable backend needs.
/// </summary>
public sealed class AuthDevice
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
    /// <summary>Base64 of SHA-256(token). The token itself is shown once, at issue, and never stored.</summary>
    public string TokenHash { get; set; } = "";
    public DateTime CreatedUtc { get; set; }
    public DateTime? LastSeenUtc { get; set; }
}

/// <summary>Device metadata safe to hand to a UI — the hash is deliberately omitted.</summary>
public sealed record AuthDeviceInfo(string Id, string Name, DateTime CreatedUtc, DateTime? LastSeenUtc);

/// <summary>
/// Owns the set of per-device access tokens for remote (off-localhost) access.
///
/// Persisted as a small JSON file beside the database — the same
/// machine-state-not-operator-preference pattern as <c>QslBlockStateStore</c>, and
/// deliberately NOT in the settings export (a token set must never ride along in a
/// shared settings blob). Auth is only ever ENFORCED when the backend is reachable
/// off localhost (see <c>RemoteAccessGuard</c>); on the normal loopback desktop this
/// store simply stays empty and invisible.
/// </summary>
public sealed class AuthTokenStore
{
    private const string TokenPrefix = "sdrl_";
    private readonly string _path;
    private readonly object _gate = new();
    private List<AuthDevice> _devices;

    public AuthTokenStore(string path)
    {
        _path = path;
        _devices = Load();
    }

    /// <summary>True once at least one device token exists — the signal the bind guard checks.</summary>
    public bool AnyDevices
    {
        get { lock (_gate) return _devices.Count > 0; }
    }

    public IReadOnlyList<AuthDeviceInfo> List()
    {
        lock (_gate)
            return _devices
                .Select(d => new AuthDeviceInfo(d.Id, d.Name, d.CreatedUtc, d.LastSeenUtc))
                .ToList();
    }

    /// <summary>
    /// Mint a new token for a named device. Returns the plaintext token, which is the
    /// ONLY time it is ever available — the caller must show it to the operator now and
    /// then discard it. The store keeps only the hash.
    /// </summary>
    public (string Token, AuthDeviceInfo Device) Issue(string deviceName)
    {
        var token = TokenPrefix + Base64Url(RandomNumberGenerator.GetBytes(32));
        var device = new AuthDevice
        {
            Id = Guid.NewGuid().ToString("N")[..12],
            Name = string.IsNullOrWhiteSpace(deviceName) ? "device" : deviceName.Trim(),
            TokenHash = HashToken(token),
            CreatedUtc = DateTime.UtcNow,
        };
        lock (_gate)
        {
            _devices.Add(device);
            Save();
        }
        return (token, new AuthDeviceInfo(device.Id, device.Name, device.CreatedUtc, device.LastSeenUtc));
    }

    /// <summary>
    /// Constant-time-validate a presented token against every registered device.
    /// On success stamps LastSeen (persisted at most once a minute to avoid a write
    /// per request). Returns false for null/empty/unknown tokens.
    /// </summary>
    public bool Validate(string? presentedToken)
    {
        if (string.IsNullOrEmpty(presentedToken)) return false;
        var presentedHash = Sha256(presentedToken);

        lock (_gate)
        {
            foreach (var d in _devices)
            {
                var storedHash = TryDecodeBase64(d.TokenHash);
                if (storedHash is null) continue;
                if (!CryptographicOperations.FixedTimeEquals(storedHash, presentedHash)) continue;

                var now = DateTime.UtcNow;
                if (d.LastSeenUtc is null || now - d.LastSeenUtc.Value > TimeSpan.FromMinutes(1))
                {
                    d.LastSeenUtc = now;
                    Save();
                }
                return true;
            }
        }
        return false;
    }

    public bool Revoke(string deviceId)
    {
        lock (_gate)
        {
            var removed = _devices.RemoveAll(d => d.Id == deviceId) > 0;
            if (removed) Save();
            return removed;
        }
    }

    // -- persistence ---------------------------------------------------------

    private List<AuthDevice> Load()
    {
        lock (_gate)
        {
            try
            {
                if (!File.Exists(_path)) return new List<AuthDevice>();
                return JsonSerializer.Deserialize<List<AuthDevice>>(File.ReadAllText(_path))
                       ?? new List<AuthDevice>();
            }
            catch
            {
                // Unlike QslBlockStateStore (which fails OPEN), a corrupt token file must
                // fail CLOSED: an empty device set means the bind guard refuses a remote
                // bind rather than silently accepting all callers.
                return new List<AuthDevice>();
            }
        }
    }

    private void Save()
    {
        // caller holds _gate
        var dir = Path.GetDirectoryName(_path);
        if (!string.IsNullOrEmpty(dir)) Directory.CreateDirectory(dir);
        File.WriteAllText(_path, JsonSerializer.Serialize(_devices,
            new JsonSerializerOptions { WriteIndented = true }));
    }

    // -- hashing helpers -----------------------------------------------------

    private static string HashToken(string token) => Convert.ToBase64String(Sha256(token));
    private static byte[] Sha256(string s) => SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(s));

    private static byte[]? TryDecodeBase64(string s)
    {
        try { return string.IsNullOrEmpty(s) ? null : Convert.FromBase64String(s); }
        catch { return null; }
    }

    private static string Base64Url(byte[] bytes) =>
        Convert.ToBase64String(bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_');
}
