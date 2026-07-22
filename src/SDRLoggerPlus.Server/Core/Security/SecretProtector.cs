using System.Security.Cryptography;
using System.Text;

namespace SDRLoggerPlus.Server.Core.Security;

/// <summary>
/// Per-OS credential protection: DPAPI on Windows, AES-GCM with a local key
/// file everywhere else.
///
/// The two are not equivalent, and the difference is stated here rather than
/// buried: DPAPI derives its key from the Windows user account and never
/// writes it anywhere we control, so a copied database is undecryptable
/// without that account. The AES fallback must keep its key on the same disk,
/// protected only by file permissions — so it defeats the copied-file threat
/// (backups, cloud sync, a shared support zip) but not someone who takes the
/// whole config directory. That is a real improvement over plaintext and an
/// honest step short of a keychain; libsecret/Keychain can slot in behind
/// <see cref="ISecretProtector"/> without touching a caller.
/// </summary>
public class SecretProtector : ISecretProtector
{
    private const string KeyFileName = ".secret.key";

    private readonly ILogger<SecretProtector> _logger;
    private readonly string _configDirectory;
    private readonly bool _useDpapi;
    private readonly object _keyLock = new();
    private byte[]? _aesKey;

    public SecretProtector(string configDirectory, ILogger<SecretProtector> logger)
        : this(configDirectory, logger, OperatingSystem.IsWindows())
    {
    }

    // useDpapi is injectable so the AES path is testable on Windows — without
    // it, the fallback would ship having only ever run in CI on Linux.
    internal SecretProtector(string configDirectory, ILogger<SecretProtector> logger, bool useDpapi)
    {
        _configDirectory = configDirectory;
        _logger = logger;
        _useDpapi = useDpapi && OperatingSystem.IsWindows();
    }

    public string? Protect(string? plaintext)
    {
        if (string.IsNullOrEmpty(plaintext)) return plaintext;

        // Never double-wrap. Callers include a migration that may see values
        // it already converted, and the repository, which round-trips whatever
        // the API handed back.
        if (SecretEnvelope.IsProtected(plaintext)) return plaintext;

        try
        {
            var bytes = Encoding.UTF8.GetBytes(plaintext);
            return _useDpapi
                ? SecretEnvelope.Wrap(SecretEnvelope.SchemeDpapi, Convert.ToBase64String(ProtectDpapi(bytes)))
                : SecretEnvelope.Wrap(SecretEnvelope.SchemeAes, Convert.ToBase64String(ProtectAes(bytes)));
        }
        catch (Exception ex)
        {
            // Storing the plaintext would silently undo the whole feature, so
            // fail the save instead and let the operator see it.
            _logger.LogError(ex, "Could not encrypt a credential for storage");
            throw;
        }
    }

    public string? Unprotect(string? stored)
    {
        if (string.IsNullOrEmpty(stored)) return stored;

        // Legacy plaintext, written before this shipped or by an older build.
        if (!SecretEnvelope.TryUnwrap(stored, out var scheme, out var base64)) return stored;

        try
        {
            var bytes = Convert.FromBase64String(base64);
            var plain = scheme switch
            {
                SecretEnvelope.SchemeDpapi when OperatingSystem.IsWindows() => UnprotectDpapi(bytes),
                SecretEnvelope.SchemeAes => UnprotectAes(bytes),
                _ => null
            };

            if (plain == null)
            {
                _logger.LogWarning(
                    "A stored credential uses the '{Scheme}' scheme, which this machine cannot read. " +
                    "It will appear blank and needs re-entering.", scheme);
                return null;
            }

            return Encoding.UTF8.GetString(plain);
        }
        catch (Exception ex)
        {
            // Wrong user account, restored-from-another-machine backup, or a
            // corrupt value. An unreadable password must not stop the app from
            // starting — the operator re-enters it.
            _logger.LogWarning(ex,
                "A stored credential could not be decrypted on this machine and will appear blank");
            return null;
        }
    }

    // --- Windows -----------------------------------------------------------

    [System.Runtime.Versioning.SupportedOSPlatform("windows")]
    private static byte[] ProtectDpapi(byte[] plaintext) =>
        ProtectedData.Protect(plaintext, optionalEntropy: null, DataProtectionScope.CurrentUser);

    [System.Runtime.Versioning.SupportedOSPlatform("windows")]
    private static byte[] UnprotectDpapi(byte[] ciphertext) =>
        ProtectedData.Unprotect(ciphertext, optionalEntropy: null, DataProtectionScope.CurrentUser);

    // --- Everything else ---------------------------------------------------
    //
    // AES-256-GCM. Layout: [12-byte nonce][16-byte tag][ciphertext]. GCM is
    // authenticated, so a tampered or truncated value fails to decrypt rather
    // than yielding plausible garbage.

    private byte[] ProtectAes(byte[] plaintext)
    {
        var key = GetOrCreateAesKey();
        var nonce = RandomNumberGenerator.GetBytes(AesGcm.NonceByteSizes.MaxSize);
        var tag = new byte[AesGcm.TagByteSizes.MaxSize];
        var ciphertext = new byte[plaintext.Length];

        using var aes = new AesGcm(key, tag.Length);
        aes.Encrypt(nonce, plaintext, ciphertext, tag);

        var output = new byte[nonce.Length + tag.Length + ciphertext.Length];
        nonce.CopyTo(output, 0);
        tag.CopyTo(output, nonce.Length);
        ciphertext.CopyTo(output, nonce.Length + tag.Length);
        return output;
    }

    private byte[] UnprotectAes(byte[] input)
    {
        var key = GetOrCreateAesKey();
        var nonceLength = AesGcm.NonceByteSizes.MaxSize;
        var tagLength = AesGcm.TagByteSizes.MaxSize;
        if (input.Length < nonceLength + tagLength)
            throw new CryptographicException("Protected value is too short to be valid");

        var nonce = input.AsSpan(0, nonceLength);
        var tag = input.AsSpan(nonceLength, tagLength);
        var ciphertext = input.AsSpan(nonceLength + tagLength);
        var plaintext = new byte[ciphertext.Length];

        using var aes = new AesGcm(key, tagLength);
        aes.Decrypt(nonce, ciphertext, tag, plaintext);
        return plaintext;
    }

    /// <summary>
    /// The AES key, created on first use beside the database and restricted to
    /// the owner. Losing this file means every stored credential must be
    /// re-entered, which is why it is never regenerated implicitly — a read
    /// failure throws rather than quietly minting a new key that would render
    /// existing values permanently unreadable.
    /// </summary>
    private byte[] GetOrCreateAesKey()
    {
        lock (_keyLock)
        {
            if (_aesKey != null) return _aesKey;

            var path = Path.Combine(_configDirectory, KeyFileName);
            if (File.Exists(path))
            {
                var existing = Convert.FromBase64String(File.ReadAllText(path).Trim());
                if (existing.Length != 32)
                    throw new CryptographicException($"Key file {path} is malformed");
                return _aesKey = existing;
            }

            Directory.CreateDirectory(_configDirectory);
            var key = RandomNumberGenerator.GetBytes(32);
            File.WriteAllText(path, Convert.ToBase64String(key));
            RestrictToOwner(path);
            _logger.LogInformation("Created a new credential key at {Path}", path);
            return _aesKey = key;
        }
    }

    private void RestrictToOwner(string path)
    {
        try
        {
            if (!OperatingSystem.IsWindows())
            {
                File.SetUnixFileMode(path, UnixFileMode.UserRead | UnixFileMode.UserWrite);
            }
        }
        catch (Exception ex)
        {
            // Worth knowing about — the key is the whole protection on this
            // platform — but not worth refusing to run over.
            _logger.LogWarning(ex, "Could not restrict permissions on the credential key file");
        }
    }
}
