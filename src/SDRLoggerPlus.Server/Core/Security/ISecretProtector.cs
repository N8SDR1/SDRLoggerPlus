namespace SDRLoggerPlus.Server.Core.Security;

/// <summary>
/// Encrypts and decrypts individual credential values at the storage boundary.
///
/// Threat model, stated plainly so the design can be judged against it: the
/// database is a plain file in the user's roaming profile, copied nightly by
/// the backup service and, for many operators, into OneDrive/Dropbox. The
/// realistic loss is a credential leaking through one of those COPIES — a
/// synced folder, a backup handed to someone for troubleshooting, a support
/// zip. Binding the ciphertext to the local user account defeats all of that.
///
/// What this does NOT defend against is malware already running as the user:
/// it can call the same API. No local-only scheme can fix that, and pretending
/// otherwise is how the "Stored obfuscated" comments got written in the first
/// place.
/// </summary>
public interface ISecretProtector
{
    /// <summary>
    /// Encrypt a value for storage. Returns an enveloped string
    /// (see <see cref="SecretEnvelope"/>). Null/empty passes through unchanged
    /// so an unconfigured credential stays visibly empty rather than becoming
    /// an opaque blob.
    /// </summary>
    string? Protect(string? plaintext);

    /// <summary>
    /// Decrypt a stored value. A value with no envelope is legacy plaintext
    /// and is returned as-is — that is what makes an existing database keep
    /// working before (and during) the encryption migration.
    ///
    /// A value that IS enveloped but cannot be decrypted (restored onto a
    /// different machine or user account) returns null rather than throwing:
    /// the operator sees an empty credential field to re-enter, instead of an
    /// app that will not start.
    /// </summary>
    string? Unprotect(string? stored);
}
