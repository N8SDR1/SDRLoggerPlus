namespace SDRLoggerPlus.Server.Core.Security;

/// <summary>
/// The on-disk format for a protected value: <c>slp$1$scheme$base64</c>.
///
/// Every stored secret is self-describing. That buys three things worth more
/// than the eight bytes of overhead:
///
///   * Legacy plaintext is distinguishable from ciphertext without a flag
///     column, so the encryption migration knows exactly what it has already
///     done and is idempotent for free.
///   * The scheme travels with the value, so a database written on Windows
///     (dpapi) is recognisably undecryptable on Linux (aes) — it reports a
///     re-entry prompt instead of returning garbage.
///   * A version field exists before it is needed, which is the only time it
///     can be added cheaply.
///
/// The prefix is deliberately unmistakable. A password that happens to look
/// like base64 must never be mistaken for ciphertext, or the migration would
/// "already encrypted"-skip a real plaintext credential and leave it exposed.
/// </summary>
public static class SecretEnvelope
{
    public const string Prefix = "slp$1$";

    public const string SchemeDpapi = "dpapi";
    public const string SchemeAes = "aes";

    public static string Wrap(string scheme, string base64) => $"{Prefix}{scheme}${base64}";

    /// <summary>True when the value is one of ours. Legacy plaintext is not.</summary>
    public static bool IsProtected(string? value) =>
        value != null && value.StartsWith(Prefix, StringComparison.Ordinal);

    /// <summary>
    /// Split an enveloped value. Returns false for anything that is not a
    /// well-formed envelope, including a value that merely starts with the
    /// prefix but is truncated.
    /// </summary>
    public static bool TryUnwrap(string? value, out string scheme, out string base64)
    {
        scheme = string.Empty;
        base64 = string.Empty;
        if (!IsProtected(value)) return false;

        var body = value!.Substring(Prefix.Length);
        var separator = body.IndexOf('$');
        if (separator <= 0 || separator == body.Length - 1) return false;

        scheme = body.Substring(0, separator);
        base64 = body.Substring(separator + 1);
        return true;
    }
}
