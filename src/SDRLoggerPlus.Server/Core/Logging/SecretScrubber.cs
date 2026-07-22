using System.Text.RegularExpressions;

namespace SDRLoggerPlus.Server.Core.Logging;

/// <summary>
/// Masks credentials in text on its way to a log or a user-facing error message.
///
/// Several of the services we talk to only accept credentials in the query
/// string — LoTW's report endpoint, QRZ's XML API, Club Log's getlotwstate,
/// eQSL's inbox download. We cannot change their APIs, so the URLs we build
/// genuinely contain passwords and API keys. That is only a leak once such a
/// string reaches a log, and the distance between "builds a credentialed URL"
/// and "logs it" is one future edit.
///
/// Two kinds of masking, because they catch different leaks:
///
///   - Parameter masking finds <c>name=value</c> pairs whose NAME marks the
///     value as a secret. This works on strings nobody anticipated — it is what
///     protects code not yet written.
///   - Value masking blanks specific secrets wherever they appear, with no
///     surrounding syntax needed. This is for third-party responses that echo
///     the credential back at us: QRZ's logbook API returns the offending key
///     inside REASON= when it rejects one, and we log that response verbatim.
///
/// Over-masking is the intended failure mode. A log line reading
/// <c>key=***</c> when the value was harmless costs a debugging session;
/// the reverse costs a credential.
/// </summary>
public static partial class SecretScrubber
{
    public const string Mask = "***";

    /// <summary>
    /// Values shorter than this are not masked by value. Short strings appear
    /// inside unrelated words by coincidence, and a 3-character "secret" would
    /// shred the whole line. Real API keys and passwords are far longer; a
    /// secret this short is not one worth protecting at that cost.
    /// </summary>
    private const int MinimumMaskableSecretLength = 6;

    /// <summary>
    /// Parameter names whose value is a credential. Deliberately broad: it
    /// covers our own URLs (password, api, apikey, application_key, upload_code)
    /// plus the shapes third-party APIs use. <c>s</c> is QRZ's session key.
    /// </summary>
    /// <remarks>
    /// The lookbehind rejects an alphanumeric predecessor but allows an
    /// underscore, so <c>EQSL_PSWD=</c> is caught while <c>items=</c> is not.
    /// A plain <c>\b</c> gets this backwards: underscore is a word character,
    /// so it would let <c>items=</c> through only by also missing every
    /// underscore-prefixed field name, which is how eQSL names all of its.
    /// </remarks>
    [GeneratedRegex(
        @"(?<![A-Za-z0-9])(?<name>password|passwd|pswd|apikey|api_key|api|applicationkey|application_key|appkey|app_key|upload_code|uploadcode|secret|token|auth|key|s)=(?<value>[^&\s""'<>\]}]+)",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex CredentialParameter();

    /// <summary>
    /// Returns <paramref name="text"/> with credential parameter values masked,
    /// plus any of <paramref name="secrets"/> that appear in it. Null and empty
    /// inputs pass through so callers can wrap log arguments unconditionally.
    /// </summary>
    public static string? Redact(string? text, params string?[] secrets)
    {
        if (string.IsNullOrEmpty(text)) return text;

        var result = CredentialParameter().Replace(text, m => $"{m.Groups["name"].Value}={Mask}");

        foreach (var secret in secrets)
        {
            if (string.IsNullOrWhiteSpace(secret) || secret.Length < MinimumMaskableSecretLength)
                continue;
            result = result.Replace(secret, Mask, StringComparison.Ordinal);
            // Credentials reach a log through a URL as often as raw, and by then
            // they are percent-encoded — a different string than the one we hold.
            var escaped = Uri.EscapeDataString(secret);
            if (!string.Equals(escaped, secret, StringComparison.Ordinal))
                result = result.Replace(escaped, Mask, StringComparison.Ordinal);
        }

        return result;
    }

    /// <summary>
    /// First <paramref name="maxLength"/> characters of <paramref name="text"/>.
    /// Third-party bodies go into log lines and into the sync ledger's stored
    /// error, and both want the head of the message, not an HTML page.
    /// </summary>
    public static string Head(string text, int maxLength) =>
        text.Length > maxLength ? text[..maxLength] : text;
}
