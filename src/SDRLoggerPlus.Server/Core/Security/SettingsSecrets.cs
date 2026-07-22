using SDRLoggerPlus.Contracts.Models;

namespace SDRLoggerPlus.Server.Core.Security;

/// <summary>
/// Every credential field in <see cref="UserSettings"/>, in one place.
///
/// This list is the security boundary. A credential added to Settings.cs and
/// not added here is stored in plaintext, silently, forever — so
/// SettingsSecretsCoverageTests reflects over the settings model and fails
/// when a secret-looking property is missing from this file. That test is the
/// point of the design; the list is just data.
///
/// Accessors are used rather than BSON paths so a rename in Settings.cs is a
/// compile error here instead of a field that quietly stops being encrypted.
/// </summary>
public static class SettingsSecrets
{
    /// <summary>One credential: how to read it and how to write it back.</summary>
    public record Accessor(string Name, Func<UserSettings, string?> Get, Action<UserSettings, string?> Set);

    /// <summary>
    /// The fixed (non-collection) credentials. Cluster passwords live in a
    /// list and are handled separately by <see cref="Transform"/>.
    /// </summary>
    public static readonly IReadOnlyList<Accessor> Fields = new Accessor[]
    {
        new("qrz.password", s => s.Qrz.Password, (s, v) => s.Qrz.Password = v),
        new("qrz.apiKey", s => s.Qrz.ApiKey, (s, v) => s.Qrz.ApiKey = v),
        new("hamqth.password", s => s.HamQth.Password, (s, v) => s.HamQth.Password = v),
        new("lotw.password", s => s.Lotw.Password, (s, v) => s.Lotw.Password = v),
        new("clubLog.password", s => s.ClubLog.Password, (s, v) => s.ClubLog.Password = v),
        new("clubLog.apiKey", s => s.ClubLog.ApiKey, (s, v) => s.ClubLog.ApiKey = v),
        new("hrdLog.uploadCode", s => s.HrdLog.UploadCode, (s, v) => s.HrdLog.UploadCode = v),
        new("pota.password", s => s.Pota.Password, (s, v) => s.Pota.Password = v),
        new("eqsl.password", s => s.Eqsl.Password, (s, v) => s.Eqsl.Password = v),
        new("ai.apiKey", s => s.Ai.ApiKey, (s, v) => s.Ai.ApiKey = v),
        new("weather.credentials.ambientApiKey",
            s => s.Weather.Credentials.AmbientApiKey, (s, v) => s.Weather.Credentials.AmbientApiKey = v),
        new("weather.credentials.ambientAppKey",
            s => s.Weather.Credentials.AmbientAppKey, (s, v) => s.Weather.Credentials.AmbientAppKey = v),
        new("weather.credentials.ecowittApiKey",
            s => s.Weather.Credentials.EcowittApiKey, (s, v) => s.Weather.Credentials.EcowittApiKey = v),
        new("weather.credentials.ecowittAppKey",
            s => s.Weather.Credentials.EcowittAppKey, (s, v) => s.Weather.Credentials.EcowittAppKey = v),
    };

    /// <summary>
    /// Apply <paramref name="transform"/> to every credential on
    /// <paramref name="settings"/>, in place. Used with Protect on save and
    /// Unprotect on load, so the two directions cannot cover different fields.
    /// </summary>
    public static void Transform(UserSettings settings, Func<string?, string?> transform)
    {
        foreach (var field in Fields)
        {
            field.Set(settings, transform(field.Get(settings)));
        }

        // Cluster passwords are per-connection and unbounded in number.
        foreach (var connection in settings.Cluster.Connections)
        {
            connection.Password = transform(connection.Password);
        }
    }
}
