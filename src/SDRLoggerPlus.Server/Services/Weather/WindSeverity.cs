namespace SDRLoggerPlus.Server.Services.Weather;

/// <summary>
/// High-wind severity tiers, ported from SDRLogger+ (_wind_severity).
/// Thresholds are in mph — internal math always runs in mph so saved
/// configurations keep working regardless of the display unit.
/// </summary>
public static class WindSeverity
{
    public const string None = "";
    public const string Elevated = "elevated";
    public const string High = "high";
    public const string Extreme = "extreme";

    public static string Classify(double? sustainedMph, double? gustMph,
        string? nwsEvent, bool nwsIsExtreme, double threshSustMph, double threshGustMph)
    {
        if (nwsIsExtreme) return Extreme;

        var s = sustainedMph ?? 0.0;
        var g = gustMph ?? 0.0;

        if (s >= threshSustMph + 15 || g >= threshGustMph + 15) return Extreme;
        if (s >= threshSustMph || g >= threshGustMph) return High;
        if (!string.IsNullOrEmpty(nwsEvent)) return High; // advisory/watch without extreme flag
        if (s >= Math.Max(10, threshSustMph - 10) || g >= Math.Max(15, threshGustMph - 10)) return Elevated;
        return None;
    }
}
