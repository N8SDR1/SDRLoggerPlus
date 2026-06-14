using System.Text.RegularExpressions;

namespace SDRLoggerPlus.Server.Services.BandOpening;

/// <summary>A parsed RBN telnet spot line.</summary>
public record RbnSpot(string Skimmer, double FrequencyKhz, string DxCall, string Mode, int Snr);

/// <summary>
/// Pure logic for RBN band-opening alerts (SDRLogger+ port): spot-line parsing,
/// VHF/UHF alert-band mapping, skimmer callsign normalization, distance.
/// </summary>
public static partial class BandOpeningLogic
{
    // "DX de W3OA-#:  50125.1  K5XYZ  CW  24 dB  22 WPM  CQ  1830Z"
    [GeneratedRegex(@"DX\s+de\s+(\S+?):\s+(\d+\.?\d*)\s+(\S+)\s+(\S+)\s+(\d+)\s+dB\s+(\d+)\s+(?:WPM|BPS)\s+(\S+)\s+(\d{4})Z", RegexOptions.IgnoreCase)]
    private static partial Regex SpotRegex();

    /// <summary>VHF/UHF alert bands: name → (lo, hi) in kHz.</summary>
    public static readonly IReadOnlyDictionary<string, (double Lo, double Hi)> AlertBands =
        new Dictionary<string, (double, double)>
        {
            ["10m"] = (28000, 29700),
            ["6m"] = (50000, 54000),
            ["2m"] = (144000, 148000),
            ["70cm"] = (420000, 450000),
        };

    public static RbnSpot? ParseSpotLine(string line)
    {
        var m = SpotRegex().Match(line);
        if (!m.Success) return null;
        if (!double.TryParse(m.Groups[2].Value, System.Globalization.NumberStyles.Float,
                System.Globalization.CultureInfo.InvariantCulture, out var khz))
            return null;
        return new RbnSpot(
            Skimmer: m.Groups[1].Value,
            FrequencyKhz: khz,
            DxCall: m.Groups[3].Value.ToUpperInvariant(),
            Mode: m.Groups[4].Value.ToUpperInvariant(),
            Snr: int.Parse(m.Groups[5].Value));
    }

    /// <summary>Strips skimmer suffixes ("-#", "-2", digits) for the QRZ grid lookup.</summary>
    public static string NormalizeSkimmerCall(string raw) =>
        raw.ToUpperInvariant().TrimEnd('-', '#', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9');

    /// <summary>The alert band containing the frequency, or null when not an alert band.</summary>
    public static string? AlertBandFor(double khz)
    {
        foreach (var (band, (lo, hi)) in AlertBands)
        {
            if (khz >= lo && khz <= hi) return band;
        }
        return null;
    }

    public static double HaversineKm(double lat1, double lon1, double lat2, double lon2)
    {
        const double r = 6371.0;
        var dLat = (lat2 - lat1) * Math.PI / 180;
        var dLon = (lon2 - lon1) * Math.PI / 180;
        var a = Math.Sin(dLat / 2) * Math.Sin(dLat / 2) +
                Math.Cos(lat1 * Math.PI / 180) * Math.Cos(lat2 * Math.PI / 180) *
                Math.Sin(dLon / 2) * Math.Sin(dLon / 2);
        return r * 2 * Math.Atan2(Math.Sqrt(a), Math.Sqrt(1 - a));
    }
}

/// <summary>Per-band alert cooldown so a band opening doesn't re-alert every spot.</summary>
public class BandCooldownGate
{
    private readonly Dictionary<string, DateTime> _lastAlert = new();

    public bool ShouldAlert(string band, int cooldownMinutes, DateTime now)
    {
        if (_lastAlert.TryGetValue(band, out var last) && (now - last).TotalMinutes < cooldownMinutes)
            return false;
        _lastAlert[band] = now;
        return true;
    }
}
