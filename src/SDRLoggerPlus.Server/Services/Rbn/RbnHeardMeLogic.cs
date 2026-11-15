using SDRLoggerPlus.Contracts.Models;

namespace SDRLoggerPlus.Server.Services.Rbn;

/// <summary>
/// Pure helpers for the RBN "who heard me" endpoint — no telnet, no I/O, unit-testable.
/// </summary>
public static class RbnHeardMeLogic
{
    public static int ClampWindowMinutes(int minutes) => Math.Clamp(minutes, 5, 15);

    /// <summary>Spots where <paramref name="myCall"/> is the spotted DX, optionally on one band.</summary>
    public static List<RbnSpot> HeardBy(IEnumerable<RbnSpot> spots, string myCall, string? band)
    {
        var call = (myCall ?? string.Empty).Trim();
        return spots.Where(s =>
                string.Equals(s.Dx, call, StringComparison.OrdinalIgnoreCase) &&
                (string.IsNullOrEmpty(band) || string.Equals(s.Band, band, StringComparison.OrdinalIgnoreCase)))
            .ToList();
    }

    /// <summary>QRZ coords when BOTH present (precise), else the cty centroid (not precise), else null.</summary>
    public static StationLocation? PickLocation(double? qrzLat, double? qrzLon, (double Lat, double Lon)? ctyCentroid)
    {
        if (qrzLat is { } la && qrzLon is { } lo) return new StationLocation(la, lo, Precise: true);
        return ctyCentroid is { } c ? new StationLocation(c.Lat, c.Lon, Precise: false) : null;
    }
}
