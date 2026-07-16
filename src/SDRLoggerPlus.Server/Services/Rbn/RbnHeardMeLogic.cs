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

    /// <summary>QRZ coords when BOTH present, else the cty centroid, else null.</summary>
    public static (double Lat, double Lon)? PickLocation(double? qrzLat, double? qrzLon, (double Lat, double Lon)? ctyCentroid)
    {
        if (qrzLat is { } la && qrzLon is { } lo) return (la, lo);
        return ctyCentroid;
    }
}
