namespace SDRLoggerPlus.Server.Services.Weather;

public static class GeoMath
{
    /// <summary>Initial great-circle bearing from point 1 to point 2, degrees 0–360.</summary>
    public static double BearingDeg(double lat1, double lon1, double lat2, double lon2)
    {
        var φ1 = lat1 * Math.PI / 180;
        var φ2 = lat2 * Math.PI / 180;
        var Δλ = (lon2 - lon1) * Math.PI / 180;
        var y = Math.Sin(Δλ) * Math.Cos(φ2);
        var x = Math.Cos(φ1) * Math.Sin(φ2) - Math.Sin(φ1) * Math.Cos(φ2) * Math.Cos(Δλ);
        return (Math.Atan2(y, x) * 180 / Math.PI + 360) % 360;
    }

    private static readonly string[] CompassPoints =
        ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
         "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"];

    public static string BearingToCompass(double bearingDeg)
    {
        var idx = (int)Math.Round(((bearingDeg % 360 + 360) % 360) / 22.5) % 16;
        return CompassPoints[idx];
    }

    /// <summary>Maidenhead grid (4+ chars) → approximate center lat/lon.</summary>
    public static (double Lat, double Lon)? GridToLatLon(string? grid)
    {
        if (string.IsNullOrWhiteSpace(grid) || grid.Length < 4) return null;
        var g = grid.Trim().ToUpperInvariant();
        if (!char.IsLetter(g[0]) || !char.IsLetter(g[1]) || !char.IsDigit(g[2]) || !char.IsDigit(g[3]))
            return null;

        var lon = (g[0] - 'A') * 20.0 - 180 + (g[2] - '0') * 2.0;
        var lat = (g[1] - 'A') * 10.0 - 90 + (g[3] - '0') * 1.0;

        if (g.Length >= 6 && char.IsLetter(g[4]) && char.IsLetter(g[5]))
        {
            lon += (g[4] - 'A') * (2.0 / 24) + 1.0 / 24;
            lat += (g[5] - 'A') * (1.0 / 24) + 0.5 / 24;
        }
        else
        {
            lon += 1.0; // center of 2° field
            lat += 0.5; // center of 1° field
        }
        return (lat, lon);
    }
}
