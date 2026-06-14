namespace SDRLoggerPlus.Server.Services.Sat;

/// <summary>
/// Spherical-Earth satellite geometry (R = 6371 km; &lt;0.3% error at LEO).
/// Ported from SDRLogger+ with its v1.10 footprint fix: the footprint
/// radius is computed from altitude via the 0° geometric-horizon formula —
/// never trust the controller's satFootprint value as a radius, it is a
/// DIAMETER (predict/Gpredict convention) and renders circles 2× too large.
/// </summary>
public static class SatGeometry
{
    private const double EarthRadiusKm = 6371.0;

    /// <summary>Footprint (visibility circle) radius in km from altitude: r = R·acos(R/(R+h)).</summary>
    public static double FootprintRadiusKm(double altitudeKm)
    {
        if (altitudeKm <= 0) return 0;
        return EarthRadiusKm * Math.Acos(EarthRadiusKm / (EarthRadiusKm + altitudeKm));
    }

    /// <summary>
    /// Sub-satellite geodetic point from station look-angle + slant range:
    /// station ECEF + range·(ENU look vector rotated to ECEF) → lat/lon/alt.
    /// </summary>
    public static (double Lat, double Lon, double AltKm)? SubpointFromLook(
        double stationLatDeg, double stationLonDeg, double azimuthDeg, double elevationDeg, double rangeKm)
    {
        if (rangeKm <= 0) return null;

        var az = azimuthDeg * Math.PI / 180;
        var el = elevationDeg * Math.PI / 180;
        var lat = stationLatDeg * Math.PI / 180;
        var lon = stationLonDeg * Math.PI / 180;

        // Station ECEF (spherical)
        var sx = EarthRadiusKm * Math.Cos(lat) * Math.Cos(lon);
        var sy = EarthRadiusKm * Math.Cos(lat) * Math.Sin(lon);
        var sz = EarthRadiusKm * Math.Sin(lat);

        // ENU components of look vector (azimuth clockwise from north)
        var e = Math.Cos(el) * Math.Sin(az);
        var n = Math.Cos(el) * Math.Cos(az);
        var u = Math.Sin(el);

        // ENU → ECEF rotation
        var ex = -Math.Sin(lon) * e - Math.Sin(lat) * Math.Cos(lon) * n + Math.Cos(lat) * Math.Cos(lon) * u;
        var ey = Math.Cos(lon) * e - Math.Sin(lat) * Math.Sin(lon) * n + Math.Cos(lat) * Math.Sin(lon) * u;
        var ez = Math.Cos(lat) * n + Math.Sin(lat) * u;

        var satX = sx + rangeKm * ex;
        var satY = sy + rangeKm * ey;
        var satZ = sz + rangeKm * ez;

        var rSat = Math.Sqrt(satX * satX + satY * satY + satZ * satZ);
        var satLat = Math.Asin(satZ / rSat) * 180 / Math.PI;
        var satLon = Math.Atan2(satY, satX) * 180 / Math.PI;
        return (Math.Round(satLat, 4), Math.Round(satLon, 4), Math.Round(rSat - EarthRadiusKm, 1));
    }
}
