using SDRLoggerPlus.Contracts.Models;

namespace SDRLoggerPlus.Server.Services.Satellites;

/// <summary>
/// Decides whether a QSO went through a satellite, and which bird.
///
/// There is no satellite field on <see cref="Qso"/>. Log Entry's SAT mode and
/// ADIF import both record the ADIF convention in <c>AdifExtra</c>:
/// <c>PROP_MODE=SAT</c> plus <c>SAT_NAME</c> (e.g. "IO-117"). Reading from
/// there means existing logs count immediately, with no migration.
/// </summary>
public static class SatelliteResolver
{
    private const string PropModeField = "PROP_MODE";
    private const string SatNameField = "SAT_NAME";

    /// <summary>ADIF PROP_MODE value marking a satellite contact.</summary>
    private const string SatellitePropMode = "SAT";

    /// <summary>
    /// True when the QSO went via a satellite. PROP_MODE is the authority, but
    /// a named bird counts too: some loggers write SAT_NAME and omit
    /// PROP_MODE, and a QSO that names a satellite plainly went through one.
    /// </summary>
    public static bool IsSatellite(Qso qso) =>
        string.Equals(AdifExtraReader.ReadTrimmed(qso, PropModeField), SatellitePropMode,
            StringComparison.OrdinalIgnoreCase)
        || Name(qso) != null;

    /// <summary>
    /// The bird's designator, uppercased so "io-117" and "IO-117" are one
    /// satellite rather than two rows. Null when the QSO names no satellite —
    /// including a PROP_MODE=SAT QSO whose SAT_NAME was never filled in, which
    /// is why <see cref="IsSatellite"/> and this are separate questions.
    /// </summary>
    public static string? Name(Qso qso)
    {
        var raw = AdifExtraReader.ReadTrimmed(qso, SatNameField);
        return raw?.ToUpperInvariant();
    }
}
