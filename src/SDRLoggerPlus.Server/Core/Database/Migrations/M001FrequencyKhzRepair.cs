using LiteDB;
using SDRLoggerPlus.Contracts.Models;

namespace SDRLoggerPlus.Server.Core.Database.Migrations;

/// <summary>
/// Repairs QSOs whose Frequency was stored in MHz into the kHz field.
///
/// Four write paths (WSJT-X, satellite, and two edit paths) stored MHz into
/// <see cref="Qso.Frequency"/>, which is kHz. Exporters then divided by 1000
/// again, so QRZ/LoTW/ClubLog received values 1000x low — 14.075287 MHz went
/// up as FREQ=0.014. v2.6.0 (commit 6f0bb1c) stopped writing bad rows but
/// repaired nothing, so every user who logged FT8 or satellite before that
/// release still carries them. This is that repair, shipped.
///
/// The predicate is BAND-ANCHORED, never value-magnitude:
///
///     repair iff  Band known AND Frequency > 0
///             AND NOT inBand(Frequency)          // not already correct
///             AND     inBand(Frequency * 1000)   // correct once scaled
///
/// where inBand tests against the range of the band named ON THAT QSO.
///
/// Provably unambiguous: for a row to satisfy both clauses, its band would
/// need an upper/lower edge ratio of at least 1000. The widest amateur band
/// is 80m at 1.143. The tempting shortcut — "value under 1000 means MHz" —
/// would instead have corrupted 630m (correct value 474) and 2200m (137),
/// where a legitimate kHz reading is under 1000.
///
/// Idempotent by construction: after repair the row satisfies inBand, so the
/// second clause is false and it can never fire again. (Verified on live
/// data: 63 rows repaired, second pass found 0.)
///
/// UpdatedAt is deliberately NOT bumped and the write goes straight to the
/// collection rather than through LiteQsoRepository.UpdateAsync — that method
/// flips a Synced QSO to Modified, which would queue thousands of pointless
/// re-uploads to QRZ for rows this migration never touched the content of.
/// </summary>
public class M001FrequencyKhzRepair : IDbMigration
{
    public int Version => 1;
    public string Name => "frequency-mhz-to-khz-repair";

    /// <summary>
    /// Band edges in kHz, FROZEN as of this migration. Deliberately not
    /// BandHelper: that table is shared, editable, and maps frequency to band
    /// rather than band to range. A later PR widening a band there must not
    /// retroactively change which rows this shipped migration repairs.
    /// </summary>
    private static readonly Dictionary<string, (double LowKhz, double HighKhz)> BandsKhz =
        new(StringComparer.OrdinalIgnoreCase)
        {
            ["2200m"] = (135.7, 137.8),
            ["630m"] = (472, 479),
            ["160m"] = (1800, 2000),
            ["80m"] = (3500, 4000),
            ["60m"] = (5330.5, 5405),
            ["40m"] = (7000, 7300),
            ["30m"] = (10100, 10150),
            ["20m"] = (14000, 14350),
            ["17m"] = (18068, 18168),
            ["15m"] = (21000, 21450),
            ["12m"] = (24890, 24990),
            ["10m"] = (28000, 29700),
            ["6m"] = (50000, 54000),
            ["4m"] = (70000, 71000),
            ["2m"] = (144000, 148000),
            ["1.25m"] = (222000, 225000),
            ["70cm"] = (420000, 450000),
            ["33cm"] = (902000, 928000),
            ["23cm"] = (1240000, 1300000),
        };

    public MigrationResult Apply(LiteDatabase database, ILogger logger)
    {
        var collection = database.GetCollection<Qso>("qsos");
        var examined = 0;
        var details = new List<string>();
        var repaired = new List<Qso>();

        foreach (var qso in collection.FindAll())
        {
            examined++;
            if (!NeedsRepair(qso.Band, qso.Frequency)) continue;

            var before = qso.Frequency!.Value;
            // Rounded because 14.075287 * 1000 is 14075.286999999999 in binary
            // floating point, and the stored value should read as clean kHz.
            var after = Math.Round(before * 1000.0, 6);

            qso.Frequency = after;
            repaired.Add(qso);
            details.Add(
                $"{qso.Id}  {qso.Callsign,-12} {qso.Band,-7} {qso.QsoDate:yyyy-MM-dd} " +
                $"{before} -> {after} kHz");
        }

        // One bulk write, and only if something matched, so the common case
        // (an already-clean database) touches nothing at all.
        if (repaired.Count > 0)
        {
            collection.Update(repaired);
            logger.LogWarning(
                "Frequency repair: {Count} QSO(s) had MHz stored in the kHz field and were " +
                "corrected. These previously uploaded to QRZ/LoTW/ClubLog 1000x low.",
                repaired.Count);
        }

        return new MigrationResult(examined, repaired.Count, details);
    }

    /// <summary>
    /// The band-anchored predicate. Exposed internally so the tests can pin it
    /// directly, including the 630m/2200m cases that a magnitude rule breaks.
    /// </summary>
    internal static bool NeedsRepair(string? band, double? frequencyKhz)
    {
        if (string.IsNullOrWhiteSpace(band)) return false;
        if (frequencyKhz is not > 0) return false;
        if (!BandsKhz.TryGetValue(band.Trim(), out var range)) return false;

        var value = frequencyKhz.Value;
        var alreadyCorrect = value >= range.LowKhz && value <= range.HighKhz;
        if (alreadyCorrect) return false;

        var scaled = value * 1000.0;
        return scaled >= range.LowKhz && scaled <= range.HighKhz;
    }
}
