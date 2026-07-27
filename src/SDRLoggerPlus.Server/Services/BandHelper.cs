namespace SDRLoggerPlus.Server.Services;

/// <summary>
/// Helper class for frequency to amateur band mapping
/// </summary>
public static class BandHelper
{
    private static readonly (long LowerHz, long UpperHz, string Band)[] BandRanges =
    {
        (135_700, 137_800, "2200m"),
        (472_000, 479_000, "630m"),
        (1_800_000, 2_000_000, "160m"),
        (3_500_000, 4_000_000, "80m"),
        (5_330_500, 5_405_000, "60m"),
        (7_000_000, 7_300_000, "40m"),
        (10_100_000, 10_150_000, "30m"),
        (14_000_000, 14_350_000, "20m"),
        (18_068_000, 18_168_000, "17m"),
        (21_000_000, 21_450_000, "15m"),
        (24_890_000, 24_990_000, "12m"),
        (28_000_000, 29_700_000, "10m"),
        (50_000_000, 54_000_000, "6m"),
        (144_000_000, 148_000_000, "2m"),
        (222_000_000, 225_000_000, "1.25m"),
        (420_000_000, 450_000_000, "70cm"),
        (902_000_000, 928_000_000, "33cm"),
        (1_240_000_000, 1_300_000_000, "23cm"),
        // Microwave bands (ADIF). 13cm + 3cm are the QO-100 (Es'hail-2) uplink/downlink.
        (2_300_000_000, 2_450_000_000, "13cm"),
        (3_300_000_000, 3_500_000_000, "9cm"),
        (5_650_000_000, 5_925_000_000, "6cm"),
        (10_000_000_000, 10_500_000_000, "3cm"),
    };

    /// <summary>
    /// Get the amateur band for a given frequency in Hz
    /// </summary>
    public static string GetBand(long frequencyHz)
    {
        foreach (var (lower, upper, band) in BandRanges)
        {
            if (frequencyHz >= lower && frequencyHz <= upper)
            {
                return band;
            }
        }
        return "Unknown";
    }

    /// <summary>
    /// Get the amateur band for a given frequency in MHz
    /// </summary>
    public static string GetBandFromMhz(double frequencyMhz)
    {
        return GetBand((long)(frequencyMhz * 1_000_000));
    }

    /// <summary>
    /// Check if a frequency is within amateur bands
    /// </summary>
    public static bool IsAmateurBand(long frequencyHz)
    {
        return GetBand(frequencyHz) != "Unknown";
    }
}
