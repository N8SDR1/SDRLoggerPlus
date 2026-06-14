namespace SDRLoggerPlus.Server.Services.Sat;

public abstract record SatMessage;
public record SatBoot(string Serial, string Firmware) : SatMessage;
public record SatStartTrack(string Satellite, string CatalogNumber) : SatMessage;
public record SatAos(string AzimuthDeg) : SatMessage;
public record SatLos(string AzimuthDeg) : SatMessage;
public record SatTransponder(string Name, string UplinkFreq, string UplinkMode,
    string DownlinkFreq, string DownlinkMode) : SatMessage;
public record SatQso(string SatName, string Callsign, string Grid, string Mode,
    string Comment, string RstSent, string RstReceived, string UplinkHz,
    string DownlinkHz, string Name) : SatMessage;
public record SatStop : SatMessage;

/// <summary>
/// Parser for CSN Technologies S.A.T. controller UDP broadcasts — comma
/// separated "SAT,&lt;CMD&gt;,..." messages on port 9932 (ported from
/// SDRLogger+'s _parse_sat_message). Returns null for unknown/short frames.
/// </summary>
public static class SatMessageParser
{
    public static SatMessage? Parse(string text)
    {
        var trimmed = text.Trim();
        if (!trimmed.StartsWith("SAT,", StringComparison.OrdinalIgnoreCase)) return null;

        var parts = trimmed.Split(',');
        if (parts.Length < 2) return null;
        var cmd = parts[1].Trim().ToUpperInvariant();

        return cmd switch
        {
            "BOOT" when parts.Length >= 4 =>
                new SatBoot(parts[2].Trim(), parts[3].Trim()),
            "START TRACK" when parts.Length >= 4 =>
                new SatStartTrack(parts[2].Trim(), parts[3].Trim()),
            "AOS" when parts.Length >= 3 =>
                new SatAos(parts[2].Trim()),
            "LOS" when parts.Length >= 3 =>
                new SatLos(parts[2].Trim()),
            "TRANSPONDER" when parts.Length >= 7 =>
                new SatTransponder(parts[2].Trim(), parts[3].Trim(), parts[4].Trim(),
                    parts[5].Trim(), parts[6].Trim()),
            "QSO" when parts.Length >= 12 =>
                new SatQso(parts[2].Trim(), parts[3].Trim().ToUpperInvariant(), parts[4].Trim(),
                    parts[5].Trim().ToUpperInvariant(), parts[6].Trim(), parts[7].Trim(),
                    parts[8].Trim(), parts[9].Trim(), parts[10].Trim(),
                    parts.Length > 11 ? parts[11].Trim() : ""),
            "STOP" => new SatStop(),
            _ => null,
        };
    }
}
