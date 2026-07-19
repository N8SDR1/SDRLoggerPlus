using SDRLoggerPlus.Contracts.Api;

namespace SDRLoggerPlus.Server.Services.Wsjtx;

/// <summary>
/// Maps a WSJT-X "QSO Logged" message to a SDRLoggerPlus CreateQsoRequest, with the
/// same enrichment a manually entered QSO gets (country from cty.dat).
/// </summary>
public static class WsjtxQsoMapper
{
    public static CreateQsoRequest? ToCreateRequest(WsjtxQsoLogged qso)
    {
        if (string.IsNullOrWhiteSpace(qso.DxCall)) return null;

        var call = qso.DxCall.Trim().ToUpperInvariant();
        // Qso.Frequency is stored in kHz (ADIF export divides by 1000 → MHz).
        var freqKhz = qso.TxFrequencyHz / 1_000.0;
        var band = BandHelper.GetBand((long)qso.TxFrequencyHz);
        var (country, _, _) = CtyService.GetEntityFromCallsign(call);

        return new CreateQsoRequest(
            Callsign: call,
            QsoDate: qso.DateTimeOn,
            TimeOn: qso.DateTimeOn.ToString("HHmm"),
            Band: band,
            Mode: string.IsNullOrWhiteSpace(qso.Mode) ? "FT8" : qso.Mode.Trim().ToUpperInvariant(),
            Frequency: freqKhz,
            RstSent: NullIfEmpty(qso.ReportSent),
            RstRcvd: NullIfEmpty(qso.ReportReceived),
            Name: NullIfEmpty(qso.Name),
            Grid: NullIfEmpty(qso.DxGrid),
            Country: country,
            Comment: NullIfEmpty(qso.Comments));
    }

    private static string? NullIfEmpty(string? s) => string.IsNullOrWhiteSpace(s) ? null : s.Trim();
}

/// <summary>
/// Suppresses duplicate auto-log frames: the same call + time-off minute is
/// only logged once. Entries expire so the set can't grow unbounded.
/// </summary>
public class WsjtxDedupe
{
    private readonly TimeSpan _window;
    private readonly Dictionary<string, DateTime> _seen = new();
    private readonly object _lock = new();

    public WsjtxDedupe(TimeSpan? window = null) => _window = window ?? TimeSpan.FromMinutes(10);

    /// <returns>true when this QSO has not been seen before (caller should log it)</returns>
    public bool TryAdd(string callsign, DateTime timeOffUtc, DateTime? nowUtc = null)
    {
        var now = nowUtc ?? DateTime.UtcNow;
        var key = $"{callsign.ToUpperInvariant()}|{timeOffUtc:yyyyMMddHHmm}";
        lock (_lock)
        {
            // Expire old entries
            foreach (var stale in _seen.Where(kv => now - kv.Value > _window).Select(kv => kv.Key).ToList())
                _seen.Remove(stale);

            if (_seen.ContainsKey(key)) return false;
            _seen[key] = now;
            return true;
        }
    }
}
