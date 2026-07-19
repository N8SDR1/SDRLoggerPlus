using SDRLoggerPlus.Contracts.Models.Contesting;

namespace SDRLoggerPlus.Server.Services.Contesting;

/// <summary>
/// Pure serial-number allocation for a contest session. Honors the definition's
/// <see cref="SerialMode"/> (all-band vs per-band). Mutation is confined to the
/// passed <see cref="ContestSession"/> counters so the session service can persist
/// afterward; there is no I/O here.
/// </summary>
public static class ContestSerials
{
    /// <summary>The serial that will be assigned to the next QSO on this band (0 if none).</summary>
    public static int Peek(ContestSession session, SerialMode mode, string band)
    {
        return mode switch
        {
            SerialMode.AllBand => session.NextSerialAllBand,
            SerialMode.PerBand => session.NextSerialPerBand.TryGetValue(band, out var n) ? n : 1,
            _ => 0,
        };
    }

    /// <summary>Return the next serial and advance the appropriate counter.</summary>
    public static int Allocate(ContestSession session, SerialMode mode, string band)
    {
        switch (mode)
        {
            case SerialMode.AllBand:
                var all = session.NextSerialAllBand;
                session.NextSerialAllBand = all + 1;
                return all;
            case SerialMode.PerBand:
                var cur = session.NextSerialPerBand.TryGetValue(band, out var n) ? n : 1;
                session.NextSerialPerBand[band] = cur + 1;
                return cur;
            default:
                return 0;
        }
    }

    /// <summary>Format a serial for display / Cabrillo (zero-padded to at least 3 digits).</summary>
    public static string Format(int serial) => serial.ToString("D3");
}
