// Frame shapes verified against Thetis TCIServer.cs and the ExpertSDR3 TCI
// Protocol spec (github.com/ExpertSDR3/TCI):
//   rx_sensors:<rx>,<dBm>;
//   rx_channel_sensors:<rx>,<ch>,<dBm>;
//   rx_channel_sensors_ex:<rx>,<ch>,<dBm>,<avg dBm>,<peak-bin dBm>;
//   tx_sensors:<trx>,<mic dBm>,<watts>,<peak watts>,<swr>;
using System.Globalization;

namespace SDRLoggerPlus.Server.Services.Tci;

/// <summary>
/// RX sensor reading from a <c>rx_sensors:rx,dBm;</c> frame.
/// </summary>
public readonly record struct TciRxSensorReading(int Rx, double Dbm);

/// <summary>
/// Per-channel RX sensor reading from <c>rx_channel_sensors</c> (3 args) or
/// <c>rx_channel_sensors_ex</c> (5 args — adds average and peak-bin dBm).
/// </summary>
public readonly record struct TciRxChannelSensorReading(
    int Rx, int Channel, double Dbm, double? AvgDbm, double? PeakBinDbm);

/// <summary>
/// TX sensor reading from <c>tx_sensors:trx,micDbm,watts,peakWatts,swr;</c>
/// (5 args, transceiver index first — per the ExpertSDR3 TCI spec and
/// Thetis TCIServer.cs; Thetis emits one frame per trx each tick).
/// </summary>
public readonly record struct TciTxSensorReading(
    int Trx, double MicDbm, double PowerWatts, double PeakPowerWatts, double Swr);

/// <summary>
/// Decodes TCI sensor frame arguments (already split on ',' by the receive
/// loop). Malformed frames yield null so the caller can drop them silently —
/// sensor data is advisory and must never break the connection.
/// </summary>
public static class TciSensorParser
{
    public static TciRxSensorReading? ParseRxSensors(string[] args)
    {
        if (args.Length < 2) return null;
        if (!int.TryParse(args[0], NumberStyles.Integer, CultureInfo.InvariantCulture, out var rx)) return null;
        if (!TryParseDouble(args[1], out var dbm)) return null;

        return new TciRxSensorReading(rx, dbm);
    }

    public static TciRxChannelSensorReading? ParseRxChannelSensors(string[] args)
    {
        if (args.Length < 3) return null;
        if (!int.TryParse(args[0], NumberStyles.Integer, CultureInfo.InvariantCulture, out var rx)) return null;
        if (!int.TryParse(args[1], NumberStyles.Integer, CultureInfo.InvariantCulture, out var channel)) return null;
        if (!TryParseDouble(args[2], out var dbm)) return null;

        double? avgDbm = null;
        double? peakBinDbm = null;
        if (args.Length >= 5)
        {
            if (!TryParseDouble(args[3], out var avg)) return null;
            if (!TryParseDouble(args[4], out var peak)) return null;
            avgDbm = avg;
            peakBinDbm = peak;
        }

        return new TciRxChannelSensorReading(rx, channel, dbm, avgDbm, peakBinDbm);
    }

    public static TciTxSensorReading? ParseTxSensors(string[] args)
    {
        if (args.Length < 5) return null;
        if (!int.TryParse(args[0], NumberStyles.Integer, CultureInfo.InvariantCulture, out var trx)) return null;
        if (!TryParseDouble(args[1], out var micDbm)) return null;
        if (!TryParseDouble(args[2], out var watts)) return null;
        if (!TryParseDouble(args[3], out var peakWatts)) return null;
        if (!TryParseDouble(args[4], out var swr)) return null;

        return new TciTxSensorReading(trx, micDbm, watts, peakWatts, swr);
    }

    // Rejects non-finite values: "NaN"/"Infinity"/"1e309" all parse under
    // NumberStyles.Float, but System.Text.Json cannot serialize them and a
    // hostile peer must not be able to break the hub broadcast.
    private static bool TryParseDouble(string s, out double value) =>
        double.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out value)
        && double.IsFinite(value);
}
