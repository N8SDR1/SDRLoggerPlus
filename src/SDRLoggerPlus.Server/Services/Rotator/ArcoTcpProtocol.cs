using System.Globalization;
using System.Text.RegularExpressions;

namespace SDRLoggerPlus.Server.Services.Rotator;

/// <summary>
/// microHAM ARCO controller — GS-232A emulation over TCP. Ported from SDRLogger+
/// (<c>main.py</c>, the <c>arco_tcp</c> protocol):
/// <list type="bullet">
/// <item><c>C2\r</c> → combined AZ+EL query; reply <c>+0nnn+0eee</c> (signed 3–4 digit).</item>
/// <item><c>M###\r</c> → set azimuth, truncated integer degrees (e.g. 270.6 → <c>M270</c>).</item>
/// <item><c>S\r</c> → stop all motion.</item>
/// </list>
/// Commands are raw CR-terminated; M/S produce no response. Elevation is parsed off the
/// reply but ignored for now (azimuth-only). Unlike SDRLogger+, which opens a short-lived
/// socket per set/stop, this reuses RotatorService's single persistent connection — ARCO's
/// M/S are silent, so they never desync the C2 poll on the shared socket.
/// </summary>
public sealed partial class ArcoTcpProtocol : IRotatorProtocol
{
    // First signed 3–4 digit run in the reply = azimuth (e.g. "+0270" from "+0270+0000").
    [GeneratedRegex(@"[+\-]?\d{3,4}")]
    private static partial Regex AzimuthRegex();

    public async Task<double?> PollAzimuthAsync(StreamReader reader, StreamWriter writer, CancellationToken ct)
    {
        await writer.WriteAsync("C2\r");
        var line = await reader.ReadLineAsync(ct);
        if (string.IsNullOrWhiteSpace(line)) return null;

        var match = AzimuthRegex().Match(line);
        return match.Success
            && double.TryParse(match.Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var az)
            ? az : null;
    }

    public Task SetAzimuthAsync(double azimuth, StreamReader reader, StreamWriter writer, CancellationToken ct)
    {
        var degrees = (int)azimuth; // truncate, matching SDRLogger+ int(az)
        return writer.WriteAsync($"M{degrees:D3}\r");
    }

    public Task StopAsync(StreamReader reader, StreamWriter writer, CancellationToken ct)
        => writer.WriteAsync("S\r");
}
