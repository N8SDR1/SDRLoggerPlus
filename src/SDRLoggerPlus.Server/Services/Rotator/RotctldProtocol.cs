namespace SDRLoggerPlus.Server.Services.Rotator;

/// <summary>
/// hamlib rotctld TCP protocol (default port 4533). This is the behavior SDRLoggerPlus has
/// always used, extracted verbatim from RotatorService:
/// <list type="bullet">
/// <item><c>p</c> → azimuth line, then elevation line.</item>
/// <item><c>P az el</c> → set position (elevation 0, azimuth-only), reads an RPRT reply.</item>
/// <item><c>S</c> → stop, reads an RPRT reply.</item>
/// </list>
/// Writes use <see cref="StreamWriter.WriteLineAsync(string)"/> so the line terminator
/// matches the prior implementation; rotctld is lenient about CR/LF.
/// </summary>
public sealed class RotctldProtocol : IRotatorProtocol
{
    public async Task<double?> PollAzimuthAsync(StreamReader reader, StreamWriter writer, CancellationToken ct)
    {
        await writer.WriteLineAsync("p");
        var azLine = await reader.ReadLineAsync(ct);
        _ = await reader.ReadLineAsync(ct); // elevation line — azimuth-only, ignored
        return azLine != null && double.TryParse(azLine, out var az) ? az : null;
    }

    public async Task SetAzimuthAsync(double azimuth, StreamReader reader, StreamWriter writer, CancellationToken ct)
    {
        await writer.WriteLineAsync($"P {azimuth:F1} 0");
        _ = await reader.ReadLineAsync(ct); // RPRT response
    }

    public async Task StopAsync(StreamReader reader, StreamWriter writer, CancellationToken ct)
    {
        await writer.WriteLineAsync("S");
        _ = await reader.ReadLineAsync(ct); // RPRT response
    }
}
