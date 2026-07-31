using System.Globalization;

namespace SDRLoggerPlus.Server.Services.Rotator;

/// <summary>
/// hamlib rotctld TCP protocol (default port 4533):
/// <list type="bullet">
/// <item><c>p</c> → azimuth line, then elevation line.</item>
/// <item><c>P az el</c> → set position (elevation 0, azimuth-only), answered with RPRT.</item>
/// <item><c>S</c> → stop, answered with RPRT.</item>
/// </list>
///
/// Two things here exist because of what real controllers do rather than what the spec says.
///
/// Commands are terminated with a bare LF, not <c>WriteLine</c>'s platform newline. On
/// Windows that wrote <c>"p\r\n"</c>, and a controller that treats CR as its own terminator
/// sees a second, empty command and answers it — one extra line per poll, which used to
/// accumulate in the reader until the reported position was a minute stale. rotctld itself
/// accepts either terminator, so nothing is lost by sending the stricter one.
///
/// The elevation line is optional. Azimuth-only emulators answer <c>p</c> with a single
/// line; waiting unconditionally for a second one parked the service until unrelated
/// traffic happened to arrive. It is collected when offered, briefly, and skipped when not.
/// </summary>
public sealed class RotctldProtocol : IRotatorProtocol
{
    /// <summary>How long a controller has to answer a request at all.</summary>
    private static readonly TimeSpan ReplyTimeout = TimeSpan.FromSeconds(2);

    /// <summary>Grace for a trailing line we do not need, only tidy up (elevation, RPRT).</summary>
    private static readonly TimeSpan TrailingLineGrace = TimeSpan.FromMilliseconds(250);

    public async Task<double?> PollAzimuthAsync(IRotatorChannel channel, CancellationToken ct)
    {
        channel.Drain();
        await channel.WriteAsync("p\n", ct);

        var azLine = await channel.ReadLineAsync(ReplyTimeout, ct);
        if (azLine == null) return null;

        _ = await channel.ReadLineAsync(TrailingLineGrace, ct); // elevation — taken if sent, ignored

        return double.TryParse(azLine, NumberStyles.Float, CultureInfo.InvariantCulture, out var az)
            ? az : null;
    }

    public async Task SetAzimuthAsync(double azimuth, IRotatorChannel channel, CancellationToken ct)
    {
        channel.Drain();
        // Invariant culture: on a comma-decimal machine "P 270,6 0" is not a command rotctld
        // (or anything emulating it) understands.
        await channel.WriteAsync(
            FormattableString.Invariant($"P {azimuth:F1} 0\n"), ct);
        _ = await channel.ReadLineAsync(ReplyTimeout, ct); // RPRT
    }

    public async Task StopAsync(IRotatorChannel channel, CancellationToken ct)
    {
        channel.Drain();
        await channel.WriteAsync("S\n", ct);
        _ = await channel.ReadLineAsync(ReplyTimeout, ct); // RPRT
    }
}
