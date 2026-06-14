namespace SDRLoggerPlus.Server.Services.Rotator;

/// <summary>
/// Wire-protocol strategy for an antenna rotator. The owning <see cref="RotatorService"/>
/// manages the TCP connection, polling loop, normalization and broadcasting; a protocol
/// implementation only translates the three operations to/from bytes on a provided
/// reader/writer pair. This keeps each protocol small and unit-testable on in-memory
/// streams, and lets new controllers (gs232, easycomm, pstrotator, …) be added without
/// touching the service loop.
/// </summary>
public interface IRotatorProtocol
{
    /// <summary>Query and return the current azimuth in degrees (raw, not normalized).
    /// Returns null if the controller gave no parseable answer this poll.</summary>
    Task<double?> PollAzimuthAsync(StreamReader reader, StreamWriter writer, CancellationToken ct);

    /// <summary>Command the rotator to a target azimuth (already normalized to [0,360)).</summary>
    Task SetAzimuthAsync(double azimuth, StreamReader reader, StreamWriter writer, CancellationToken ct);

    /// <summary>Stop rotation.</summary>
    Task StopAsync(StreamReader reader, StreamWriter writer, CancellationToken ct);
}
