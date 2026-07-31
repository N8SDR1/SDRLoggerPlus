namespace SDRLoggerPlus.Server.Services.Rotator;

/// <summary>
/// Wire-protocol strategy for an antenna rotator. The owning <see cref="RotatorService"/>
/// manages the TCP connection, polling loop, normalization and broadcasting; a protocol
/// implementation only translates the three operations to/from lines on a provided
/// <see cref="IRotatorChannel"/>. This keeps each protocol small and unit-testable against
/// a fake channel, and lets new controllers (gs232, easycomm, pstrotator, …) be added
/// without touching the service loop.
///
/// Every operation starts by draining the channel, so a controller that answered an earlier
/// request with more (or fewer) lines than the spec calls for cannot push this one off by
/// one. Waits are bounded: a reply that never comes yields null rather than blocking the
/// service forever.
/// </summary>
public interface IRotatorProtocol
{
    /// <summary>Query and return the current azimuth in degrees (raw, not normalized).
    /// Returns null if the controller gave no parseable answer this poll.</summary>
    Task<double?> PollAzimuthAsync(IRotatorChannel channel, CancellationToken ct);

    /// <summary>Command the rotator to a target azimuth (already normalized to [0,360)).</summary>
    Task SetAzimuthAsync(double azimuth, IRotatorChannel channel, CancellationToken ct);

    /// <summary>Stop rotation.</summary>
    Task StopAsync(IRotatorChannel channel, CancellationToken ct);
}
