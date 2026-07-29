namespace SDRLoggerPlus.Server.Services.Rotator;

/// <summary>
/// A line-oriented view of the rotator's socket, decoupled from it.
///
/// Protocols used to hold a <see cref="StreamReader"/> and read exactly the number of
/// lines the controller was *supposed* to send. That assumption is what broke against
/// PSTRotator: any controller that answers with one extra line (or one fewer) leaves the
/// reader permanently offset, and because a socket read has no timeout, the service either
/// falls further behind every poll or parks forever. Reconnecting was the only cure, which
/// is why toggling the rotator off and on "fixed" it.
///
/// So reading is now somebody else's job. A background loop drains the socket into a queue
/// and the protocol talks to that queue instead: it <see cref="Drain"/>s whatever arrived
/// unsolicited, writes its command, and waits a bounded time for the answer. A line the
/// controller never sends costs a timeout, not a hang; a line we did not expect is discarded
/// on the next request instead of accumulating.
/// </summary>
public interface IRotatorChannel
{
    /// <summary>
    /// Discard every line received but not yet consumed, returning how many there were.
    /// Called before each request: anything still queued answers an earlier one.
    /// </summary>
    int Drain();

    /// <summary>Send raw text. Callers include their own terminator.</summary>
    Task WriteAsync(string text, CancellationToken ct);

    /// <summary>
    /// The next line, or null if none arrives within <paramref name="timeout"/> or the
    /// connection has ended. Never throws on timeout — a silent controller is a normal
    /// outcome, not an error.
    /// </summary>
    Task<string?> ReadLineAsync(TimeSpan timeout, CancellationToken ct);
}
