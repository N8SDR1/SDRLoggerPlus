namespace SDRLoggerPlus.Server.Services.Qrv;

/// <summary>
/// Which program asked Logger to save the QSO on this async flow.
/// WSJT-X UDP carries the sender id ("STD", "WSJT-X", …). A manual log,
/// including a Lyra {LOG} macro, leaves this unset — the macro is not a source.
/// </summary>
public static class QsoLoggingOrigin
{
    private static readonly AsyncLocal<string?> Current = new();

    public static string? ProgramId => Current.Value;

    public static IDisposable Use(string? programId) => new Scope(programId);

    private sealed class Scope : IDisposable
    {
        private readonly string? _previous;

        public Scope(string? programId)
        {
            _previous = Current.Value;
            Current.Value = string.IsNullOrWhiteSpace(programId) ? null : programId.Trim();
        }

        public void Dispose() => Current.Value = _previous;
    }
}
