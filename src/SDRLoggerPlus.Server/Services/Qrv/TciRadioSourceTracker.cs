namespace SDRLoggerPlus.Server.Services.Qrv;

/// <summary>
/// Live TCI identity per radio. Cleared on disconnect so a saved QSO is stamped
/// from the session that is up now, not from a radio that went away.
/// </summary>
public sealed class TciRadioSourceTracker
{
    private readonly object _gate = new();
    private readonly Dictionary<string, Session> _sessions = new(StringComparer.Ordinal);

    public void Connected(string radioId)
    {
        if (string.IsNullOrWhiteSpace(radioId)) return;
        lock (_gate)
        {
            _sessions[radioId] = new Session { Connected = true };
        }
    }

    public void Disconnected(string radioId)
    {
        if (string.IsNullOrWhiteSpace(radioId)) return;
        lock (_gate)
        {
            if (_sessions.TryGetValue(radioId, out var session))
            {
                session.Connected = false;
                session.Protocol = null;
                session.ComboLinked = false;
                session.StdIdentified = false;
            }
        }
    }

    public void NoteProtocol(string radioId, string? name)
    {
        if (string.IsNullOrWhiteSpace(radioId) || string.IsNullOrWhiteSpace(name)) return;
        lock (_gate)
        {
            var session = Touch(radioId);
            session.Protocol = name.Trim();
            if (session.Protocol.Equals(QrvQsoTotals.SourceStd, StringComparison.OrdinalIgnoreCase))
                session.StdIdentified = true;
        }
    }

    public void NoteCombo(string radioId, bool linked)
    {
        if (string.IsNullOrWhiteSpace(radioId)) return;
        lock (_gate)
        {
            Touch(radioId).ComboLinked = linked;
        }
    }

    public void NoteStd(string radioId)
    {
        if (string.IsNullOrWhiteSpace(radioId)) return;
        lock (_gate)
        {
            Touch(radioId).StdIdentified = true;
        }
    }

    /// <summary>
    /// The session to stamp. The log entry's radio wins when it is a live TCI
    /// session. Otherwise the only live TCI session is used. Two different
    /// live sessions and no selection is not a guess.
    /// </summary>
    public QrvQsoTotals.TciSession View(string? preferredRadioId)
    {
        lock (_gate)
        {
            var live = _sessions.Where(kv => kv.Value.Connected).ToList();
            if (!string.IsNullOrEmpty(preferredRadioId))
            {
                var match = live.FirstOrDefault(kv => kv.Key == preferredRadioId);
                if (match.Value != null) return match.Value.ToView();
            }
            if (live.Count == 1) return live[0].Value.ToView();
            if (live.Count > 1)
            {
                var first = live[0].Value.ToView();
                if (live.All(kv => kv.Value.ToView().Equals(first))) return first;
            }
            return QrvQsoTotals.TciSession.None;
        }
    }

    private Session Touch(string radioId)
    {
        if (!_sessions.TryGetValue(radioId, out var session))
        {
            session = new Session { Connected = true };
            _sessions[radioId] = session;
        }
        return session;
    }

    private sealed class Session
    {
        public bool Connected;
        public string? Protocol;
        public bool ComboLinked;
        public bool StdIdentified;

        public QrvQsoTotals.TciSession ToView() => new(Protocol, ComboLinked, StdIdentified);
    }
}
