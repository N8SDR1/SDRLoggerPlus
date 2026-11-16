using SDRLoggerPlus.Contracts.Models;

namespace SDRLoggerPlus.Server.Services.Qrv;

/// <summary>
/// Writes <see cref="Qso.RadioSource"/> at save time. Does not replace a source
/// already on the row, and does not invent one when the TCI session has not identified.
/// </summary>
public static class QsoRadioSourceStamp
{
    public static void Apply(Qso qso, TciRadioSourceTracker? tracker, string? preferredRadioId, string? loggingProgramId)
    {
        if (!string.IsNullOrEmpty(qso.RadioSource) || tracker == null) return;
        var source = QrvQsoTotals.Classify(tracker.View(preferredRadioId), loggingProgramId);
        if (source != null)
            qso.RadioSource = source;
    }
}

/// <summary>Set when a counted QSO is saved so the totals snapshot is posted.</summary>
public sealed class QrvQsoTotalsSignal
{
    private int _logged;

    public void NoteLogged() => Interlocked.Exchange(ref _logged, 1);

    public bool Consume() => Interlocked.Exchange(ref _logged, 0) == 1;
}
