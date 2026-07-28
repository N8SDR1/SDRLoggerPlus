using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Contracts.Models;

namespace SDRLoggerPlus.Server.Services;

/// <summary>
/// The single place that decides whether a QSO counts as confirmed.
///
/// There are two honest answers, which is why this exists rather than one
/// predicate: the operator's own "has anyone confirmed this?" view accepts every
/// channel, while ARRL awards credit only LoTW and paper cards. Issue #46 was
/// those two answers disagreeing across screens — the grid map painted a grid
/// solid green off an eQSL confirmation while FFMA, correctly, did not.
/// Both rules live here so the award screens and the maps can't drift apart again.
/// </summary>
public static class ConfirmationPolicy
{
    /// <summary>LoTW, paper QSL, eQSL, or QRZ Logbook — "somebody confirmed this".</summary>
    public static bool Any(Qso qso) =>
        IsLotwOrCard(qso) ||
        Received(qso.Qsl?.Eqsl?.Rcvd) ||
        Received(qso.Qsl?.Qrz?.Rcvd);

    /// <summary>
    /// LoTW or paper QSL only. ARRL accepts nothing else for VUCC/FFMA, so eQSL
    /// and QRZ Logbook confirmations deliberately do not count here.
    /// </summary>
    public static bool AwardRules(Qso qso) => IsLotwOrCard(qso);

    /// <summary>The predicate for a rule; <c>null</c> filters fall back to <see cref="Any"/>.</summary>
    public static Func<Qso, bool> For(ConfirmationRule rule) =>
        rule == ConfirmationRule.AwardRules ? AwardRules : Any;

    private static bool IsLotwOrCard(Qso qso) =>
        Received(qso.Qsl?.Lotw?.Rcvd) || Received(qso.Qsl?.Rcvd);

    // ADIF stores these as single-character flags (Y/N/R/I/V); only "Y" means a
    // confirmation is in hand. Unchanged from the predicate this replaced.
    private static bool Received(string? flag) =>
        string.Equals(flag, "Y", StringComparison.OrdinalIgnoreCase);
}
