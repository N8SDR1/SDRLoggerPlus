using SDRLoggerPlus.Contracts.Models;

namespace SDRLoggerPlus.Server.Services;

/// <summary>
/// The single rule that decides whether a QSO belongs to the operator's PERSONAL identity — i.e.
/// whether it may be uploaded to their personal QRZ/LoTW, count toward their personal awards, and
/// export with their personal call.
///
/// A QSO is personal iff it carries NO contest operating callsign (null — every legacy and casual
/// QSO), or that callsign equals the operator's own station call (trimmed, case-insensitive). A QSO
/// logged under a DIFFERENT call (club / /P / special-event contest) is NOT personal and must be
/// excluded from personal uploads and awards. This is the spine of the contest-log-separation work;
/// applied everywhere the log is swept for personal upload/awards/worked-before.
/// </summary>
public static class QsoOwnership
{
    public static bool IsPersonalQso(Qso q, string? myCall)
    {
        var stationCall = q.Contest?.StationCallsign;
        if (string.IsNullOrWhiteSpace(stationCall)) return true; // null == legacy/casual == personal
        return !string.IsNullOrWhiteSpace(myCall)
            && string.Equals(stationCall.Trim(), myCall.Trim(), StringComparison.OrdinalIgnoreCase);
    }
}
