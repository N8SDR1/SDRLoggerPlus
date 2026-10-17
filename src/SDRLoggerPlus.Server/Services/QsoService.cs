using Microsoft.AspNetCore.SignalR;
using MongoDB.Bson;
using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Contracts.Events;
using SDRLoggerPlus.Server.Core.Database;
using SDRLoggerPlus.Server.Hubs;

namespace SDRLoggerPlus.Server.Services;

public class QsoService : IQsoService
{
    private readonly IQsoRepository _repository;
    private readonly IHubContext<LogHub, ILogHubClient> _hub;
    private readonly ISpotStatusService? _spotStatusService;
    private readonly ClubLogService? _clubLog;
    private readonly HrdLogService? _hrdLog;
    private readonly EqslService? _eqsl;
    private readonly IServiceScopeFactory? _scopeFactory;
    private readonly ILogger<QsoService>? _logger;

    public QsoService(IQsoRepository repository, IHubContext<LogHub, ILogHubClient> hub,
        ISpotStatusService? spotStatusService = null, ClubLogService? clubLog = null,
        HrdLogService? hrdLog = null, EqslService? eqsl = null,
        IServiceScopeFactory? scopeFactory = null, ILogger<QsoService>? logger = null)
    {
        _repository = repository;
        _hub = hub;
        _spotStatusService = spotStatusService;
        _clubLog = clubLog;
        _hrdLog = hrdLog;
        _eqsl = eqsl;
        _scopeFactory = scopeFactory;
        _logger = logger;
    }

    /// <summary>
    /// Run one upload in the background and write its outcome to the ledger.
    ///
    /// The work outlives the HTTP request that started it, so it resolves its
    /// own scope rather than capturing this instance's scoped repository —
    /// that repository belongs to a scope which is disposed the moment the
    /// response is sent.
    ///
    /// Nothing is awaited by the caller (logging must not block on a slow
    /// service), so this swallows and logs its own exceptions: an unobserved
    /// faulted task would otherwise die silently, which is the failure mode
    /// this whole feature exists to end.
    /// </summary>
    private void RecordUpload(string qsoId, string service, Func<Task<QslUploadResult>> upload)
    {
        _ = Task.Run(async () =>
        {
            try
            {
                var result = await upload();

                if (_scopeFactory == null) return;
                using var scope = _scopeFactory.CreateScope();
                var recorder = scope.ServiceProvider.GetRequiredService<Qsl.QslSyncRecorder>();
                await recorder.RecordAsync(qsoId, service, result);
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "QSL upload/ledger write failed for {Service}", service);
            }
        });
    }

    /// <summary>
    /// Auto-upload a freshly logged QSO to the QRZ logbook — OPT-IN via QrzSettings.AutoUploadOnLog.
    /// Kept separate from <see cref="RecordUpload"/> because QRZ tracks its own sync status (the returned
    /// QrzLogId is stamped on the QSO), not the QSL ledger the confirmation services use. Runs in a fresh
    /// scope so it outlives the request, and self-gates on the opt-in setting so a normal manual-sync
    /// user is unaffected.
    /// </summary>
    private void RecordQrzUpload(Qso qso)
    {
        if (_scopeFactory == null) return;
        _ = Task.Run(async () =>
        {
            try
            {
                using var scope = _scopeFactory.CreateScope();
                var settings = scope.ServiceProvider.GetRequiredService<ISettingsService>();
                if (!(await settings.GetSettingsAsync()).Qrz.AutoUploadOnLog) return; // opt-in only

                var qrz = scope.ServiceProvider.GetRequiredService<IQrzService>();
                var result = await qrz.UploadQsoAsync(qso);
                if (result.Success && !string.IsNullOrEmpty(result.LogId))
                {
                    var repo = scope.ServiceProvider.GetRequiredService<IQsoRepository>();
                    await repo.UpdateQrzSyncStatusAsync(qso.Id, result.LogId);
                }
            }
            catch (Exception ex)
            {
                _logger?.LogWarning(ex, "QRZ auto-upload on log failed");
            }
        });
    }

    /// <summary>
    /// How far back an identical (callsign, band, mode) entry counts as a
    /// probable duplicate. 30 minutes catches double-clicks, UDP re-logs and
    /// "did I already log him?" without flagging legit repeat contacts later
    /// in the day.
    /// </summary>
    internal static readonly TimeSpan DupeWindow = TimeSpan.FromMinutes(30);

    public async Task<QsoResponse?> GetByIdAsync(string id)
    {
        var qso = await _repository.GetByIdAsync(id);
        return qso is null ? null : MapToResponse(qso);
    }

    public async Task<QsoResponse?> CheckRecentDupeAsync(string callsign, string band, string mode)
    {
        if (string.IsNullOrWhiteSpace(callsign) ||
            string.IsNullOrWhiteSpace(band) ||
            string.IsNullOrWhiteSpace(mode))
        {
            return null;
        }

        var since = DateTime.UtcNow - DupeWindow;
        var qso = await _repository.FindRecentDuplicateAsync(callsign, band, mode, since);
        return qso is null ? null : MapToResponse(qso);
    }

    public async Task<QsoResponse?> GetMostRecentByCallsignAsync(string callsign)
    {
        if (string.IsNullOrWhiteSpace(callsign)) return null;
        var qso = await _repository.GetMostRecentByCallsignAsync(callsign.Trim());
        return qso is null ? null : MapToResponse(qso);
    }

    public async Task<PaginatedQsoResponse> GetQsosAsync(QsoSearchRequest request)
    {
        var (items, totalCount) = await _repository.SearchAsync(request);

        // A null Limit means the unbounded whole-log form: everything is one page.
        var pageSize = request.Limit ?? Math.Max(totalCount, 1);
        var totalPages = (int)Math.Ceiling(totalCount / (double)pageSize);
        var page = (request.Skip / pageSize) + 1;

        return new PaginatedQsoResponse(
            Items: items.Select(MapToResponse),
            TotalCount: totalCount,
            Page: page,
            PageSize: pageSize,
            TotalPages: totalPages
        );
    }

    public async Task<QsoResponse> CreateAsync(CreateQsoRequest request)
    {
        var qso = new Qso
        {
            Callsign = request.Callsign.ToUpperInvariant(),
            QsoDate = CanonicalUtc(request.QsoDate),
            TimeOn = request.TimeOn,
            Band = request.Band,
            Mode = request.Mode,
            Frequency = request.Frequency,
            RstSent = request.RstSent,
            RstRcvd = request.RstRcvd,
            Comment = request.Comment,
            // v1.x General-mode fields — Contest maps into ContestInfo.ContestId
            // (the free-text contest/event/park identifier the operator
            // types), Notes stays as a top-level free-text string, and Qth
            // sits on the nested StationInfo (worked-station QTH).
            Contest = !string.IsNullOrWhiteSpace(request.Contest)
                ? new ContestInfo { ContestId = request.Contest }
                : null,
            Notes = request.Notes,
            Station = new StationInfo
            {
                Name = request.Name,
                Qth = request.Qth,
                Grid = request.Grid,
                Country = request.Country,
                State = request.State,
                // Stored as the bare name; a "ST," prefix (ADIF CNTY style)
                // is stripped in case a caller sends the prefixed form.
                County = Counties.CountyNameNormalizer.SplitStatePrefix(request.County).County
                    is { Length: > 0 } county ? county : null,
            }
        };

        // POTA tagging — write the standard ADIF POTA fields into
        // AdifExtra so PotaStatistics picks them up and QSOs round-
        // trip through ADIF export cleanly.
        //   my_pota_ref = park the operator is activating
        //   pota_ref    = worked-station's park for P2P contacts
        if (!string.IsNullOrWhiteSpace(request.MyPotaRef) || !string.IsNullOrWhiteSpace(request.PotaRef))
        {
            qso.AdifExtra ??= new BsonDocument();
            if (!string.IsNullOrWhiteSpace(request.MyPotaRef))
                qso.AdifExtra["my_pota_ref"] = request.MyPotaRef.Trim().ToUpperInvariant();
            if (!string.IsNullOrWhiteSpace(request.PotaRef))
                qso.AdifExtra["pota_ref"] = request.PotaRef.Trim().ToUpperInvariant();
        }

        // SAT tagging — primary Frequency/Mode carry the uplink leg (what the
        // operator TX'd), the downlink leg + satellite name land in AdifExtra
        // as the ADIF-standard fields so LoTW satellite credit survives an
        // ADIF round-trip: sat_name, prop_mode=SAT, freq_rx (downlink MHz),
        // down_mode. When the caller supplied an uplink frequency/mode we
        // promote them to the top-level Frequency/Mode too.
        if (!string.IsNullOrWhiteSpace(request.Satellite) ||
            request.UplinkFreq.HasValue || request.DownlinkFreq.HasValue ||
            !string.IsNullOrWhiteSpace(request.UpMode) || !string.IsNullOrWhiteSpace(request.DownMode))
        {
            qso.AdifExtra ??= new BsonDocument();
            qso.AdifExtra["prop_mode"] = "SAT";
            if (!string.IsNullOrWhiteSpace(request.Satellite))
                qso.AdifExtra["sat_name"] = request.Satellite.Trim().ToUpperInvariant();
            // UplinkFreq/DownlinkFreq are MHz on the wire (see QsoDto). Qso.Frequency
            // is kHz, but freq_rx is exported raw so it stays MHz.
            if (request.UplinkFreq.HasValue)
            {
                qso.Frequency = request.UplinkFreq.Value * 1000.0;
                // Backstop: derive Band from the uplink (TX) leg when the caller
                // didn't supply one — a SAT QSO logged straight from the controller
                // has no rig to source the band, so it would otherwise store blank.
                if (string.IsNullOrWhiteSpace(qso.Band))
                    qso.Band = BandHelper.GetBandFromMhz(request.UplinkFreq.Value);
            }
            if (!string.IsNullOrWhiteSpace(request.UpMode))
                qso.Mode = request.UpMode;
            if (request.DownlinkFreq.HasValue)
                qso.AdifExtra["freq_rx"] = request.DownlinkFreq.Value;
            if (!string.IsNullOrWhiteSpace(request.DownMode))
                qso.AdifExtra["down_mode"] = request.DownMode;
        }

        var created = await _repository.CreateAsync(qso);

        // Fire-and-forget QSL uploads — logging must never block on (or fail
        // because of) a slow external service. Each service no-ops when
        // disabled, and Club Log self-blocks on auth failure.
        //
        // The results are no longer discarded: every attempt is written to the
        // QSO's ledger so "did this reach Club Log?" has an answer. That is the
        // whole point of R-1 — after the June frequency incident, months of
        // wrong uploads to three services could not be repaired because nothing
        // recorded what had been sent.
        // Only auto-upload to the operator's PERSONAL QSL services when this QSO carries no distinct
        // operating call. Casual QSOs (this path) always have a null station call, so this is a no-op
        // today; it future-proofs the moment a different-call (club/special) QSO ever reaches here so
        // it can't be silently uploaded under the personal identity. (== IsPersonalQso for this path.)
        if (string.IsNullOrWhiteSpace(created.Contest?.StationCallsign))
        {
            if (_clubLog != null)
                RecordUpload(created.Id, QslSyncLedger.ClubLogKey, () => _clubLog.UploadQsoAsync(created));

            if (_hrdLog != null)
                RecordUpload(created.Id, QslSyncLedger.HrdLogKey, () => _hrdLog.UploadQsoAsync(created));

            if (_eqsl != null)
                RecordUpload(created.Id, QslSyncLedger.EqslKey, () => _eqsl.UploadQsoAsync(created));

            // QRZ logbook — opt-in auto-upload (self-gates on QrzSettings.AutoUploadOnLog).
            RecordQrzUpload(created);
        }

        // Update spot status cache incrementally — including the grid, so a grid
        // you just worked stops showing as "needed" on the next decode.
        _spotStatusService?.OnQsoLogged(
            created.Callsign,
            created.Country,
            created.Band,
            created.Mode,
            request.Grid);

        // Broadcast to all clients via SignalR
        await _hub.BroadcastQso(new QsoLoggedEvent(
            created.Id,
            created.Callsign,
            created.QsoDate,
            created.TimeOn,
            created.Band,
            created.Mode,
            created.Frequency,
            created.RstSent,
            created.RstRcvd,
            created.Station?.Grid
        ));

        return MapToResponse(created);
    }

    public async Task<QsoResponse?> UpdateAsync(string id, UpdateQsoRequest request)
    {
        var existing = await _repository.GetByIdAsync(id);
        if (existing is null) return null;

        // Snapshot the fields QRZ actually uploads (exactly the data fields this method can edit).
        // If none of them change, this was a confirmation-only edit — persist it WITHOUT re-queuing
        // the QSO for QRZ re-upload (QRZ never carries our record of their confirmation). This holds
        // even when the edit dialog re-sends every field unchanged.
        var dataSigBefore = QrzDataSignature(existing);

        if (request.Callsign != null) existing.Callsign = request.Callsign.ToUpperInvariant();
        if (request.QsoDate.HasValue) existing.QsoDate = CanonicalUtc(request.QsoDate.Value);
        if (request.TimeOn != null) existing.TimeOn = request.TimeOn;
        if (request.Band != null) existing.Band = request.Band;
        if (request.Mode != null) existing.Mode = request.Mode;
        if (request.Frequency.HasValue) existing.Frequency = request.Frequency;
        if (request.RstSent != null) existing.RstSent = request.RstSent;
        if (request.RstRcvd != null) existing.RstRcvd = request.RstRcvd;
        if (request.Comment != null) existing.Comment = request.Comment;

        existing.Station ??= new StationInfo();
        // Name, Grid and Country exist BOTH at the top level and on Station, and
        // MapToResponse reads the top-level one first (`qso.Grid ?? Station.Grid`).
        // ADIF import populates both, so writing only Station left the top-level
        // value stale and every read handed back the old one — the edit was saved
        // and then invisibly overwritten on display (issue #34). Write both.
        // State and County were never affected: they live only on Station.
        if (request.Name != null) { existing.Name = request.Name; existing.Station.Name = request.Name; }
        if (request.Grid != null) { existing.Grid = request.Grid; existing.Station.Grid = request.Grid; }
        if (request.Country != null) { existing.Country = request.Country; existing.Station.Country = request.Country; }
        if (request.State != null) existing.Station.State = request.State;
        // Stripped the same way as on create: a caller may send the ADIF
        // "ST,County" form, but the field is stored as the bare name.
        if (request.County != null)
            existing.Station.County = Counties.CountyNameNormalizer.SplitStatePrefix(request.County).County
                is { Length: > 0 } county ? county : null;

        // Confirmation flags. Null means "this edit didn't touch it". A true/false
        // that already matches the stored Y/not-Y is also a no-op, so opening Edit
        // and saving a grid change cannot turn a stored "N" into a blank.
        if (request.LotwRcvd is bool lotw)
            ApplyReceived(existing, lotw, q => q.Lotw?.Rcvd, q => { q.Lotw ??= new LotwStatus(); },
                (q, v) => q.Lotw!.Rcvd = v, q => q.Lotw?.RcvdDate, (q, d) => q.Lotw!.RcvdDate = d);
        if (request.EqslRcvd is bool eqsl)
            ApplyReceived(existing, eqsl, q => q.Eqsl?.Rcvd, q => { q.Eqsl ??= new EqslStatus(); },
                (q, v) => q.Eqsl!.Rcvd = v, _ => null, null);
        if (request.QrzRcvd is bool qrz)
            ApplyQrzReceived(existing, qrz);
        if (request.CardRcvd is bool cardRcvd)
            ApplyReceived(existing, cardRcvd, q => q.Rcvd, _ => { },
                (q, v) => q.Rcvd = v, q => q.RcvdDate, (q, d) => q.RcvdDate = d);
        if (request.CardSent is bool cardSent)
            ApplyReceived(existing, cardSent, q => q.Sent, _ => { },
                (q, v) => q.Sent = v, q => q.SentDate, (q, d) => q.SentDate = d);

        // Re-queue for QRZ only when a field QRZ uploads actually changed. A confirmation-only edit
        // leaves the signature identical, so it's persisted flag-preserving (no UpdatedAt bump, no
        // Synced→Modified flip) — consistent with how the automatic confirmation merge writes.
        var dataChanged = QrzDataSignature(existing) != dataSigBefore;
        await _repository.UpdateAsync(id, existing, bumpSyncState: dataChanged);
        return MapToResponse(existing);
    }

    /// <summary>
    /// A stable signature of just the fields QRZ uploads (and that <see cref="UpdateAsync"/> can edit),
    /// so a QSO edit can tell a genuine data change from a confirmation-only change. Confirmation flags
    /// are deliberately excluded — QRZ never carries them, so changing one must not re-queue an upload.
    /// </summary>
    private static string QrzDataSignature(Qso q) => string.Join('\u001f',
        q.Callsign, q.QsoDate.Ticks, q.TimeOn, q.Band, q.Mode, q.Frequency,
        q.RstSent, q.RstRcvd, q.Comment,
        q.Name, q.Grid, q.Country,
        q.Station?.Name, q.Station?.Grid, q.Station?.Country, q.Station?.State, q.Station?.County);

    public async Task<bool> DeleteAsync(string id)
    {
        return await _repository.DeleteAsync(id);
    }

    public async Task<int> DeleteManyAsync(IEnumerable<string> ids)
    {
        return await _repository.DeleteManyAsync(ids);
    }

    public async Task<QsoStatistics> GetStatisticsAsync()
    {
        // Personal dashboard — exclude different-call (club/special) contest QSOs. myCall via a
        // scope (this is a cold path; no per-QSO cost).
        string? myCall = null;
        if (_scopeFactory != null)
        {
            using var scope = _scopeFactory.CreateScope();
            myCall = (await scope.ServiceProvider.GetRequiredService<ISettingsRepository>().GetAsync())?.Station?.Callsign;
        }
        return await _repository.GetStatisticsAsync(myCall);
    }

    /// <summary>
    /// Pin an incoming QSO time to the canonical UTC frame (see
    /// docs/design/timezone-architecture.md). The API contract is UTC, but a client
    /// can hand us any Kind: a <c>Z</c> instant (Utc) or an offset (Local) convert
    /// correctly; a <b>naive</b> value (Unspecified) is <b>assumed UTC</b> — the ham/ADIF
    /// convention — and labelled as such rather than shifted as if it were local time,
    /// which would move the instant by the server's offset (the historical double-shift bug).
    /// </summary>
    private static DateTime CanonicalUtc(DateTime value) =>
        value.Kind == DateTimeKind.Unspecified
            ? DateTime.SpecifyKind(value, DateTimeKind.Utc)
            : value.ToUniversalTime();

    private static QsoResponse MapToResponse(Qso qso) => new(
        qso.Id,
        qso.Callsign,
        qso.QsoDate,
        qso.TimeOn,
        qso.TimeOff,
        qso.Band,
        qso.Mode,
        qso.Frequency,
        qso.RstSent,
        qso.RstRcvd,
        new StationInfoDto(
            qso.Name ?? qso.Station?.Name,
            qso.Grid ?? qso.Station?.Grid,
            qso.Country ?? qso.Station?.Country,
            qso.Dxcc ?? qso.Station?.Dxcc,
            qso.Station?.State,
            qso.Station?.County,
            qso.Continent ?? qso.Station?.Continent,
            qso.Station?.Latitude,
            qso.Station?.Longitude
        ),
        qso.Comment,
        qso.CreatedAt,
        qso.Contest?.ContestId,
        Satellites.SatelliteResolver.Name(qso),
        string.Equals(qso.Qsl?.Lotw?.Rcvd, "Y", StringComparison.OrdinalIgnoreCase),
        string.Equals(qso.Qsl?.Eqsl?.Rcvd, "Y", StringComparison.OrdinalIgnoreCase),
        string.Equals(qso.Qsl?.Qrz?.Rcvd, "Y", StringComparison.OrdinalIgnoreCase),
        string.Equals(qso.Qsl?.Rcvd, "Y", StringComparison.OrdinalIgnoreCase),
        MapQslSync(qso.QslSync),
        qso.Station?.Qth,
        qso.Contest?.StationCallsign,
        string.Equals(qso.Qsl?.Sent, "Y", StringComparison.OrdinalIgnoreCase)
    );

    private static bool IsYes(string? value) =>
        string.Equals(value, "Y", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Turn a confirmation on or off. Dates are stamped only when a Y is new,
    /// and cleared only when that Y is removed, so an untouched "N" survives.
    /// </summary>
    private static void ApplyReceived(
        Qso qso,
        bool requested,
        Func<QslStatus, string?> current,
        Action<QslStatus> ensure,
        Action<QslStatus, string?> setValue,
        Func<QslStatus, DateTime?> currentDate,
        Action<QslStatus, DateTime?>? setDate)
    {
        var qsl = qso.Qsl;
        if (requested == IsYes(qsl is null ? null : current(qsl))) return;

        qso.Qsl ??= new QslStatus();
        ensure(qso.Qsl);
        if (requested)
        {
            setValue(qso.Qsl, "Y");
            if (setDate != null && currentDate(qso.Qsl) is null)
                setDate(qso.Qsl, DateTime.UtcNow);
        }
        else
        {
            setValue(qso.Qsl, null);
            setDate?.Invoke(qso.Qsl, null);
        }
    }

    /// <summary>
    /// QRZ Logbook's confirmation is APP_QRZLOG_STATUS=C on the wire and "Y" on
    /// <see cref="QrzStatus.Rcvd"/>. Keep the ADIF extra in step so a manual
    /// confirm exports once, and a manual clear does not leave a stale C behind.
    /// </summary>
    private static void ApplyQrzReceived(Qso qso, bool requested)
    {
        if (requested == IsYes(qso.Qsl?.Qrz?.Rcvd)) return;

        qso.Qsl ??= new QslStatus();
        qso.Qsl.Qrz ??= new QrzStatus();
        qso.Qsl.Qrz.Rcvd = requested ? "Y" : null;

        qso.AdifExtra ??= new BsonDocument();
        if (requested) qso.AdifExtra["app_qrzlog_status"] = "C";
        else qso.AdifExtra.Remove("app_qrzlog_status");
        if (qso.AdifExtra.ElementCount == 0) qso.AdifExtra = null;
    }

    /// <summary>
    /// Null in, null out — a QSO with no ledger predates upload tracking, and
    /// the UI has to be able to tell that apart from "nothing sent yet".
    /// </summary>
    internal static QslSyncDto? MapQslSync(QslSyncLedger? ledger) =>
        ledger is null
            ? null
            : new QslSyncDto(
                MapQslService(ledger.ClubLog),
                MapQslService(ledger.HrdLog),
                MapQslService(ledger.Eqsl));

    private static QslServiceSyncDto? MapQslService(QslServiceSync? entry) =>
        entry is null
            ? null
            : new QslServiceSyncDto(
                entry.Status.ToString(),
                entry.SyncedAt,
                entry.LastAttemptAt,
                entry.LastError,
                entry.FailureKind.ToString(),
                entry.Attempts,
                entry.IsRetryable);
}
