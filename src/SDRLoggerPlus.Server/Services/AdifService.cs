using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.AspNetCore.SignalR;
using MongoDB.Bson;
using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Contracts.Events;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database;
using SDRLoggerPlus.Server.Hubs;
using SDRLoggerPlus.Server.Services.Adif;

namespace SDRLoggerPlus.Server.Services;

public partial class AdifService : IAdifService
{
    private readonly IQsoRepository _qsoRepository;
    private readonly ISettingsRepository _settingsRepository;
    private readonly IHubContext<LogHub, ILogHubClient> _hub;
    private readonly ILogger<AdifService> _logger;
    private readonly ISpotStatusService? _spotStatusService;

    // ADIF field pattern: <fieldname:length>value or <fieldname:length:type>value
    [GeneratedRegex(@"<(\w+):(\d+)(?::\w)?>([\s\S]*?)(?=<[A-Za-z_][^>]*?:\d+|\Z)", RegexOptions.IgnoreCase)]
    private static partial Regex AdifFieldPattern();

    // End of header marker
    [GeneratedRegex(@"<EOH>", RegexOptions.IgnoreCase)]
    private static partial Regex EndOfHeaderPattern();

    // End of record marker
    [GeneratedRegex(@"<EOR>", RegexOptions.IgnoreCase)]
    private static partial Regex EndOfRecordPattern();

    // Numeric fields that should be converted
    private static readonly HashSet<string> NumericFields = new(StringComparer.OrdinalIgnoreCase)
    {
        "freq", "freq_rx", "tx_pwr", "rx_pwr", "distance", "cqz", "ituz", "dxcc",
        "my_cq_zone", "my_dxcc", "my_itu_zone", "a_index", "k_index", "sfi"
    };

    public AdifService(
        IQsoRepository qsoRepository,
        ISettingsRepository settingsRepository,
        IHubContext<LogHub, ILogHubClient> hub,
        ILogger<AdifService> logger,
        ISpotStatusService? spotStatusService = null)
    {
        _qsoRepository = qsoRepository;
        _settingsRepository = settingsRepository;
        _hub = hub;
        _logger = logger;
        _spotStatusService = spotStatusService;
    }

    public IEnumerable<Qso> ParseAdif(string adifContent) => ParseAdif(adifContent, null);

    /// <summary>
    /// Parse ADIF, optionally collecting what had to be corrected or could not be understood.
    /// Pass a collection to get the detail; pass null when only the QSOs matter.
    /// </summary>
    public IEnumerable<Qso> ParseAdif(string adifContent, ICollection<AdifFieldIssue>? issues)
    {
        // Skip header if present
        var eohMatch = EndOfHeaderPattern().Match(adifContent);
        var content = eohMatch.Success ? adifContent[(eohMatch.Index + eohMatch.Length)..] : adifContent;

        // Split by end of record marker
        var records = EndOfRecordPattern().Split(content);

        foreach (var record in records)
        {
            var trimmedRecord = record.Trim();
            if (string.IsNullOrEmpty(trimmedRecord)) continue;

            var qso = ParseRecord(trimmedRecord, issues);
            if (qso != null)
            {
                yield return qso;
            }
        }
    }

    public IEnumerable<Qso> ParseAdif(Stream stream)
    {
        using var reader = new StreamReader(stream, Encoding.UTF8, detectEncodingFromByteOrderMarks: true);
        var content = reader.ReadToEnd();
        return ParseAdif(content).ToList();
    }

    public string ExportToAdif(IEnumerable<Qso> qsos, string? stationCallsign = null)
    {
        var sb = new StringBuilder();

        // ADIF Header
        sb.AppendLine("SDRLoggerPlus ADIF Export");
        sb.AppendLine($"Generated: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC");
        sb.AppendLine();
        AppendAdifField(sb, "ADIF_VER", "3.1.4");
        AppendAdifField(sb, "PROGRAMID", "SDRLoggerPlus");
        AppendAdifField(sb, "PROGRAMVERSION", "1.0");
        sb.AppendLine("<EOH>");
        sb.AppendLine();

        // QSO Records
        foreach (var qso in qsos)
        {
            ExportQsoRecord(sb, qso, stationCallsign);
            sb.AppendLine();
        }

        return sb.ToString();
    }

    public async Task<AdifImportResult> ImportAdifAsync(Stream stream, bool skipDuplicates = true, bool markAsSyncedToQrz = true, bool clearExistingLogs = false, CancellationToken cancellationToken = default)
    {
        using var reader = new StreamReader(stream, Encoding.UTF8, detectEncodingFromByteOrderMarks: true);
        var fieldIssues = new List<AdifFieldIssue>();
        var qsos = ParseAdif(reader.ReadToEnd(), fieldIssues).ToList();
        var importedCount = 0;
        var skippedDuplicates = 0;
        var errorCount = 0;
        var errors = new List<string>();

        _logger.LogInformation("Importing {Count} QSO records from ADIF (markAsSynced={Synced}, clearExisting={Clear})",
            qsos.Count, markAsSyncedToQrz, clearExistingLogs);

        // Send initial progress update
        await _hub.BroadcastAdifImportProgress(new AdifImportProgressEvent(
            qsos.Count, 0, 0, 0, 0, false, null, "Starting import..."));

        // Clear existing logs if requested
        if (clearExistingLogs)
        {
            var deletedCount = await _qsoRepository.DeleteAllAsync();
            _logger.LogInformation("Cleared {Count} existing QSOs before import", deletedCount);
        }

        // Build in-memory duplicate detection set for performance
        HashSet<string>? existingQsoKeys = null;
        if (skipDuplicates && !clearExistingLogs)
        {
            await _hub.BroadcastAdifImportProgress(new AdifImportProgressEvent(
                qsos.Count, 0, 0, 0, 0, false, null, "Building duplicate detection index..."));

            var existingQsos = await _qsoRepository.GetAllAsync();
            existingQsoKeys = existingQsos
                .Select(q => GetQsoKey(q.Callsign, q.QsoDate, q.TimeOn, q.Band, q.Mode))
                .ToHashSet();

            _logger.LogInformation("Built duplicate detection index with {Count} existing QSOs", existingQsoKeys.Count);
        }

        // Process QSOs in batches for optimal performance
        const int batchSize = 1000;
        var batches = qsos
            .Select((qso, index) => new { qso, index })
            .GroupBy(x => x.index / batchSize)
            .Select(g => g.Select(x => x.qso).ToList())
            .ToList();

        _logger.LogInformation("Processing {TotalQsos} QSOs in {BatchCount} batches of {BatchSize}",
            qsos.Count, batches.Count, batchSize);

        var processedCount = 0;

        foreach (var batch in batches)
        {
            // Check for cancellation
            cancellationToken.ThrowIfCancellationRequested();

            var qsosToImport = new List<Qso>();

            foreach (var qso in batch)
            {
                try
                {
                    // Check for duplicates using in-memory HashSet (much faster than DB queries)
                    if (existingQsoKeys != null)
                    {
                        var qsoKey = GetQsoKey(qso.Callsign, qso.QsoDate, qso.TimeOn, qso.Band, qso.Mode);
                        if (existingQsoKeys.Contains(qsoKey))
                        {
                            skippedDuplicates++;
                            processedCount++;
                            continue;
                        }

                        // Add to the set so we don't import duplicates within the same file
                        existingQsoKeys.Add(qsoKey);
                    }

                    qso.CreatedAt = DateTime.UtcNow;
                    qso.UpdatedAt = DateTime.UtcNow;

                    // Mark as already synced to QRZ if requested (useful for QRZ exports)
                    if (markAsSyncedToQrz)
                    {
                        qso.QrzSyncStatus = SyncStatus.Synced;
                        qso.QrzSyncedAt = DateTime.UtcNow;
                        // Use a placeholder ID to indicate it came from QRZ import
                        qso.QrzLogId = $"imported-{DateTime.UtcNow:yyyyMMddHHmmss}";
                    }

                    qsosToImport.Add(qso);
                }
                catch (Exception ex)
                {
                    errorCount++;
                    var errorMsg = $"{qso.Callsign}: {ex.Message}";
                    errors.Add(errorMsg);
                    _logger.LogWarning(ex, "Error preparing QSO for import: {Callsign}", qso.Callsign);
                }

                processedCount++;
            }

            // Bulk insert the entire batch with a single database operation
            if (qsosToImport.Count > 0)
            {
                try
                {
                    await _qsoRepository.CreateBulkAsync(qsosToImport);
                    importedCount += qsosToImport.Count;

                    _logger.LogInformation("Imported batch: {Imported} QSOs", qsosToImport.Count);
                }
                catch (Exception ex)
                {
                    errorCount += qsosToImport.Count;
                    var errorMsg = $"Batch import failed: {ex.Message}";
                    errors.Add(errorMsg);
                    _logger.LogError(ex, "Error importing batch of {Count} QSOs", qsosToImport.Count);
                }
            }

            // Send progress update after each batch
            await _hub.BroadcastAdifImportProgress(new AdifImportProgressEvent(
                qsos.Count,
                processedCount,
                importedCount,
                skippedDuplicates,
                errorCount,
                false,
                batch.LastOrDefault()?.Callsign,
                $"Processing batch {batches.IndexOf(batch) + 1} of {batches.Count}..."
            ));
        }

        _logger.LogInformation(
            "ADIF import complete: {Imported} imported, {Skipped} duplicates skipped, {Errors} errors",
            importedCount, skippedDuplicates, errorCount);

        // Rebuild spot status cache after import
        if (_spotStatusService != null && importedCount > 0)
        {
            _ = _spotStatusService.InvalidateCacheAsync();
        }

        // Send final progress update
        await _hub.BroadcastAdifImportProgress(new AdifImportProgressEvent(
            qsos.Count,
            qsos.Count,
            importedCount,
            skippedDuplicates,
            errorCount,
            true,
            null,
            errorCount > 0 ? $"Import complete with {errorCount} error(s)" : "Import complete"
        ));

        // Roll the per-record findings up into one line per distinct problem.
        var issueSummary = fieldIssues
            .GroupBy(i => (i.Field, i.Original, i.Result, i.Action, i.Note))
            .Select(g => new AdifImportIssueSummary(
                g.Key.Field, g.Key.Original, g.Key.Result, g.Key.Action.ToString(), g.Count(), g.Key.Note))
            .OrderByDescending(s => s.Count)
            .ToList();

        if (issueSummary.Count > 0)
        {
            _logger.LogInformation("ADIF import found {Distinct} distinct field issues across {Records} records",
                issueSummary.Count, issueSummary.Sum(s => s.Count));
        }

        return new AdifImportResult(
            qsos.Count, importedCount, skippedDuplicates, errorCount, errors, issueSummary);
    }

    /// <summary>
    /// Identity of a contact for duplicate detection: who, when, and on what band.
    ///
    /// Mode used to be part of this key, and that is what let 1,046 duplicate records into a
    /// 24,544-record log. The same contact exported by two programs arrives with two mode
    /// spellings — "DATA" and "MFSK", "PH" and "SSB", a truncated "FT2" and the "29" it was
    /// truncated from — and a key containing the mode reads those as two different QSOs, so
    /// the second one imports. Mode is the least reliable field in a real ADIF file and the
    /// worst possible thing to hang identity on.
    ///
    /// Band stays, through the normalizer so "40M" and "40m" collapse to one key. It is
    /// genuinely part of identity — the same station worked on two bands in the same minute is
    /// two contacts — and unlike mode it is verifiable against the frequency.
    /// </summary>
    private static string GetQsoKey(string callsign, DateTime qsoDate, string timeOn, string band, string mode)
    {
        _ = mode; // deliberately not part of identity — see above
        return $"{callsign.ToUpperInvariant()}|{qsoDate.Date:yyyyMMdd}|{timeOn}|{AdifFieldNormalizer.CanonicalBandKey(band)}";
    }

    /// <summary>
    /// Loose match key for confirmation merge — call + band + normalized mode,
    /// deliberately WITHOUT the date/time. The date is matched separately with a
    /// ±1-day tolerance (see PickNearestByDate) because ADIF dates are UTC while
    /// the log stores local wall-clock, so an evening QSO can land on a different
    /// calendar day. Standard confirmation-matching behaviour.
    /// </summary>
    private static string MergeKey(string callsign, string band, string mode)
    {
        return $"{callsign.ToUpperInvariant()}|{band.ToUpperInvariant()}|{NormalizeModeForMatch(mode)}";
    }

    /// <summary>Collapse sideband/sub-mode variants so a report matches the log.</summary>
    private static string NormalizeModeForMatch(string? mode)
    {
        if (string.IsNullOrEmpty(mode)) return "";
        var m = mode.ToUpperInvariant();
        return m switch
        {
            "USB" or "LSB" => "SSB",
            "PSK31" or "PSK63" or "PSK125" => "PSK",
            _ => m
        };
    }

    /// <summary>
    /// ADIF submode → parent mode, for the family fallback index. WSJT-X (and the
    /// operator) log the submode name; LoTW reports the parent with the submode in
    /// SUBMODE. Only families this log's mode set actually spans — widening a match
    /// is only safe where the parent/submode relationship is defined by the spec.
    /// </summary>
    private static readonly Dictionary<string, string> ModeFamilies = new(StringComparer.OrdinalIgnoreCase)
    {
        ["FT4"] = "MFSK",
        ["MSK144"] = "MFSK",
        ["JS8"] = "MFSK",
        ["FST4"] = "MFSK",
        ["Q65"] = "MFSK",
        ["JT65A"] = "JT65",
        ["JT65B"] = "JT65",
        ["JT65C"] = "JT65",
    };

    /// <summary>Like <see cref="MergeKey"/>, but with the mode collapsed to its ADIF family.</summary>
    private static string FamilyMergeKey(string callsign, string band, string mode)
    {
        var m = NormalizeModeForMatch(mode);
        if (ModeFamilies.TryGetValue(m, out var family)) m = family;
        return $"{callsign.ToUpperInvariant()}|{band.ToUpperInvariant()}|{m}";
    }

    /// <summary>SUBMODE from a parsed report record. Unmapped ADIF fields land in AdifExtra.</summary>
    private static string? GetReportSubmode(Qso rec) =>
        rec.AdifExtra != null && rec.AdifExtra.TryGetValue("submode", out var v)
            && v.AsString is { Length: > 0 } s ? s : null;

    private static void AddToIndex(Dictionary<string, List<Qso>> index, string key, Qso qso)
    {
        if (!index.TryGetValue(key, out var list)) index[key] = list = new List<Qso>();
        list.Add(qso);
    }

    private static Qso? Find(Dictionary<string, List<Qso>> index, string key, DateTime reportDate) =>
        index.TryGetValue(key, out var candidates) ? PickNearestByDate(candidates, reportDate) : null;

    /// <summary>
    /// The family-fallback lookup, keyed on the report's MODE. When the report also
    /// carries a SUBMODE this app knows, that submode is an explicit statement of the
    /// contact's mode — the only legitimate family targets are then QSOs logged as
    /// that submode (which the exact level already missed) or as the literal parent
    /// ("MFSK"). A different sibling — report says MSK144, log row says FT4 — is a
    /// different contact, and confirming it would invent a QSL. An unknown submode
    /// gives no such statement, so the whole family stays eligible.
    /// </summary>
    private static Qso? FindInFamily(Dictionary<string, List<Qso>> familyIndex, Qso rec, string? submode)
    {
        if (!familyIndex.TryGetValue(FamilyMergeKey(rec.Callsign, rec.Band, rec.Mode), out var candidates))
            return null;

        var sub = NormalizeModeForMatch(submode);
        if (sub.Length > 0 && ModeFamilies.TryGetValue(sub, out var parent))
        {
            candidates = candidates.Where(q =>
            {
                var m = NormalizeModeForMatch(q.Mode);
                return string.Equals(m, sub, StringComparison.OrdinalIgnoreCase)
                    || string.Equals(m, parent, StringComparison.OrdinalIgnoreCase);
            }).ToList();
            if (candidates.Count == 0) return null;
        }

        return PickNearestByDate(candidates, rec.QsoDate);
    }

    public async Task<ConfirmationMergeResponse> MergeConfirmationsAsync(
        Stream stream, ConfirmationSource source, CancellationToken cancellationToken = default)
    {
        var records = ParseAdif(stream).ToList();
        var existing = (await _qsoRepository.GetAllAsync()).ToList();

        _logger.LogInformation("Merging {Count} {Source} confirmation records against {Existing} logged QSOs",
            records.Count, source, existing.Count);

        // Index the log by call+band+mode; a key can hold many QSOs (worked the
        // same station repeatedly), so we keep a list and pick the nearest date.
        //
        // Indexed twice: by exact mode and by mode FAMILY. LoTW reports a submode
        // QSO under its ADIF parent (an MSK144 or FT4 QSO comes back MODE=MFSK,
        // with the real mode in SUBMODE), so the log's "MSK144" and the report's
        // "MFSK" never produce the same exact key — every such confirmation was
        // silently dropped as unmatched. The family index is the fallback that
        // catches those; exact matches are always tried first.
        var index = new Dictionary<string, List<Qso>>(StringComparer.OrdinalIgnoreCase);
        var familyIndex = new Dictionary<string, List<Qso>>(StringComparer.OrdinalIgnoreCase);
        foreach (var q in existing)
        {
            if (string.IsNullOrEmpty(q.Callsign) || string.IsNullOrEmpty(q.Band) || string.IsNullOrEmpty(q.Mode))
                continue;
            AddToIndex(index, MergeKey(q.Callsign, q.Band, q.Mode), q);
            AddToIndex(familyIndex, FamilyMergeKey(q.Callsign, q.Band, q.Mode), q);
        }

        int matched = 0, updated = 0, alreadyConfirmed = 0, unmatched = 0;
        var toUpdate = new List<Qso>();

        foreach (var rec in records)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (string.IsNullOrEmpty(rec.Callsign) || string.IsNullOrEmpty(rec.Band) || string.IsNullOrEmpty(rec.Mode))
            {
                unmatched++;
                continue;
            }

            // A QRZ logbook export lists unconfirmed QSOs too — skip anything QRZ
            // doesn't mark confirmed. LoTW / eQSL downloads are confirmed-only, so
            // every matched record counts for those.
            if (source == ConfirmationSource.Qrz &&
                !string.Equals(rec.Qsl?.Qrz?.Rcvd, "Y", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            // Most-specific first. SUBMODE, when the report carries one, is the truest
            // description of the QSO ("MFSK" + "FT4" means the log likely says FT4),
            // then the report's own MODE, then the family fallback for the
            // submode-vs-parent spelling gap. An exact hit always takes precedence
            // over a family hit, and the family level filters by the report's own
            // submode claim — see FindInFamily.
            var submode = GetReportSubmode(rec);
            var target =
                (submode is not null ? Find(index, MergeKey(rec.Callsign, rec.Band, submode), rec.QsoDate) : null)
                ?? Find(index, MergeKey(rec.Callsign, rec.Band, rec.Mode), rec.QsoDate)
                ?? FindInFamily(familyIndex, rec, submode);
            if (target is null)
            {
                unmatched++;
                continue;
            }

            matched++;
            if (ApplyConfirmation(target, source, rec))
            {
                toUpdate.Add(target);
            }
            else
            {
                alreadyConfirmed++;
            }
        }

        // Persist only the records we actually changed.
        foreach (var q in toUpdate)
        {
            if (string.IsNullOrEmpty(q.Id)) continue;
            q.UpdatedAt = DateTime.UtcNow;
            await _qsoRepository.UpdateAsync(q.Id, q);
            updated++;
        }

        _logger.LogInformation(
            "{Source} merge complete: {Matched} matched, {Updated} newly confirmed, {Already} already, {Unmatched} unmatched",
            source, matched, updated, alreadyConfirmed, unmatched);

        return new ConfirmationMergeResponse(records.Count, matched, updated, alreadyConfirmed, unmatched);
    }

    /// <summary>
    /// Pick the logged QSO closest in time to the report record, within ~1 day.
    /// The window absorbs the UTC-vs-local calendar-day rollover (ADIF dates are
    /// UTC; the log stores local wall-clock), which otherwise drops evening QSOs.
    /// Returns null when no candidate is close enough — a genuinely different QSO.
    /// </summary>
    private static Qso? PickNearestByDate(List<Qso> candidates, DateTime reportDate)
    {
        Qso? best = null;
        var bestHours = double.MaxValue;
        foreach (var c in candidates)
        {
            var hours = Math.Abs((c.QsoDate - reportDate).TotalHours);
            if (hours < bestHours)
            {
                bestHours = hours;
                best = c;
            }
        }
        return bestHours <= 26 ? best : null;
    }

    /// <summary>
    /// Stamp the confirmation channel on a matched QSO. Returns true if it was a
    /// new confirmation, false if the QSO was already confirmed by that channel.
    /// </summary>
    private static bool ApplyConfirmation(Qso qso, ConfirmationSource source, Qso report)
    {
        qso.Qsl ??= new QslStatus();
        switch (source)
        {
            case ConfirmationSource.Lotw:
                qso.Qsl.Lotw ??= new LotwStatus();
                if (string.Equals(qso.Qsl.Lotw.Rcvd, "Y", StringComparison.OrdinalIgnoreCase)) return false;
                qso.Qsl.Lotw.Rcvd = "Y";
                qso.Qsl.Lotw.RcvdDate = report.Qsl?.Lotw?.RcvdDate ?? report.Qsl?.RcvdDate ?? DateTime.UtcNow;
                qso.LotwSyncStatus = SyncStatus.Synced;
                return true;

            case ConfirmationSource.Eqsl:
                qso.Qsl.Eqsl ??= new EqslStatus();
                if (string.Equals(qso.Qsl.Eqsl.Rcvd, "Y", StringComparison.OrdinalIgnoreCase)) return false;
                qso.Qsl.Eqsl.Rcvd = "Y";
                return true;

            case ConfirmationSource.Qrz:
                qso.Qsl.Qrz ??= new QrzStatus();
                if (string.Equals(qso.Qsl.Qrz.Rcvd, "Y", StringComparison.OrdinalIgnoreCase)) return false;
                qso.Qsl.Qrz.Rcvd = "Y";
                return true;

            case ConfirmationSource.Card:
                if (string.Equals(qso.Qsl.Rcvd, "Y", StringComparison.OrdinalIgnoreCase)) return false;
                qso.Qsl.Rcvd = "Y";
                qso.Qsl.RcvdDate = report.Qsl?.RcvdDate ?? DateTime.UtcNow;
                return true;

            default:
                return false;
        }
    }

    public async Task<string> ExportQsosAsync(AdifExportRequest? request = null)
    {
        // Export must NEVER truncate. This is the path people use as a backup, and
        // a "large" cap here once silently dropped everything past 100k QSOs — an
        // export that completes successfully while missing contacts is the worst
        // failure mode a backup can have.
        var searchRequest = new QsoSearchRequest(
            Callsign: request?.Callsign,
            Band: request?.Band,
            Mode: request?.Mode,
            FromDate: request?.FromDate,
            ToDate: request?.ToDate,
            Limit: null
        );

        var (qsos, _) = await _qsoRepository.SearchAsync(searchRequest);
        var qsoList = qsos.ToList();

        // If specific IDs were requested, filter to those
        if (request?.QsoIds?.Any() == true)
        {
            var idSet = new HashSet<string>(request.QsoIds);
            qsoList = qsoList.Where(q => idSet.Contains(q.Id)).ToList();
        }

        var settings = await _settingsRepository.GetAsync() ?? new UserSettings();
        return ExportToAdif(qsoList, settings.Station.Callsign);
    }

    private Qso? ParseRecord(string record, ICollection<AdifFieldIssue>? issues = null)
    {
        var fields = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
        var extraFields = new BsonDocument();

        foreach (Match match in AdifFieldPattern().Matches(record))
        {
            var fieldName = match.Groups[1].Value.ToLowerInvariant();
            var length = int.Parse(match.Groups[2].Value);
            var rawValue = match.Groups[3].Value;

            // Truncate to specified length
            var value = rawValue.Length > length ? rawValue[..length] : rawValue;
            value = value.Trim();

            if (string.IsNullOrEmpty(value)) continue;

            // Convert numeric fields
            if (NumericFields.Contains(fieldName))
            {
                double? dVal = null;
                if (double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsedDouble))
                {
                    dVal = parsedDouble;
                }

                if (dVal.HasValue)
                {
                    // ADIF FREQ is MHz; Qso.Frequency is kHz, so scale it here.
                    // FREQ_RX is NOT scaled: it round-trips through AdifExtra,
                    // which is exported verbatim, so it must stay in MHz.
                    if (fieldName == "freq")
                    {
                        dVal *= 1000.0;
                    }
                    fields[fieldName] = dVal.Value;
                }
            }
            else
            {
                fields[fieldName] = value;
            }
        }

        // Validate required fields
        if (!fields.TryGetValue("call", out var callObj) || callObj is not string call)
        {
            _logger.LogWarning("Skipping ADIF record: missing CALL field");
            return null;
        }

        if (!fields.TryGetValue("qso_date", out var dateObj) || dateObj is not string dateStr)
        {
            _logger.LogWarning("Skipping ADIF record for {Call}: missing QSO_DATE field", call);
            return null;
        }

        // Parse date and time
        DateTime qsoDateTime;
        if (fields.TryGetValue("time_on", out var timeObj) && timeObj is string timeStr)
        {
            var paddedTime = timeStr.PadRight(6, '0')[..6];
            if (!DateTime.TryParseExact($"{dateStr}{paddedTime}", "yyyyMMddHHmmss",
                CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out qsoDateTime))
            {
                if (!DateTime.TryParseExact(dateStr, "yyyyMMdd",
                    CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out qsoDateTime))
                {
                    _logger.LogWarning("Skipping ADIF record for {Call}: invalid date format", call);
                    return null;
                }
            }
        }
        else
        {
            if (!DateTime.TryParseExact(dateStr, "yyyyMMdd",
                CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out qsoDateTime))
            {
                _logger.LogWarning("Skipping ADIF record for {Call}: invalid date format", call);
                return null;
            }
        }

        // Extract DXCC data from ADIF fields if present
        var country = GetStringField(fields, "country");
        var continent = GetStringField(fields, "cont");
        var dxcc = GetIntField(fields, "dxcc");

        // If DXCC data is missing or incomplete, look it up from the callsign
        if (string.IsNullOrEmpty(country) || string.IsNullOrEmpty(continent))
        {
            var (lookedUpCountry, lookedUpContinent) = CtyService.GetCountryFromCallsign(call);

            // Use looked-up values for any missing fields
            country ??= lookedUpCountry;
            continent ??= lookedUpContinent;

            // If country is provided in ADIF but continent isn't, also try country-to-continent lookup
            if (!string.IsNullOrEmpty(country) && string.IsNullOrEmpty(continent))
            {
                continent = CtyService.GetContinentFromCountryName(country);
            }
        }

        // Canonicalise band and mode before they reach the database. Until this existed, both
        // were stored exactly as written: this log ended up holding "40M" and "40m" as separate
        // spellings across 13,106 records, and 1,130 records with modes the ADIF enumeration
        // does not define. Anything unrecognised is kept verbatim and reported, never guessed at.
        //
        // The stored fallbacks below (20m, SSB, Unknown) predate this code and are kept so the
        // change is report-only — but they are fabrications, so each one is now flagged. A
        // band derived from FREQ is not flagged: frequency is the trustworthy field.
        var rawBand = GetStringField(fields, "band");
        var rawMode = GetStringField(fields, "mode");
        var freqKhz = GetDoubleField(fields, "freq");

        string bandInput;
        AdifFieldIssue? bandOriginIssue = null;
        if (!string.IsNullOrWhiteSpace(rawBand))
        {
            bandInput = rawBand;
        }
        else if (freqKhz.HasValue)
        {
            // FREQ is in kHz; BandHelper wants Hz.
            bandInput = BandHelper.GetBand((long)(freqKhz.Value * 1000.0));
            if (bandInput == "Unknown")
            {
                bandOriginIssue = new AdifFieldIssue(
                    "band", $"FREQ {freqKhz.Value}", "Unknown", AdifFieldAction.Flagged,
                    $"No BAND field, and FREQ {freqKhz.Value} kHz sits outside every amateur band. " +
                    "Stored as 'Unknown'.");
            }
        }
        else
        {
            bandInput = "20m";
            bandOriginIssue = new AdifFieldIssue(
                "band", "(missing)", "20m", AdifFieldAction.Flagged,
                "Record has neither BAND nor FREQ. Stored as '20m', the importer's long-standing " +
                "default — almost certainly wrong. Check the source log.");
        }

        string modeInput;
        AdifFieldIssue? modeOriginIssue = null;
        if (!string.IsNullOrWhiteSpace(rawMode))
        {
            modeInput = rawMode;
        }
        else
        {
            modeInput = "SSB";
            modeOriginIssue = new AdifFieldIssue(
                "mode", "(missing)", "SSB", AdifFieldAction.Flagged,
                "Record has no MODE. Stored as 'SSB', the importer's long-standing default.");
        }

        var (band, bandIssue) = AdifFieldNormalizer.NormalizeBand(bandInput);
        var (mode, modeIssue) = AdifFieldNormalizer.NormalizeMode(modeInput);
        // An origin issue supersedes the normalizer's: "this value was fabricated" explains
        // more than "this value is unrecognised" ever could.
        if ((bandOriginIssue ?? bandIssue) is { } bi) issues?.Add(bi);
        if ((modeOriginIssue ?? modeIssue) is { } mi) issues?.Add(mi);

        var qso = new Qso
        {
            Callsign = call.ToUpperInvariant(),
            QsoDate = DateTime.SpecifyKind(qsoDateTime, DateTimeKind.Utc),
            TimeOn = GetStringField(fields, "time_on") ?? qsoDateTime.ToString("HHmm"),
            TimeOff = GetStringField(fields, "time_off"),
            Band = band,
            Mode = mode,
            Frequency = GetDoubleField(fields, "freq"),
            RstSent = GetStringField(fields, "rst_sent"),
            RstRcvd = GetStringField(fields, "rst_rcvd"),
            Name = GetStringField(fields, "name"),
            Country = country,
            Grid = GetStringField(fields, "gridsquare"),
            Dxcc = dxcc,
            Continent = continent,
            Comment = GetStringField(fields, "comment"),
            Notes = GetStringField(fields, "notes"),
            Station = new StationInfo
            {
                Name = GetStringField(fields, "name"),
                Grid = GetStringField(fields, "gridsquare"),
                Country = country,
                Dxcc = dxcc,
                CqZone = GetIntField(fields, "cqz"),
                ItuZone = GetIntField(fields, "ituz"),
                State = GetStringField(fields, "state"),
                // ADIF carries CNTY as "ST,County Name". Store the county part
                // only; the state is already its own field, and keeping the
                // prefix would make every comparison strip it again.
                County = Counties.CountyNameNormalizer.SplitStatePrefix(GetStringField(fields, "cnty")).County
                    is { Length: > 0 } county ? county : null,
                Continent = continent,
                Latitude = GetDoubleField(fields, "lat"),
                Longitude = GetDoubleField(fields, "lon")
            },
            Qsl = new QslStatus
            {
                Sent = GetStringField(fields, "qsl_sent"),
                SentDate = ParseDateField(GetStringField(fields, "qslsdate")),
                Rcvd = GetStringField(fields, "qsl_rcvd"),
                RcvdDate = ParseDateField(GetStringField(fields, "qslrdate")),
                Lotw = new LotwStatus
                {
                    Sent = GetStringField(fields, "lotw_qsl_sent"),
                    Rcvd = GetStringField(fields, "lotw_qsl_rcvd")
                },
                Eqsl = new EqslStatus
                {
                    Sent = GetStringField(fields, "eqsl_qsl_sent"),
                    Rcvd = GetStringField(fields, "eqsl_qsl_rcvd")
                },
                // QRZ Logbook exposes its confirmation as app_qrzlog_status = C.
                Qrz = new QrzStatus
                {
                    Rcvd = string.Equals(GetStringField(fields, "app_qrzlog_status"), "C", StringComparison.OrdinalIgnoreCase) ? "Y" : null
                }
            },
            Contest = fields.ContainsKey("contest_id") ? new ContestInfo
            {
                ContestId = GetStringField(fields, "contest_id"),
                SerialSent = GetStringField(fields, "stx") ?? GetStringField(fields, "stx_string"),
                SerialRcvd = GetStringField(fields, "srx") ?? GetStringField(fields, "srx_string"),
                Exchange = GetStringField(fields, "srx_string")
            } : null
        };

        // Store unmapped ADIF fields in AdifExtra
        var mappedFields = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "call", "qso_date", "time_on", "time_off", "band", "mode", "freq",
            "rst_sent", "rst_rcvd", "name", "country", "gridsquare", "dxcc", "cont",
            "comment", "notes", "cqz", "ituz", "state", "cnty", "lat", "lon",
            "qsl_sent", "qslsdate", "qsl_rcvd", "qslrdate",
            "lotw_qsl_sent", "lotw_qsl_rcvd", "eqsl_qsl_sent", "eqsl_qsl_rcvd",
            "contest_id", "stx", "stx_string", "srx", "srx_string"
        };

        foreach (var kvp in fields)
        {
            if (!mappedFields.Contains(kvp.Key))
            {
                // Convert all extra fields to strings to avoid BSON type conflicts
                // (e.g., when importing "Y"/"N" values that might be stored as BsonBoolean elsewhere)
                extraFields[kvp.Key] = kvp.Value?.ToString() ?? string.Empty;
            }
        }

        if (extraFields.ElementCount > 0)
        {
            qso.AdifExtra = extraFields;
        }

        return qso;
    }

    private void ExportQsoRecord(StringBuilder sb, Qso qso, string? stationCallsign)
    {
        // Required fields. BAND and MODE must be uppercase — LOTW's ingestion silently drops
        // QSOs with lowercase band values like "20m" even though the ADIF spec says enums are
        // case-insensitive. Every mainstream logger writes uppercase here.
        //
        // QSO_DATE and TIME_ON must be in UTC and must agree. We derive both from the same
        // ToUniversalTime() DateTime so they can't drift. Previously we output QSO_DATE via
        // qso.QsoDate.ToString(...) which, if LiteDB round-tripped the DateTime as Kind=Local,
        // would produce the local date (e.g. Apr 17 in Ireland BST) while TIME_ON was the UTC
        // time (23:52 Apr 16). The mismatch looked to LOTW like a QSO in the future, so it
        // silently dropped the record during ingestion.
        var qsoUtc = qso.QsoDate.ToUniversalTime();
        AppendAdifField(sb, "CALL", qso.Callsign?.ToUpperInvariant());
        AppendAdifField(sb, "QSO_DATE", qsoUtc.ToString("yyyyMMdd"));
        AppendAdifField(sb, "TIME_ON", qsoUtc.ToString("HHmmss"));
        AppendAdifField(sb, "BAND", qso.Band?.ToUpperInvariant());
        AppendAdifField(sb, "MODE", qso.Mode?.ToUpperInvariant());

        // Optional standard fields
        if (!string.IsNullOrEmpty(qso.TimeOff))
            AppendAdifField(sb, "TIME_OFF", FormatTime(qso.TimeOff));

        if (qso.Frequency.HasValue)
            AppendAdifField(sb, "FREQ", (qso.Frequency.Value / 1000.0).ToString("F7", CultureInfo.InvariantCulture));

        if (!string.IsNullOrEmpty(qso.RstSent))
            AppendAdifField(sb, "RST_SENT", qso.RstSent);

        if (!string.IsNullOrEmpty(qso.RstRcvd))
            AppendAdifField(sb, "RST_RCVD", qso.RstRcvd);

        // Station info
        var name = qso.Name ?? qso.Station?.Name;
        if (!string.IsNullOrEmpty(name))
            AppendAdifField(sb, "NAME", name);

        var grid = qso.Grid ?? qso.Station?.Grid;
        if (!string.IsNullOrEmpty(grid))
            AppendAdifField(sb, "GRIDSQUARE", grid);

        var country = qso.Country ?? qso.Station?.Country;
        if (!string.IsNullOrEmpty(country))
            AppendAdifField(sb, "COUNTRY", country);

        var dxcc = qso.Dxcc ?? qso.Station?.Dxcc;
        if (dxcc.HasValue)
            AppendAdifField(sb, "DXCC", dxcc.Value.ToString());

        var continent = qso.Continent ?? qso.Station?.Continent;
        if (!string.IsNullOrEmpty(continent))
            AppendAdifField(sb, "CONT", continent);

        if (qso.Station?.CqZone.HasValue == true)
            AppendAdifField(sb, "CQZ", qso.Station.CqZone.Value.ToString());

        if (qso.Station?.ItuZone.HasValue == true)
            AppendAdifField(sb, "ITUZ", qso.Station.ItuZone.Value.ToString());

        if (!string.IsNullOrEmpty(qso.Station?.State))
            AppendAdifField(sb, "STATE", qso.Station.State);

        // ADIF convention is CNTY = "ST,County Name". We store the bare county,
        // so re-attach the state on the way out; without a state there is no
        // valid CNTY to emit.
        if (!string.IsNullOrEmpty(qso.Station?.County) && !string.IsNullOrEmpty(qso.Station?.State))
            AppendAdifField(sb, "CNTY", $"{qso.Station.State},{qso.Station.County}");

        if (qso.Station?.Latitude.HasValue == true)
            AppendAdifField(sb, "LAT", qso.Station.Latitude.Value.ToString("F6", CultureInfo.InvariantCulture));

        if (qso.Station?.Longitude.HasValue == true)
            AppendAdifField(sb, "LON", qso.Station.Longitude.Value.ToString("F6", CultureInfo.InvariantCulture));

        // Comments
        if (!string.IsNullOrEmpty(qso.Comment))
            AppendAdifField(sb, "COMMENT", qso.Comment);

        if (!string.IsNullOrEmpty(qso.Notes))
            AppendAdifField(sb, "NOTES", qso.Notes);

        // QSL status
        if (qso.Qsl != null)
        {
            if (!string.IsNullOrEmpty(qso.Qsl.Sent))
                AppendAdifField(sb, "QSL_SENT", qso.Qsl.Sent);

            if (qso.Qsl.SentDate.HasValue)
                AppendAdifField(sb, "QSLSDATE", qso.Qsl.SentDate.Value.ToString("yyyyMMdd"));

            if (!string.IsNullOrEmpty(qso.Qsl.Rcvd))
                AppendAdifField(sb, "QSL_RCVD", qso.Qsl.Rcvd);

            if (qso.Qsl.RcvdDate.HasValue)
                AppendAdifField(sb, "QSLRDATE", qso.Qsl.RcvdDate.Value.ToString("yyyyMMdd"));

            if (qso.Qsl.Lotw != null)
            {
                if (!string.IsNullOrEmpty(qso.Qsl.Lotw.Sent))
                    AppendAdifField(sb, "LOTW_QSL_SENT", qso.Qsl.Lotw.Sent);

                if (!string.IsNullOrEmpty(qso.Qsl.Lotw.Rcvd))
                    AppendAdifField(sb, "LOTW_QSL_RCVD", qso.Qsl.Lotw.Rcvd);
            }

            if (qso.Qsl.Eqsl != null)
            {
                if (!string.IsNullOrEmpty(qso.Qsl.Eqsl.Sent))
                    AppendAdifField(sb, "EQSL_QSL_SENT", qso.Qsl.Eqsl.Sent);

                if (!string.IsNullOrEmpty(qso.Qsl.Eqsl.Rcvd))
                    AppendAdifField(sb, "EQSL_QSL_RCVD", qso.Qsl.Eqsl.Rcvd);
            }
        }

        // Contest info
        if (qso.Contest != null)
        {
            if (!string.IsNullOrEmpty(qso.Contest.ContestId))
                AppendAdifField(sb, "CONTEST_ID", qso.Contest.ContestId);

            if (!string.IsNullOrEmpty(qso.Contest.SerialSent))
                AppendAdifField(sb, "STX_STRING", qso.Contest.SerialSent);

            if (!string.IsNullOrEmpty(qso.Contest.SerialRcvd))
                AppendAdifField(sb, "SRX_STRING", qso.Contest.SerialRcvd);
        }

        // Station callsign — a contest QSO carries the call it was actually made under (club /P /
        // special); fall back to the global export call for casual/legacy QSOs.
        var qsoStationCall = qso.Contest?.StationCallsign ?? stationCallsign;
        if (!string.IsNullOrEmpty(qsoStationCall))
            AppendAdifField(sb, "STATION_CALLSIGN", qsoStationCall);

        // Export extra ADIF fields that were preserved during import
        if (qso.AdifExtra != null)
        {
            // Whether a CNTY was already written above (from Station.County).
            var cntyEmitted = !string.IsNullOrEmpty(qso.Station?.County) && !string.IsNullOrEmpty(qso.Station?.State);
            foreach (var element in qso.AdifExtra)
            {
                // Legacy QSOs (imported before CNTY was a mapped field) carry
                // their county here, and exporting it verbatim is what keeps
                // their ADIF round-trip whole. But if the canonical field was
                // also emitted, a second CNTY would be a duplicate — and the
                // stale copy at that.
                if (cntyEmitted && string.Equals(element.Name, "cnty", StringComparison.OrdinalIgnoreCase))
                    continue;

                var value = element.Value?.ToString();
                if (!string.IsNullOrEmpty(value))
                {
                    AppendAdifField(sb, element.Name.ToUpperInvariant(), value);
                }
            }
        }

        sb.Append("<EOR>");
    }

    private static void AppendAdifField(StringBuilder sb, string fieldName, string? value)
    {
        if (string.IsNullOrEmpty(value)) return;
        sb.Append($"<{fieldName}:{value.Length}>{value}");
    }

    private static string? GetStringField(Dictionary<string, object> fields, string key)
    {
        return fields.TryGetValue(key, out var value) && value is string str ? str : null;
    }

    private static double? GetDoubleField(Dictionary<string, object> fields, string key)
    {
        if (!fields.TryGetValue(key, out var value)) return null;
        return value switch
        {
            double d => d,
            int i => i,
            string s when double.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out var d) => d,
            _ => null
        };
    }

    private static int? GetIntField(Dictionary<string, object> fields, string key)
    {
        if (!fields.TryGetValue(key, out var value)) return null;
        return value switch
        {
            int i => i,
            double d => (int)d,
            string s when int.TryParse(s, out var i) => i,
            _ => null
        };
    }

    private static DateTime? ParseDateField(string? dateStr)
    {
        if (string.IsNullOrEmpty(dateStr)) return null;
        return DateTime.TryParseExact(dateStr, "yyyyMMdd",
            CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var date) ? date : null;
    }

    private static string FormatTime(string? time)
    {
        // ADIF TIME_ON / TIME_OFF is HHMM or HHMMSS, UTC, zero-padded on the LEFT.
        // A raw "945" means 09:45, not 94:50 — PadRight was wrong and would push LOTW to
        // silently drop the QSO on ingestion (invalid time component).
        if (string.IsNullOrEmpty(time)) return "0000";
        var digits = time.Replace(":", "");
        if (digits.Length <= 4) return digits.PadLeft(4, '0');
        if (digits.Length <= 6) return digits.PadLeft(6, '0');
        return digits[..6];
    }

}
