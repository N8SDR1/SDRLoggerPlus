using System.Security.Principal;
using Microsoft.AspNetCore.SignalR;
using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Contracts.Events;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database;
using SDRLoggerPlus.Server.Hubs;

namespace SDRLoggerPlus.Server.Services;

public class LotwService : ILotwService
{
    private readonly ISettingsRepository _settingsRepository;
    private readonly IQsoRepository _qsoRepository;
    private readonly IAdifService _adifService;
    private readonly ITqslRunner _tqsl;
    private readonly IHubContext<LogHub, ILogHubClient> _hub;
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ILogger<LotwService> _logger;

    private const string LotwReportUrl = "https://lotw.arrl.org/lotwuser/lotwreport.adi";

    // Preview sample cap — UI shows first N entries in the confirm dialog.
    private const int PreviewSampleSize = 50;

    public LotwService(
        ISettingsRepository settingsRepository,
        IQsoRepository qsoRepository,
        IAdifService adifService,
        ITqslRunner tqsl,
        IHubContext<LogHub, ILogHubClient> hub,
        IHttpClientFactory httpClientFactory,
        ILogger<LotwService> logger)
    {
        _settingsRepository = settingsRepository;
        _qsoRepository = qsoRepository;
        _adifService = adifService;
        _tqsl = tqsl;
        _hub = hub;
        _httpClientFactory = httpClientFactory;
        _logger = logger;
    }

    public async Task<ConfirmationMergeResponse> DownloadConfirmationsAsync(CancellationToken cancellationToken)
    {
        var settings = await _settingsRepository.GetAsync() ?? new UserSettings();
        var lotw = settings.Lotw;

        if (string.IsNullOrWhiteSpace(lotw.Username) || string.IsNullOrWhiteSpace(lotw.Password))
            throw new InvalidOperationException("Set your LoTW website login (username + password) in Settings → LoTW first.");

        // Incremental: only pull confirmations newer than the last sync. LoTW's
        // qslsince filters on the QSL *received* date. First run pulls everything.
        var query = new List<string>
        {
            "qso_query=1",
            "qso_qsl=yes",
            "qso_qsldetail=yes",
            $"login={Uri.EscapeDataString(lotw.Username!)}",
            $"password={Uri.EscapeDataString(lotw.Password!)}"
        };
        if (lotw.LastConfirmationSync is { } since)
            query.Add($"qso_qslsince={since:yyyy-MM-dd}");

        var url = $"{LotwReportUrl}?{string.Join("&", query)}";

        _logger.LogInformation("Downloading LoTW confirmations (since {Since})",
            lotw.LastConfirmationSync?.ToString("yyyy-MM-dd") ?? "beginning");

        var client = _httpClientFactory.CreateClient();
        client.Timeout = TimeSpan.FromMinutes(5);

        string adif;
        try
        {
            adif = await client.GetStringAsync(url, cancellationToken);
        }
        catch (HttpRequestException ex)
        {
            throw new InvalidOperationException($"Could not reach LoTW: {ex.Message}", ex);
        }

        // A valid report always carries an ADIF header; a bad login returns an
        // HTML/error page instead. Fail loudly rather than reporting "0 confirmed".
        if (adif.IndexOf("<eoh>", StringComparison.OrdinalIgnoreCase) < 0)
        {
            if (adif.Contains("Username/password", StringComparison.OrdinalIgnoreCase) ||
                adif.Contains("invalid", StringComparison.OrdinalIgnoreCase) ||
                adif.Contains("incorrect", StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException("LoTW rejected the login — check your LoTW website username and password.");
            throw new InvalidOperationException("LoTW did not return a valid report (unexpected response).");
        }

        using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(adif));
        var result = await _adifService.MergeConfirmationsAsync(stream, ConfirmationSource.Lotw, cancellationToken);

        // Stamp the sync time so the next pull is incremental.
        settings.Lotw.LastConfirmationSync = DateTime.UtcNow;
        await _settingsRepository.UpsertAsync(settings);

        _logger.LogInformation("LoTW confirmation download merged: {Updated} newly confirmed, {Unmatched} unmatched",
            result.Updated, result.Unmatched);

        return result;
    }

    public async Task<LotwPreviewResponse> PreviewAsync(LotwUploadFilter filter)
    {
        var eligible = await GetEligibleQsosAsync(filter);
        var sample = eligible
            .Take(PreviewSampleSize)
            .Select(q => new LotwPreviewItem(
                q.Id,
                q.Callsign,
                q.QsoDate,
                q.Band,
                q.Mode,
                q.Qsl?.Lotw?.Sent))
            .ToList();

        return new LotwPreviewResponse(eligible.Count, sample);
    }

    public async Task<LotwUploadResult> UploadAsync(LotwUploadFilter filter, CancellationToken cancellationToken)
    {
        var settings = await _settingsRepository.GetAsync() ?? new UserSettings();
        var lotw = settings.Lotw;

        if (IsRunningAsAdmin())
        {
            var msg = "SDRLoggerPlus is running as Administrator — TQSL refuses elevated runs and would silently corrupt the upload (wrong cert store). Close SDRLoggerPlus (and any elevated terminal) and relaunch as a standard user.";
            await BroadcastAsync("error", 0, null, msg, true);
            return new LotwUploadResult(0, false, -1, msg, 0);
        }

        var pathError = ValidateTqslPath(lotw.TqslPath);
        if (pathError != null)
        {
            await BroadcastAsync("error", 0, null, pathError, true);
            return new LotwUploadResult(0, false, -1, pathError, 0);
        }

        var eligible = await GetEligibleQsosAsync(filter);
        if (eligible.Count == 0)
        {
            var msg = "No QSOs match the filter.";
            await BroadcastAsync("done", 0, null, msg, true);
            return new LotwUploadResult(0, false, -1, msg, 0);
        }

        await BroadcastAsync("preparing", eligible.Count, null, $"Exporting {eligible.Count} QSOs to ADIF", false);

        var stationCallsign = !string.IsNullOrWhiteSpace(filter.StationCallsign)
            ? filter.StationCallsign
            : (!string.IsNullOrWhiteSpace(lotw.StationCallsign)
                ? lotw.StationCallsign
                : settings.Station?.Callsign);

        var adif = _adifService.ExportToAdif(eligible, stationCallsign);

        // Write the ADIF under %APPDATA%\SDRLoggerPlus\lotw-uploads\ (or the equivalent on mac/linux).
        // On success we delete it — the QSO is marked sent in the DB and TQSL's copy is enough.
        // On failure we keep it so the user can diff against a known-good ADIF, rerun signing
        // manually with `tqsl -d -u <file>`, etc.
        var archiveDir = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
            "SDRLoggerPlus", "lotw-uploads");
        Directory.CreateDirectory(archiveDir);
        var timestamp = DateTime.UtcNow.ToString("yyyyMMdd-HHmmss");
        var adifPath = Path.Combine(archiveDir, $"lotw-upload-{timestamp}-{Guid.NewGuid():N}.adi");
        var keepAdif = false;
        try
        {
            await File.WriteAllTextAsync(adifPath, adif, cancellationToken);

            await BroadcastAsync("signing", eligible.Count, null, "Handing off to TQSL for signing and upload", false);

            var tqslResult = await _tqsl.UploadAsync(lotw.TqslPath!, adifPath, lotw.StationCallsign, cancellationToken);

            var (success, message) = InterpretExitCode(tqslResult.ExitCode, eligible.Count);
            keepAdif = !success;

            // Include TQSL's own output — lets the user see exactly what the binary reported
            // (e.g. "5 QSOs uploaded to LoTW"). Distinguishes "TQSL signed & transmitted" from
            // "LOTW server-side processed" — exit 0 only confirms the former; the latter happens
            // asynchronously on ARRL's queue.
            var tqslOutput = ExtractTqslSummary(tqslResult);
            var fullMessage = string.IsNullOrWhiteSpace(tqslOutput)
                ? message
                : $"{message}\n\nTQSL output:\n{tqslOutput}";
            if (keepAdif)
            {
                fullMessage += $"\n\nADIF retained for inspection: {adifPath}";
            }

            var markedAsSent = 0;
            if (success)
            {
                markedAsSent = await MarkQsosAsSentAsync(eligible);
                settings.Lotw.LastUploadAt = DateTime.UtcNow;
                await _settingsRepository.UpsertAsync(settings);
            }

            await BroadcastAsync(success ? "done" : "error", eligible.Count, tqslResult.ExitCode, fullMessage, true);

            return new LotwUploadResult(eligible.Count, success, tqslResult.ExitCode, fullMessage, markedAsSent);
        }
        catch (OperationCanceledException)
        {
            keepAdif = true;
            await BroadcastAsync("error", eligible.Count, null, "Upload cancelled", true);
            throw;
        }
        catch (Exception ex)
        {
            keepAdif = true;
            _logger.LogError(ex, "LOTW upload failed");
            await BroadcastAsync("error", eligible.Count, null, ex.Message, true);
            return new LotwUploadResult(eligible.Count, false, -1, ex.Message, 0);
        }
        finally
        {
            if (!keepAdif)
            {
                try { if (File.Exists(adifPath)) File.Delete(adifPath); }
                catch (Exception ex) { _logger.LogWarning(ex, "Failed to delete ADIF {Path}", adifPath); }
            }
        }
    }

    public async Task<LotwTestTqslResponse> TestTqslAsync(string path, CancellationToken cancellationToken)
    {
        var pathError = ValidateTqslPath(path);
        if (pathError != null)
        {
            return new LotwTestTqslResponse(false, null, pathError);
        }

        var result = await _tqsl.GetVersionAsync(path, cancellationToken);
        return new LotwTestTqslResponse(result.Ok, result.Version, result.Error);
    }

    // ----- helpers --------------------------------------------------------

    /// <summary>
    /// Reject obviously-wrong TQSL paths before we spawn them. Catches two footguns:
    /// empty / missing file, and paths that point at a non-TQSL binary (e.g. the user
    /// browsed to SDRLoggerPlus.exe by mistake — running *that* just launches a second SDRLoggerPlus
    /// instead of returning an error). Soft filename match: basename must contain "tqsl"
    /// (case-insensitive). Users who've renamed the binary can still proceed via a
    /// message, but the default footgun is eliminated.
    /// </summary>
    /// <summary>
    /// Detect when the backend is running elevated on Windows. TQSL refuses admin runs —
    /// without this preflight the user gets a modal dialog that `-q` is supposed to
    /// suppress, and if they click through, signing happens against the Administrator
    /// account's (empty) cert store so LOTW silently drops the upload on ingest.
    /// </summary>
    private static bool IsRunningAsAdmin()
    {
        if (!OperatingSystem.IsWindows()) return false;
        try
        {
            using var identity = WindowsIdentity.GetCurrent();
            var principal = new WindowsPrincipal(identity);
            return principal.IsInRole(WindowsBuiltInRole.Administrator);
        }
        catch
        {
            return false;
        }
    }

    private static string? ValidateTqslPath(string? path)
    {
        if (string.IsNullOrWhiteSpace(path))
        {
            return "TQSL path is not configured. Open Settings → LOTW and pick the tqsl executable.";
        }

        if (!File.Exists(path))
        {
            return $"TQSL not found at '{path}'. Check Settings → LOTW.";
        }

        var baseName = Path.GetFileNameWithoutExtension(path) ?? string.Empty;
        if (!baseName.Contains("tqsl", StringComparison.OrdinalIgnoreCase))
        {
            return $"'{Path.GetFileName(path)}' does not look like the TQSL binary. In Settings → LOTW, browse for 'tqsl' (or 'tqsl.exe' on Windows).";
        }

        return null;
    }

    private async Task<List<Qso>> GetEligibleQsosAsync(LotwUploadFilter filter)
    {
        // A QSO made under a different operating call (club/special contest) cannot be signed with
        // the operator's PERSONAL LoTW certificate — exclude it from every selection branch. (A
        // per-callsign certificate mapping is a Stage-2 feature.)
        var myCall = (await _settingsRepository.GetAsync())?.Station?.Callsign;

        // Explicit hand-picked selection wins over everything: upload exactly these QSOs,
        // ignoring band/mode/date and the not-yet-sent rule, so a deliberate re-send works.
        var ids = filter.QsoIds?.Where(s => !string.IsNullOrWhiteSpace(s)).ToHashSet();
        if (ids is { Count: > 0 })
        {
            var (all, _) = await _qsoRepository.SearchAsync(new QsoSearchRequest(Limit: 100_000));
            return all.Where(q => q.Id != null && ids.Contains(q.Id))
                .Where(q => QsoOwnership.IsPersonalQso(q, myCall)).ToList();
        }

        var searchRequest = new QsoSearchRequest(
            FromDate: filter.DateFrom,
            ToDate: filter.DateTo,
            Limit: 100_000);
        var (qsos, _) = await _qsoRepository.SearchAsync(searchRequest);

        var bands = filter.Bands?.Where(b => !string.IsNullOrWhiteSpace(b))
            .Select(b => b.ToUpperInvariant()).ToHashSet();
        var modes = filter.Modes?.Where(m => !string.IsNullOrWhiteSpace(m))
            .Select(m => m.ToUpperInvariant()).ToHashSet();

        return qsos
            .Where(q => QsoOwnership.IsPersonalQso(q, myCall))
            .Where(q => bands == null || bands.Count == 0 || (q.Band != null && bands.Contains(q.Band.ToUpperInvariant())))
            .Where(q => modes == null || modes.Count == 0 || (q.Mode != null && modes.Contains(q.Mode.ToUpperInvariant())))
            .Where(q => IsEligibleForUpload(q, filter))
            .ToList();
    }

    private static bool IsEligibleForUpload(Qso qso, LotwUploadFilter filter)
    {
        // Matches pblog's `WHERE upper(lotw_qsl_sent) IN (...) OR lotw_qsl_sent IS NULL`
        // (LotwDialog.cpp:123). Always eligible: null/empty, 'R', 'Q'. Optional: 'I', 'N'.
        var sent = qso.Qsl?.Lotw?.Sent?.Trim().ToUpperInvariant();
        if (string.IsNullOrEmpty(sent)) return true;
        return sent switch
        {
            "R" or "Q" => true,
            "I" => filter.IncludeIgnored,
            "N" => filter.IncludeNotSent,
            _ => false // 'Y' and anything else — skip
        };
    }

    private async Task<int> MarkQsosAsSentAsync(List<Qso> qsos)
    {
        var now = DateTime.UtcNow;
        var updated = 0;

        foreach (var qso in qsos)
        {
            qso.Qsl ??= new QslStatus();
            qso.Qsl.Lotw ??= new LotwStatus();
            qso.Qsl.Lotw.Sent = "Y";
            qso.Qsl.Lotw.SentDate = now;
            qso.LotwSyncedAt = now;
            qso.LotwSyncStatus = SyncStatus.Synced;

            if (await _qsoRepository.UpdateAsync(qso.Id, qso))
            {
                updated++;
            }
        }

        return updated;
    }

    /// <summary>
    /// Map TQSL exit code to (success, user-facing message). Codes sourced from
    /// http://www.arrl.org/command-1 and pblog's Lotw.cpp:88-140.
    /// Exit 9 ("some QSOs were duplicates") still counts as success — the non-dupe
    /// QSOs were accepted, and the dupes were already at LOTW. pblog treated this
    /// as pure error and skipped the DB update; that's the bug we're not inheriting.
    /// </summary>
    private static (bool Success, string Message) InterpretExitCode(int exitCode, int qsoCount)
    {
        return exitCode switch
        {
            // Exit 0 means TQSL signed the file AND LOTW's upload endpoint accepted it.
            // The server-side ingestion into the user's logbook is queued and async —
            // it's why an uploaded QSO may not appear in "Your QSOs" on the LOTW website
            // for several minutes after exit 0.
            0 => (true, $"{qsoCount} QSO(s) signed and handed to LOTW. QSOs typically appear in your LOTW logbook within a few minutes."),
            1 => (false, "Upload cancelled by user"),
            2 => (false, "Upload rejected by LoTW"),
            3 => (false, "Unexpected response from TQSL server"),
            4 => (false, "TQSL utility error"),
            5 => (false, "TQSLlib error"),
            6 => (false, "TQSL could not open the input file"),
            7 => (false, "TQSL could not open the output file"),
            8 => (false, "All QSOs were duplicates or out of date range"),
            9 => (true, "Upload accepted; some QSOs were duplicates or out of date range"),
            10 => (false, "TQSL command syntax error"),
            11 => (false, "LoTW connection error (no network or LoTW unreachable)"),
            _ => (false, $"Unexpected TQSL exit code {exitCode}")
        };
    }

    private Task BroadcastAsync(string stage, int qsoCount, int? exitCode, string? message, bool isComplete)
    {
        return _hub.BroadcastLotwUploadProgress(new LotwUploadProgressEvent(
            stage, qsoCount, isComplete, exitCode, message));
    }

    /// <summary>
    /// Pull a compact summary out of TQSL's output — stdout first, then stderr. Strips blank
    /// lines and trims noise. Returned as-is for display in the upload banner.
    /// </summary>
    private static string ExtractTqslSummary(TqslRunResult result)
    {
        var combined = string.IsNullOrWhiteSpace(result.StdOut)
            ? result.StdErr
            : result.StdOut;
        if (string.IsNullOrWhiteSpace(combined)) return string.Empty;

        var lines = combined
            .Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries)
            .Select(l => l.Trim())
            .Where(l => l.Length > 0)
            .ToList();

        // Cap at last 10 lines — TQSL typically prints 1-4 lines, but guard against runaway.
        if (lines.Count > 10)
        {
            lines = lines.GetRange(lines.Count - 10, 10);
        }
        return string.Join("\n", lines);
    }
}
