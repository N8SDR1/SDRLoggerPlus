using System.Text;
using SDRLoggerPlus.Contracts.Api;

namespace SDRLoggerPlus.Server.Services;

/// <summary>
/// One place that downloads a confirmation source and merges it into the log,
/// shared by the manual endpoints and the background auto-sync. Scoped, so it
/// can pull together the scoped merge/settings services with the LoTW/eQSL ones.
/// </summary>
public interface IConfirmationSyncService
{
    Task<ConfirmationMergeResponse> SyncLotwAsync(CancellationToken ct = default);
    Task<ConfirmationMergeResponse> SyncEqslAsync(CancellationToken ct = default);
    Task<ConfirmationMergeResponse> SyncQrzAsync(CancellationToken ct = default);
}

public class ConfirmationSyncService : IConfirmationSyncService
{
    private readonly ILotwService _lotw;
    private readonly EqslService _eqsl;
    private readonly IQrzService _qrz;
    private readonly IAdifService _adif;
    private readonly ISettingsService _settings;

    public ConfirmationSyncService(ILotwService lotw, EqslService eqsl, IQrzService qrz, IAdifService adif, ISettingsService settings)
    {
        _lotw = lotw;
        _eqsl = eqsl;
        _qrz = qrz;
        _adif = adif;
        _settings = settings;
    }

    // LoTW self-merges internally (LotwService is scoped and owns IAdifService).
    public Task<ConfirmationMergeResponse> SyncLotwAsync(CancellationToken ct = default)
        => _lotw.DownloadConfirmationsAsync(ct);

    // eQSL is a singleton, so the merge happens here in a scoped context.
    public async Task<ConfirmationMergeResponse> SyncEqslAsync(CancellationToken ct = default)
    {
        var adifText = await _eqsl.DownloadInboxAdifAsync(ct);
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(adifText));
        var result = await _adif.MergeConfirmationsAsync(stream, ConfirmationSource.Eqsl, ct);

        var settings = await _settings.GetSettingsAsync();
        settings.Eqsl.LastConfirmationSync = DateTime.UtcNow;
        await _settings.SaveSettingsAsync(settings);
        return result;
    }

    // QRZ fetch is a full logbook pull; the merge only stamps records QRZ marks
    // confirmed and is idempotent, so no incremental state is needed for v1.
    public async Task<ConfirmationMergeResponse> SyncQrzAsync(CancellationToken ct = default)
    {
        var adifText = await _qrz.FetchLogbookAdifAsync(ct);
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(adifText));
        return await _adif.MergeConfirmationsAsync(stream, ConfirmationSource.Qrz, ct);
    }
}
