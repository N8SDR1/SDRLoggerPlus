using SDRLoggerPlus.Contracts.Api;

namespace SDRLoggerPlus.Server.Services;

public interface ILotwService
{
    Task<LotwUploadResult> UploadAsync(LotwUploadFilter filter, CancellationToken cancellationToken);

    Task<LotwPreviewResponse> PreviewAsync(LotwUploadFilter filter);

    Task<LotwTestTqslResponse> TestTqslAsync(string path, CancellationToken cancellationToken);

    /// <summary>
    /// Download the LoTW confirmation report (incremental, using the stored
    /// LoTW website login) and merge it into the log — the Log4OM one-click sync.
    /// </summary>
    Task<ConfirmationMergeResponse> DownloadConfirmationsAsync(CancellationToken cancellationToken);
}
