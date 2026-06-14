using SDRLoggerPlus.Contracts.Api;

namespace SDRLoggerPlus.Server.Services;

public interface ILotwService
{
    Task<LotwUploadResult> UploadAsync(LotwUploadFilter filter, CancellationToken cancellationToken);

    Task<LotwPreviewResponse> PreviewAsync(LotwUploadFilter filter);

    Task<LotwTestTqslResponse> TestTqslAsync(string path, CancellationToken cancellationToken);
}
