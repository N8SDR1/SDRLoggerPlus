using SDRLoggerPlus.Contracts.Api;

namespace SDRLoggerPlus.Server.Services;

public interface IAiService
{
    Task<GenerateTalkPointsResponse> GenerateTalkPointsAsync(GenerateTalkPointsRequest request);
    Task<ChatResponse> ChatAsync(ChatRequest request);
    IAsyncEnumerable<string> ChatStreamAsync(ChatRequest request, CancellationToken cancellationToken = default);
    Task<TestApiKeyResponse> TestApiKeyAsync(TestApiKeyRequest request);
}
