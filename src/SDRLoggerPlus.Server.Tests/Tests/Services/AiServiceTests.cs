using System.Net;
using System.Text;
using System.Text.Json;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Moq;
using Moq.Protected;
using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database;
using SDRLoggerPlus.Server.Services;
using Xunit;
using Xunit.Sdk;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

[Trait("Category", "Unit")]
public class AiServiceTests
{
    private readonly Mock<ISettingsRepository> _settingsRepoMock = new();
    private readonly Mock<IQsoService> _qsoServiceMock = new();
    private readonly Mock<IQrzService> _qrzServiceMock = new();
    private readonly Mock<IHttpClientFactory> _httpClientFactoryMock = new();
    private readonly Mock<ILogger<AiService>> _loggerMock = new();

    private AiService CreateService(HttpClient httpClient)
    {
        _httpClientFactoryMock
            .Setup(f => f.CreateClient("AI"))
            .Returns(httpClient);

        return new AiService(
            _settingsRepoMock.Object,
            _qsoServiceMock.Object,
            _qrzServiceMock.Object,
            _httpClientFactoryMock.Object,
            _loggerMock.Object);
    }

    private void SetupSettingsWithAi(string provider = "anthropic", string apiKey = "test-key", string model = "claude-sonnet-4-5-20250929")
    {
        _settingsRepoMock.Setup(r => r.GetAsync(It.IsAny<string>()))
            .ReturnsAsync(new UserSettings
            {
                Station = new StationSettings { Callsign = "EI2KK" },
                Ai = new AiSettings
                {
                    Provider = provider,
                    ApiKey = apiKey,
                    Model = model,
                    IncludeQrzProfile = true,
                    IncludeQsoHistory = true
                }
            });
    }

    private void SetupEmptyQsoHistory()
    {
        _qsoServiceMock.Setup(s => s.GetQsosAsync(It.IsAny<QsoSearchRequest>()))
            .ReturnsAsync(new PaginatedQsoResponse(
                Items: Enumerable.Empty<QsoResponse>(),
                TotalCount: 0,
                Page: 1,
                PageSize: 10,
                TotalPages: 0));
    }

    private static HttpClient CreateMockHttpClient(HttpStatusCode statusCode, string responseBody)
    {
        var handler = new Mock<HttpMessageHandler>();
        handler.Protected()
            .Setup<Task<HttpResponseMessage>>(
                "SendAsync",
                ItExpr.IsAny<HttpRequestMessage>(),
                ItExpr.IsAny<CancellationToken>())
            .ReturnsAsync(new HttpResponseMessage
            {
                StatusCode = statusCode,
                Content = new StringContent(responseBody, Encoding.UTF8, "application/json")
            });

        return new HttpClient(handler.Object);
    }

    /// <summary>
    /// Like CreateMockHttpClient, but records the requests it serves so a test can
    /// assert which provider endpoint the service actually called.
    /// </summary>
    private static HttpClient CreateCapturingHttpClient(
        HttpStatusCode statusCode, string responseBody, List<HttpRequestMessage> captured)
    {
        var handler = new Mock<HttpMessageHandler>();
        handler.Protected()
            .Setup<Task<HttpResponseMessage>>(
                "SendAsync",
                ItExpr.IsAny<HttpRequestMessage>(),
                ItExpr.IsAny<CancellationToken>())
            .Callback<HttpRequestMessage, CancellationToken>((req, _) => captured.Add(req))
            .ReturnsAsync(new HttpResponseMessage
            {
                StatusCode = statusCode,
                Content = new StringContent(responseBody, Encoding.UTF8, "application/json")
            });

        return new HttpClient(handler.Object);
    }

    #region GenerateTalkPointsAsync

    [Fact]
    public async Task GenerateTalkPointsAsync_WithAnthropicProvider_CallsAnthropicApi()
    {
        SetupSettingsWithAi("anthropic", "test-api-key");
        SetupEmptyQsoHistory();
        _qrzServiceMock.Setup(s => s.LookupCallsignAsync(It.IsAny<string>())).ReturnsAsync((QrzCallsignInfo?)null);
        _qrzServiceMock.Setup(s => s.GetBiographyAsync(It.IsAny<string>())).ReturnsAsync((string?)null);

        var anthropicResponse = JsonSerializer.Serialize(new
        {
            content = new[] { new { text = "- Talk about their location\n- Discuss band conditions\n- Ask about their setup" } }
        });

        var httpClient = CreateMockHttpClient(HttpStatusCode.OK, anthropicResponse);
        var service = CreateService(httpClient);

        var result = await service.GenerateTalkPointsAsync(new GenerateTalkPointsRequest("W1AW"));

        result.Should().NotBeNull();
        result.Callsign.Should().Be("W1AW");
        result.TalkPoints.Should().NotBeEmpty();
    }

    [Fact]
    public async Task GenerateTalkPointsAsync_WithOpenAiProvider_CallsOpenAiApi()
    {
        SetupSettingsWithAi("openai", "test-openai-key", "gpt-4");
        SetupEmptyQsoHistory();
        _qrzServiceMock.Setup(s => s.LookupCallsignAsync(It.IsAny<string>())).ReturnsAsync((QrzCallsignInfo?)null);
        _qrzServiceMock.Setup(s => s.GetBiographyAsync(It.IsAny<string>())).ReturnsAsync((string?)null);

        var openAiResponse = JsonSerializer.Serialize(new
        {
            choices = new[] { new { message = new { content = "1. Mention propagation\n2. Ask about antenna" } } }
        });

        var httpClient = CreateMockHttpClient(HttpStatusCode.OK, openAiResponse);
        var service = CreateService(httpClient);

        var result = await service.GenerateTalkPointsAsync(new GenerateTalkPointsRequest("W1AW"));

        result.Should().NotBeNull();
        result.TalkPoints.Should().NotBeEmpty();
    }

    [Fact]
    public async Task GenerateTalkPointsAsync_NoApiKey_ThrowsInvalidOperation()
    {
        _settingsRepoMock.Setup(r => r.GetAsync(It.IsAny<string>()))
            .ReturnsAsync(new UserSettings
            {
                Station = new StationSettings { Callsign = "EI2KK" },
                Ai = new AiSettings { ApiKey = "" }
            });

        var httpClient = CreateMockHttpClient(HttpStatusCode.OK, "{}");
        var service = CreateService(httpClient);

        var act = () => service.GenerateTalkPointsAsync(new GenerateTalkPointsRequest("W1AW"));

        await act.Should().ThrowAsync<InvalidOperationException>()
            .WithMessage("*API key*not configured*");
    }

    [Fact]
    public async Task GenerateTalkPointsAsync_WithPreviousQsos_IncludesInContext()
    {
        SetupSettingsWithAi();
        _qrzServiceMock.Setup(s => s.LookupCallsignAsync(It.IsAny<string>())).ReturnsAsync((QrzCallsignInfo?)null);
        _qrzServiceMock.Setup(s => s.GetBiographyAsync(It.IsAny<string>())).ReturnsAsync((string?)null);

        _qsoServiceMock.Setup(s => s.GetQsosAsync(It.IsAny<QsoSearchRequest>()))
            .ReturnsAsync(new PaginatedQsoResponse(
                Items: new[]
                {
                    new QsoResponse("1", "W1AW", DateTime.UtcNow, "1430", null, "20m", "SSB",
                        14200, "59", "57", null, "Great signal!", DateTime.UtcNow)
                },
                TotalCount: 1,
                Page: 1,
                PageSize: 10,
                TotalPages: 1));

        var anthropicResponse = JsonSerializer.Serialize(new
        {
            content = new[] { new { text = "- Mention your previous QSO on 20m" } }
        });

        var httpClient = CreateMockHttpClient(HttpStatusCode.OK, anthropicResponse);
        var service = CreateService(httpClient);

        var result = await service.GenerateTalkPointsAsync(new GenerateTalkPointsRequest("W1AW"));

        result.PreviousQsos.Should().HaveCount(1);
        result.PreviousQsos[0].Band.Should().Be("20m");
    }

    [Fact]
    public async Task GenerateTalkPointsAsync_WithQrzProfile_IncludesInContext()
    {
        SetupSettingsWithAi();
        SetupEmptyQsoHistory();

        _qrzServiceMock.Setup(s => s.LookupCallsignAsync("W1AW"))
            .ReturnsAsync(new QrzCallsignInfo(
                "W1AW", "ARRL", "ARRL", null, "Newington", "CT", "Hartford",
                "United States", "FN31", 41.7, -72.7,
                291, 5, 8, null, null, null, null));
        _qrzServiceMock.Setup(s => s.GetBiographyAsync("W1AW"))
            .ReturnsAsync("The ARRL headquarters station.");

        var anthropicResponse = JsonSerializer.Serialize(new
        {
            content = new[] { new { text = "- Ask about ARRL events" } }
        });

        var httpClient = CreateMockHttpClient(HttpStatusCode.OK, anthropicResponse);
        var service = CreateService(httpClient);

        var result = await service.GenerateTalkPointsAsync(new GenerateTalkPointsRequest("W1AW"));

        result.QrzProfile.Should().NotBeNull();
        result.QrzProfile!.Name.Should().Be("ARRL");
    }

    #endregion

    #region ChatAsync

    [Fact]
    public async Task ChatAsync_ValidRequest_ReturnsResponse()
    {
        SetupSettingsWithAi();
        SetupEmptyQsoHistory();
        _qrzServiceMock.Setup(s => s.LookupCallsignAsync(It.IsAny<string>())).ReturnsAsync((QrzCallsignInfo?)null);
        _qrzServiceMock.Setup(s => s.GetBiographyAsync(It.IsAny<string>())).ReturnsAsync((string?)null);

        var anthropicResponse = JsonSerializer.Serialize(new
        {
            content = new[] { new { text = "W1AW is the ARRL headquarters station in Newington, CT." } }
        });

        var httpClient = CreateMockHttpClient(HttpStatusCode.OK, anthropicResponse);
        var service = CreateService(httpClient);

        var result = await service.ChatAsync(new ChatRequest("W1AW", "Tell me about this station"));

        result.Should().NotBeNull();
        result.Answer.Should().Contain("ARRL");
    }

    [Fact]
    public async Task ChatAsync_NoApiKey_ThrowsInvalidOperation()
    {
        _settingsRepoMock.Setup(r => r.GetAsync(It.IsAny<string>()))
            .ReturnsAsync(new UserSettings
            {
                Station = new StationSettings { Callsign = "EI2KK" },
                Ai = new AiSettings { ApiKey = "" }
            });

        var httpClient = CreateMockHttpClient(HttpStatusCode.OK, "{}");
        var service = CreateService(httpClient);

        var act = () => service.ChatAsync(new ChatRequest("W1AW", "Hello"));

        await act.Should().ThrowAsync<InvalidOperationException>();
    }

    [Fact]
    public async Task ChatAsync_WithConversationHistory_SendsHistory()
    {
        SetupSettingsWithAi();
        SetupEmptyQsoHistory();
        _qrzServiceMock.Setup(s => s.LookupCallsignAsync(It.IsAny<string>())).ReturnsAsync((QrzCallsignInfo?)null);
        _qrzServiceMock.Setup(s => s.GetBiographyAsync(It.IsAny<string>())).ReturnsAsync((string?)null);

        var anthropicResponse = JsonSerializer.Serialize(new
        {
            content = new[] { new { text = "Their grid square is FN31." } }
        });

        var httpClient = CreateMockHttpClient(HttpStatusCode.OK, anthropicResponse);
        var service = CreateService(httpClient);

        var history = new List<ChatMessage>
        {
            new("user", "What is W1AW?"),
            new("assistant", "W1AW is the ARRL HQ station.")
        };

        var result = await service.ChatAsync(new ChatRequest("W1AW", "What is their grid?", history));

        result.Answer.Should().NotBeNullOrEmpty();
    }

    #endregion

    #region TestApiKeyAsync

    [Fact]
    public async Task TestApiKeyAsync_ValidKey_ReturnsValid()
    {
        var anthropicResponse = JsonSerializer.Serialize(new
        {
            content = new[] { new { text = "OK" } }
        });

        var httpClient = CreateMockHttpClient(HttpStatusCode.OK, anthropicResponse);
        var service = CreateService(httpClient);

        var result = await service.TestApiKeyAsync(new TestApiKeyRequest("anthropic", "valid-key", "claude-sonnet-4-5-20250929"));

        result.IsValid.Should().BeTrue();
        result.ErrorMessage.Should().BeNull();
    }

    [Fact]
    public async Task TestApiKeyAsync_InvalidKey_ReturnsInvalid()
    {
        var httpClient = CreateMockHttpClient(HttpStatusCode.Unauthorized, "{\"error\": \"invalid_api_key\"}");
        var service = CreateService(httpClient);

        var result = await service.TestApiKeyAsync(new TestApiKeyRequest("anthropic", "bad-key", "claude-sonnet-4-5-20250929"));

        result.IsValid.Should().BeFalse();
        result.ErrorMessage.Should().NotBeNullOrEmpty();
    }

    /// <summary>
    /// An unrecognised provider name is NOT an error: since 764b3f5 the service keeps
    /// a native path for Anthropic and treats everything else as OpenAI-compatible,
    /// which is what makes the "Custom (OpenAI-compatible)" preset and its Base-URL
    /// field work. So the key test succeeds or fails on what the endpoint says, not
    /// on whether we recognise the provider string.
    /// </summary>
    [Fact]
    public async Task TestApiKeyAsync_UnrecognisedProvider_UsesOpenAiCompatiblePath()
    {
        var captured = new List<HttpRequestMessage>();
        var httpClient = CreateCapturingHttpClient(HttpStatusCode.OK, "{}", captured);
        var service = CreateService(httpClient);

        var result = await service.TestApiKeyAsync(new TestApiKeyRequest("unrecognised", "key", "model"));

        result.IsValid.Should().BeTrue();
        result.ErrorMessage.Should().BeNull();
        captured.Should().ContainSingle();
        captured[0].RequestUri!.ToString().Should().StartWith("https://api.openai.com/v1");
    }

    /// <summary>
    /// The same unrecognised provider still reports a failure when the endpoint
    /// rejects the key — proving the OK result above comes from the endpoint rather
    /// than from the service skipping the call.
    /// </summary>
    [Fact]
    public async Task TestApiKeyAsync_UnrecognisedProviderWithBadKey_ReturnsInvalid()
    {
        var httpClient = CreateMockHttpClient(HttpStatusCode.Unauthorized, "{\"error\": \"invalid_api_key\"}");
        var service = CreateService(httpClient);

        var result = await service.TestApiKeyAsync(new TestApiKeyRequest("unrecognised", "bad-key", "model"));

        result.IsValid.Should().BeFalse();
        result.ErrorMessage.Should().NotBeNullOrEmpty();
    }

    #endregion

    #region Provider Routing

    /// <summary>
    /// An unrecognised provider routes through the OpenAI-compatible path rather than
    /// throwing (see TestApiKeyAsync_UnrecognisedProvider_UsesOpenAiCompatiblePath).
    /// Serving an OpenAI-shaped body and getting talk points back proves the routing:
    /// the Anthropic parser reads `content[].text` and would yield nothing from this.
    /// </summary>
    [Fact]
    public async Task GenerateTalkPointsAsync_UnrecognisedProvider_UsesOpenAiCompatiblePath()
    {
        SetupSettingsWithAi("unrecognised_provider", "key", "model");
        SetupEmptyQsoHistory();
        _qrzServiceMock.Setup(s => s.LookupCallsignAsync(It.IsAny<string>())).ReturnsAsync((QrzCallsignInfo?)null);
        _qrzServiceMock.Setup(s => s.GetBiographyAsync(It.IsAny<string>())).ReturnsAsync((string?)null);

        var openAiResponse = JsonSerializer.Serialize(new
        {
            choices = new[] { new { message = new { content = "1. Mention propagation\n2. Ask about antenna" } } }
        });

        var captured = new List<HttpRequestMessage>();
        var httpClient = CreateCapturingHttpClient(HttpStatusCode.OK, openAiResponse, captured);
        var service = CreateService(httpClient);

        var result = await service.GenerateTalkPointsAsync(new GenerateTalkPointsRequest("W1AW"));

        result.TalkPoints.Should().NotBeEmpty();
        captured.Should().ContainSingle();
        captured[0].RequestUri!.ToString().Should().StartWith("https://api.openai.com/v1");
    }

    #endregion
}

/// <summary>
/// Live AI tests that make real API calls. Only run when AI_LIVE_TESTS=true.
/// These are gated behind environment variables to avoid CI costs.
/// </summary>
[Trait("Category", "LiveAI")]
public class AiServiceLiveTests
{
    private static bool IsEnabled =>
        string.Equals(
            Environment.GetEnvironmentVariable("AI_LIVE_TESTS"),
            "true",
            StringComparison.OrdinalIgnoreCase);

    private static string? GetAnthropicApiKey =>
        Environment.GetEnvironmentVariable("ANTHROPIC_API_KEY");

    private static string? GetOpenAiApiKey =>
        Environment.GetEnvironmentVariable("OPENAI_API_KEY");

    [SkippableFact]
    public async Task LiveAnthropicApi_TestApiKey_ReturnsValid()
    {
        Skip.IfNot(IsEnabled, "AI_LIVE_TESTS not enabled");
        var apiKey = GetAnthropicApiKey;
        Skip.If(string.IsNullOrEmpty(apiKey), "ANTHROPIC_API_KEY not set");

        var settingsRepo = new Mock<ISettingsRepository>();
        var qsoService = new Mock<IQsoService>();
        var qrzService = new Mock<IQrzService>();
        var logger = new Mock<ILogger<AiService>>();

        var httpClientFactory = new Mock<IHttpClientFactory>();
        var httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(60) };
        httpClient.DefaultRequestHeaders.Add("User-Agent", "SDRLoggerPlus-Tests/1.0");
        httpClientFactory.Setup(f => f.CreateClient("AI")).Returns(httpClient);

        var service = new AiService(
            settingsRepo.Object, qsoService.Object, qrzService.Object,
            httpClientFactory.Object, logger.Object);

        var result = await service.TestApiKeyAsync(
            new TestApiKeyRequest("anthropic", apiKey!, "claude-sonnet-4-5-20250929"));

        result.IsValid.Should().BeTrue();
    }

    [SkippableFact]
    public async Task LiveOpenAiApi_TestApiKey_ReturnsValid()
    {
        Skip.IfNot(IsEnabled, "AI_LIVE_TESTS not enabled");
        var apiKey = GetOpenAiApiKey;
        Skip.If(string.IsNullOrEmpty(apiKey), "OPENAI_API_KEY not set");

        var settingsRepo = new Mock<ISettingsRepository>();
        var qsoService = new Mock<IQsoService>();
        var qrzService = new Mock<IQrzService>();
        var logger = new Mock<ILogger<AiService>>();

        var httpClientFactory = new Mock<IHttpClientFactory>();
        var httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(60) };
        httpClient.DefaultRequestHeaders.Add("User-Agent", "SDRLoggerPlus-Tests/1.0");
        httpClientFactory.Setup(f => f.CreateClient("AI")).Returns(httpClient);

        var service = new AiService(
            settingsRepo.Object, qsoService.Object, qrzService.Object,
            httpClientFactory.Object, logger.Object);

        var result = await service.TestApiKeyAsync(
            new TestApiKeyRequest("openai", apiKey!, "gpt-4o-mini"));

        result.IsValid.Should().BeTrue();
    }
}
