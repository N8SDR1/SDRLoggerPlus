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

    /// <summary>
    /// Records each request's URL and JSON body as it is sent (the service disposes
    /// the request afterwards, so the body can't be read back later).
    /// </summary>
    private static HttpClient CreateBodyCapturingHttpClient(
        HttpStatusCode statusCode, string responseBody, List<(string Url, string Body, HttpRequestMessage Request)> captured)
    {
        var handler = new Mock<HttpMessageHandler>();
        handler.Protected()
            .Setup<Task<HttpResponseMessage>>(
                "SendAsync",
                ItExpr.IsAny<HttpRequestMessage>(),
                ItExpr.IsAny<CancellationToken>())
            .Callback<HttpRequestMessage, CancellationToken>((req, _) =>
                captured.Add((req.RequestUri!.ToString(), req.Content?.ReadAsStringAsync().Result ?? "", req)))
            .ReturnsAsync(() => new HttpResponseMessage
            {
                StatusCode = statusCode,
                Content = new StringContent(responseBody, Encoding.UTF8, "application/json")
            });

        return new HttpClient(handler.Object);
    }

    private void SetupNoQrz()
    {
        _qrzServiceMock.Setup(s => s.LookupCallsignAsync(It.IsAny<string>())).ReturnsAsync((QrzCallsignInfo?)null);
        _qrzServiceMock.Setup(s => s.GetBiographyAsync(It.IsAny<string>())).ReturnsAsync((string?)null);
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
        var openAiResponse = JsonSerializer.Serialize(new
        {
            choices = new[] { new { message = new { content = "OK" } } }
        });
        var captured = new List<HttpRequestMessage>();
        var httpClient = CreateCapturingHttpClient(HttpStatusCode.OK, openAiResponse, captured);
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

    #region Wire format

    /// <summary>
    /// Anthropic rejects a "system" role inside messages with a 400, which broke
    /// every Chat AI question on Claude. The prompt must go in the top-level field.
    /// </summary>
    [Fact]
    public async Task ChatAsync_Anthropic_SendsSystemPromptAsTopLevelField()
    {
        SetupSettingsWithAi("anthropic", "key", "claude-test");
        SetupEmptyQsoHistory();
        SetupNoQrz();

        var captured = new List<(string Url, string Body, HttpRequestMessage Request)>();
        var reply = JsonSerializer.Serialize(new { content = new[] { new { type = "text", text = "Hi" } } });
        var service = CreateService(CreateBodyCapturingHttpClient(HttpStatusCode.OK, reply, captured));

        var history = new List<ChatMessage> { new("user", "Who?"), new("assistant", ""), new("assistant", "ARRL HQ") };
        await service.ChatAsync(new ChatRequest("W1AW", "Grid?", history));

        captured.Should().ContainSingle();
        captured[0].Url.Should().Be("https://api.anthropic.com/v1/messages");
        using var doc = JsonDocument.Parse(captured[0].Body);
        doc.RootElement.GetProperty("system").GetString().Should().Contain("W1AW");
        var roles = doc.RootElement.GetProperty("messages").EnumerateArray()
            .Select(m => m.GetProperty("role").GetString()).ToList();
        roles.Should().NotContain("system");
        roles.Should().Equal("user", "assistant", "user"); // the blank assistant turn is dropped
    }

    [Fact]
    public async Task ChatAsync_OpenAiCompatibleProvider_SendsMaxTokensToItsOwnEndpoint()
    {
        SetupSettingsWithAi("groq", "key", "some-model");
        SetupEmptyQsoHistory();
        SetupNoQrz();

        var captured = new List<(string Url, string Body, HttpRequestMessage Request)>();
        var reply = JsonSerializer.Serialize(new { choices = new[] { new { message = new { content = "Hi" } } } });
        var service = CreateService(CreateBodyCapturingHttpClient(HttpStatusCode.OK, reply, captured));

        await service.ChatAsync(new ChatRequest("W1AW", "Hello"));

        captured[0].Url.Should().Be("https://api.groq.com/openai/v1/chat/completions");
        using var doc = JsonDocument.Parse(captured[0].Body);
        doc.RootElement.TryGetProperty("max_tokens", out _).Should().BeTrue();
        doc.RootElement.TryGetProperty("max_completion_tokens", out _).Should().BeFalse();
        doc.RootElement.GetProperty("messages")[0].GetProperty("role").GetString().Should().Be("system");
    }

    [Fact]
    public async Task ChatAsync_OpenAi_SendsMaxCompletionTokens()
    {
        SetupSettingsWithAi("openai", "key", "some-model");
        SetupEmptyQsoHistory();
        SetupNoQrz();

        var captured = new List<(string Url, string Body, HttpRequestMessage Request)>();
        var reply = JsonSerializer.Serialize(new { choices = new[] { new { message = new { content = "Hi" } } } });
        var service = CreateService(CreateBodyCapturingHttpClient(HttpStatusCode.OK, reply, captured));

        await service.ChatAsync(new ChatRequest("W1AW", "Hello"));

        using var doc = JsonDocument.Parse(captured[0].Body);
        doc.RootElement.TryGetProperty("max_completion_tokens", out _).Should().BeTrue();
        doc.RootElement.TryGetProperty("max_tokens", out _).Should().BeFalse();
    }

    [Fact]
    public async Task GenerateTalkPointsAsync_Ollama_NeedsNoApiKey()
    {
        SetupSettingsWithAi("ollama", "", "llama3.2");
        SetupEmptyQsoHistory();
        SetupNoQrz();

        var captured = new List<(string Url, string Body, HttpRequestMessage Request)>();
        var reply = JsonSerializer.Serialize(new { choices = new[] { new { message = new { content = "<think>hmm</think>- Ask about the rig" } } } });
        var service = CreateService(CreateBodyCapturingHttpClient(HttpStatusCode.OK, reply, captured));

        var result = await service.GenerateTalkPointsAsync(new GenerateTalkPointsRequest("W1AW"));

        captured[0].Url.Should().Be("http://localhost:11434/v1/chat/completions");
        captured[0].Request.Headers.Authorization.Should().BeNull();
        result.TalkPoints.Should().Equal("Ask about the rig");
    }

    [Fact]
    public async Task GenerateTalkPointsAsync_NoModel_ThrowsInvalidOperation()
    {
        SetupSettingsWithAi("groq", "key", "");
        var service = CreateService(CreateMockHttpClient(HttpStatusCode.OK, "{}"));

        var act = () => service.GenerateTalkPointsAsync(new GenerateTalkPointsRequest("W1AW"));

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*model*");
    }

    [Fact]
    public async Task ChatAsync_ReasoningModelOutOfBudget_ThrowsReadableError()
    {
        SetupSettingsWithAi("openai", "key", "reasoner");
        SetupEmptyQsoHistory();
        SetupNoQrz();

        var reply = JsonSerializer.Serialize(new { choices = new[] { new { message = new { content = "" }, finish_reason = "length" } } });
        var service = CreateService(CreateMockHttpClient(HttpStatusCode.OK, reply));

        var act = () => service.ChatAsync(new ChatRequest("W1AW", "Hello"));

        await act.Should().ThrowAsync<AiProviderException>().WithMessage("*budget*");
    }

    [Fact]
    public async Task ChatAsync_RetiredModel_ReportsProviderMessage()
    {
        SetupSettingsWithAi("anthropic", "key", "claude-old");
        SetupEmptyQsoHistory();
        SetupNoQrz();

        var body = """{"type":"error","error":{"type":"not_found_error","message":"model: claude-old"}}""";
        var service = CreateService(CreateMockHttpClient(HttpStatusCode.NotFound, body));

        var act = () => service.ChatAsync(new ChatRequest("W1AW", "Hello"));

        await act.Should().ThrowAsync<AiProviderException>().WithMessage("*Refresh the model list*model: claude-old*");
    }

    #endregion

    #region ListModelsAsync

    [Fact]
    public async Task ListModelsAsync_Anthropic_ReturnsLiveModels()
    {
        var captured = new List<(string Url, string Body, HttpRequestMessage Request)>();
        var body = JsonSerializer.Serialize(new
        {
            data = new[] { new { id = "claude-new", display_name = "Claude New" }, new { id = "claude-older", display_name = "Claude Older" } }
        });
        var service = CreateService(CreateBodyCapturingHttpClient(HttpStatusCode.OK, body, captured));

        var result = await service.ListModelsAsync(new ListAiModelsRequest("anthropic", "key"));

        result.Success.Should().BeTrue();
        result.Models.Select(m => m.Id).Should().Equal("claude-new", "claude-older");
        captured[0].Url.Should().StartWith("https://api.anthropic.com/v1/models");
        captured[0].Request.Headers.GetValues("x-api-key").Should().Equal("key");
    }

    [Fact]
    public async Task ListModelsAsync_OpenRouter_FlagsFreeFirstAndDropsNonChat()
    {
        var body = """
        { "data": [
          { "id": "vendor/paid", "name": "Paid", "pricing": { "prompt": "0.000001", "completion": "0.000002" } },
          { "id": "vendor/zero", "name": "Zero", "pricing": { "prompt": "0", "completion": "0" } },
          { "id": "vendor/thing:free", "name": "Thing" },
          { "id": "vendor/picture", "architecture": { "output_modalities": ["image"] } },
          { "id": "vendor/text-embedding-3" }
        ] }
        """;
        var service = CreateService(CreateMockHttpClient(HttpStatusCode.OK, body));

        var result = await service.ListModelsAsync(new ListAiModelsRequest("openrouter", "key"));

        result.Models.Select(m => (m.Id, m.Free)).Should().Equal(
            ("vendor/thing:free", true), ("vendor/zero", true), ("vendor/paid", false));
    }

    [Fact]
    public async Task ListModelsAsync_Gemini_StripsModelsPrefix()
    {
        var captured = new List<(string Url, string Body, HttpRequestMessage Request)>();
        var body = """{ "data": [ { "id": "models/gemini-x-flash" }, { "id": "models/gemini-embedding-001" } ] }""";
        var service = CreateService(CreateBodyCapturingHttpClient(HttpStatusCode.OK, body, captured));

        var result = await service.ListModelsAsync(new ListAiModelsRequest("gemini", "key"));

        captured[0].Url.Should().Be("https://generativelanguage.googleapis.com/v1beta/openai/models");
        result.Models.Select(m => m.Id).Should().Equal("gemini-x-flash");
    }

    [Fact]
    public async Task ListModelsAsync_OllamaWithNoModels_SaysHowToPullOne()
    {
        var service = CreateService(CreateMockHttpClient(HttpStatusCode.OK, """{ "data": [] }"""));

        var result = await service.ListModelsAsync(new ListAiModelsRequest("ollama"));

        result.Success.Should().BeFalse();
        result.ErrorMessage.Should().Contain("ollama pull");
    }

    [Fact]
    public async Task ListModelsAsync_OllamaNotRunning_SaysSo()
    {
        var handler = new Mock<HttpMessageHandler>();
        handler.Protected()
            .Setup<Task<HttpResponseMessage>>("SendAsync", ItExpr.IsAny<HttpRequestMessage>(), ItExpr.IsAny<CancellationToken>())
            .ThrowsAsync(new HttpRequestException("No connection could be made"));
        var service = CreateService(new HttpClient(handler.Object));

        var result = await service.ListModelsAsync(new ListAiModelsRequest("ollama"));

        result.Success.Should().BeFalse();
        result.ErrorMessage.Should().Contain("ollama.com");
    }

    [Fact]
    public async Task ListModelsAsync_CloudProviderWithoutKey_AsksForKey()
    {
        var service = CreateService(CreateMockHttpClient(HttpStatusCode.OK, "{}"));

        var result = await service.ListModelsAsync(new ListAiModelsRequest("groq", ""));

        result.Success.Should().BeFalse();
        result.ErrorMessage.Should().Contain("API key");
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
