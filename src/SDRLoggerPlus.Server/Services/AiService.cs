using System.Net.Http.Headers;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database;

namespace SDRLoggerPlus.Server.Services;

public class AiService : IAiService
{
    private readonly ISettingsRepository _settingsRepository;
    private readonly IQsoService _qsoService;
    private readonly IQrzService _qrzService;
    private readonly HttpClient _httpClient;
    private readonly ILogger<AiService> _logger;

    public AiService(
        ISettingsRepository settingsRepository,
        IQsoService qsoService,
        IQrzService qrzService,
        IHttpClientFactory httpClientFactory,
        ILogger<AiService> logger)
    {
        _settingsRepository = settingsRepository;
        _qsoService = qsoService;
        _qrzService = qrzService;
        _httpClient = httpClientFactory.CreateClient("AI");
        _logger = logger;
    }

    public async Task<GenerateTalkPointsResponse> GenerateTalkPointsAsync(GenerateTalkPointsRequest request)
    {
        var settings = await _settingsRepository.GetAsync() ?? new UserSettings();
        var aiSettings = settings.Ai;

        EnsureConfigured(aiSettings);

        // Gather context data
        var previousQsos = await GetPreviousQsosAsync(request.Callsign);
        var qrzProfile = aiSettings.IncludeQrzProfile
            ? await GetQrzProfileAsync(request.Callsign)
            : null;

        // Build prompt
        var context = BuildTalkPointsContext(
            settings.Station.Callsign,
            request.Callsign,
            previousQsos,
            qrzProfile,
            request.CurrentBand,
            request.CurrentMode
        );

        // Call LLM
        var response = await CallLlmAsync(aiSettings, context);

        // Parse response
        var talkPoints = ParseTalkPoints(response);

        return new GenerateTalkPointsResponse(
            request.Callsign,
            previousQsos,
            qrzProfile,
            talkPoints,
            response
        );
    }

    public async Task<ChatResponse> ChatAsync(ChatRequest request)
    {
        var settings = await _settingsRepository.GetAsync() ?? new UserSettings();
        var aiSettings = settings.Ai;

        EnsureConfigured(aiSettings);

        // Gather context for this callsign
        var previousQsos = await GetPreviousQsosAsync(request.Callsign);
        var qrzProfile = aiSettings.IncludeQrzProfile
            ? await GetQrzProfileAsync(request.Callsign)
            : null;

        // Build context with conversation history
        var messages = BuildChatMessages(
            settings.Station.Callsign,
            request.Callsign,
            previousQsos,
            qrzProfile,
            request.Question,
            request.ConversationHistory
        );

        // Call LLM with conversation
        var response = await CallLlmWithMessagesAsync(aiSettings, messages);

        return new ChatResponse(response);
    }

    public async IAsyncEnumerable<string> ChatStreamAsync(
        ChatRequest request,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var settings = await _settingsRepository.GetAsync() ?? new UserSettings();
        var aiSettings = settings.Ai;

        EnsureConfigured(aiSettings);

        var previousQsos = await GetPreviousQsosAsync(request.Callsign);
        var qrzProfile = aiSettings.IncludeQrzProfile
            ? await GetQrzProfileAsync(request.Callsign)
            : null;

        var messages = BuildChatMessages(
            settings.Station.Callsign,
            request.Callsign,
            previousQsos,
            qrzProfile,
            request.Question,
            request.ConversationHistory
        );

        await foreach (var token in StreamLlmAsync(aiSettings, messages, cancellationToken))
        {
            yield return token;
        }
    }

    public async Task<TestApiKeyResponse> TestApiKeyAsync(TestApiKeyRequest request)
    {
        try
        {
            var settings = new AiSettings
            {
                Provider = request.Provider,
                ApiKey = request.ApiKey,
                Model = request.Model,
                BaseUrl = request.BaseUrl
            };
            EnsureConfigured(settings);
            await CallLlmAsync(settings, "Reply with 'OK' if you can read this.");

            return new TestApiKeyResponse(true);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "API key test failed");
            return new TestApiKeyResponse(false, DescribeFailure(ex, request.Provider));
        }
    }

    public async Task<ListAiModelsResponse> ListModelsAsync(ListAiModelsRequest request, CancellationToken cancellationToken = default)
    {
        var settings = new AiSettings
        {
            Provider = request.Provider,
            ApiKey = request.ApiKey,
            BaseUrl = request.BaseUrl
        };

        if (RequiresApiKey(settings) && string.IsNullOrWhiteSpace(settings.ApiKey))
            return new ListAiModelsResponse(false, new(), "Enter your API key first.");

        try
        {
            var models = IsAnthropic(settings)
                ? await ListAnthropicModelsAsync(settings, cancellationToken)
                : await ListOpenAiCompatibleModelsAsync(settings, cancellationToken);

            if (models.Count == 0)
            {
                var hint = IsProvider(settings, "ollama")
                    ? "Ollama is running but has no models yet. Open a terminal and run: ollama pull llama3.2"
                    : "The provider returned no chat models for this key.";
                return new ListAiModelsResponse(false, models, hint);
            }

            return new ListAiModelsResponse(true, models);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            _logger.LogWarning(ex, "Listing AI models failed for {Provider}", request.Provider);
            return new ListAiModelsResponse(false, new(), DescribeFailure(ex, request.Provider));
        }
    }

    private async Task<List<PreviousQsoSummary>> GetPreviousQsosAsync(string callsign)
    {
        var searchRequest = new QsoSearchRequest(
            Callsign: callsign,
            Name: null,
            Band: null,
            Mode: null,
            FromDate: null,
            ToDate: null,
            Limit: 10,
            Skip: 0
        );

        var result = await _qsoService.GetQsosAsync(searchRequest);

        return result.Items
            .OrderByDescending(q => q.QsoDate)
            .Select(q => new PreviousQsoSummary(
                q.QsoDate.ToString("yyyy-MM-dd"),
                q.Band,
                q.Mode,
                q.RstSent,
                q.RstRcvd,
                q.Comment
            ))
            .ToList();
    }

    private async Task<QrzProfileSummary?> GetQrzProfileAsync(string callsign)
    {
        try
        {
            var profile = await _qrzService.LookupCallsignAsync(callsign);
            if (profile == null) return null;

            // Fetch biography in parallel with building the basic profile
            var bioText = await _qrzService.GetBiographyAsync(callsign);

            // Build bio combining email and biography text
            var bioParts = new List<string>();
            if (!string.IsNullOrEmpty(profile.Email))
                bioParts.Add($"Email: {profile.Email}");
            if (!string.IsNullOrEmpty(bioText))
                bioParts.Add(bioText);

            var bio = bioParts.Count > 0 ? string.Join("\n", bioParts) : null;

            return new QrzProfileSummary(
                profile.Name,
                $"{profile.City}, {profile.State}, {profile.Country}".Trim(' ', ','),
                profile.Grid,
                bio,
                null
            );
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to get QRZ profile for {Callsign}", callsign);
            return null;
        }
    }

    private string BuildTalkPointsContext(
        string myCallsign,
        string theirCallsign,
        List<PreviousQsoSummary> previousQsos,
        QrzProfileSummary? qrzProfile,
        string? currentBand,
        string? currentMode)
    {
        var sb = new StringBuilder();
        sb.AppendLine("You are a ham radio operating assistant. Generate concise, practical talk points for a QSO.");
        sb.AppendLine();
        sb.AppendLine($"My callsign: {myCallsign}");
        sb.AppendLine($"Their callsign: {theirCallsign}");

        if (!string.IsNullOrEmpty(currentBand))
            sb.AppendLine($"Current band: {currentBand}");
        if (!string.IsNullOrEmpty(currentMode))
            sb.AppendLine($"Current mode: {currentMode}");

        if (qrzProfile != null)
        {
            sb.AppendLine();
            sb.AppendLine("Their QRZ Profile:");
            if (!string.IsNullOrEmpty(qrzProfile.Name))
                sb.AppendLine($"  Name: {qrzProfile.Name}");
            if (!string.IsNullOrEmpty(qrzProfile.Location))
                sb.AppendLine($"  Location: {qrzProfile.Location}");
            if (!string.IsNullOrEmpty(qrzProfile.Grid))
                sb.AppendLine($"  Grid: {qrzProfile.Grid}");
            if (!string.IsNullOrEmpty(qrzProfile.Bio))
            {
                sb.AppendLine();
                sb.AppendLine("Their QRZ Biography (equipment, interests, station details):");
                sb.AppendLine(qrzProfile.Bio);
            }
        }

        if (previousQsos.Any())
        {
            sb.AppendLine();
            sb.AppendLine($"Previous QSOs with {theirCallsign} ({previousQsos.Count}):");
            foreach (var qso in previousQsos.Take(5))
            {
                sb.Append($"  - {qso.QsoDate} on {qso.Band} {qso.Mode}");
                if (!string.IsNullOrEmpty(qso.Comment))
                    sb.Append($" — \"{qso.Comment}\"");
                sb.AppendLine();
            }
        }
        else
        {
            sb.AppendLine();
            sb.AppendLine($"No previous QSOs with {theirCallsign}.");
        }

        sb.AppendLine();
        sb.AppendLine("Generate 3-5 natural talk points. Each point should be a complete sentence suggesting what to mention during the contact. Focus on personal connections, previous conversations, and shared interests.");

        return sb.ToString();
    }

    private List<LlmMessage> BuildChatMessages(
        string myCallsign,
        string theirCallsign,
        List<PreviousQsoSummary> previousQsos,
        QrzProfileSummary? qrzProfile,
        string question,
        List<ChatMessage>? conversationHistory)
    {
        var messages = new List<LlmMessage>();

        // System message with context
        var systemContent = new StringBuilder();
        systemContent.AppendLine($"You are a ham radio operating assistant helping {myCallsign} with information about {theirCallsign}.");

        if (qrzProfile != null)
        {
            systemContent.AppendLine($"\nTheir profile: {qrzProfile.Name}, {qrzProfile.Location}, Grid: {qrzProfile.Grid}");
            if (!string.IsNullOrEmpty(qrzProfile.Bio))
            {
                systemContent.AppendLine($"\nTheir QRZ biography (equipment, interests, station details):\n{qrzProfile.Bio}");
            }
        }

        if (previousQsos.Any())
        {
            systemContent.AppendLine($"\nPrevious QSOs: {previousQsos.Count} contacts");
            foreach (var qso in previousQsos.Take(3))
            {
                systemContent.Append($"- {qso.QsoDate} {qso.Band} {qso.Mode}");
                if (!string.IsNullOrEmpty(qso.Comment))
                    systemContent.Append($" ({qso.Comment})");
                systemContent.AppendLine();
            }
        }

        messages.Add(new LlmMessage("system", systemContent.ToString()));

        // Add conversation history
        if (conversationHistory != null)
        {
            // Drop blanks (an aborted reply leaves an empty assistant turn, which
            // Anthropic rejects) and anything that isn't a plain user/assistant turn.
            foreach (var msg in conversationHistory)
            {
                if (string.IsNullOrWhiteSpace(msg.Content)) continue;
                var role = string.Equals(msg.Role, "assistant", StringComparison.OrdinalIgnoreCase) ? "assistant" : "user";
                messages.Add(new LlmMessage(role, msg.Content));
            }
        }

        // Add current question
        messages.Add(new LlmMessage("user", question));

        return messages;
    }

    // ── LLM transport ────────────────────────────────────────────────────────
    // Two wire formats cover every supported provider: Anthropic's native
    // Messages API, and OpenAI chat-completions, which OpenAI, Google Gemini,
    // Groq, OpenRouter, Ollama, LM Studio and any "custom" server all speak.
    //
    // Model names are never hard-coded here or in the UI: providers retire them
    // every few months, which is what broke every AI feature last time. The
    // Settings screen asks the provider for its live list (ListModelsAsync).

    private const string AnthropicApi = "https://api.anthropic.com/v1";
    private const string AnthropicVersion = "2023-06-01";
    private const int AnthropicMaxTokens = 1024;

    // Reasoning models (GPT-5 / o-series, Gemini 2.5, gpt-oss, Qwen3, DeepSeek-R1…)
    // spend part of this budget thinking before they write anything; at 1024 they
    // can finish with an empty reply. 4096 leaves room for both.
    private const int OpenAiMaxTokens = 4096;

    private record LlmMessage(
        [property: JsonPropertyName("role")] string Role,
        [property: JsonPropertyName("content")] string Content);

    private Task<string> CallLlmAsync(AiSettings settings, string prompt) =>
        CallLlmWithMessagesAsync(settings, new List<LlmMessage> { new("user", prompt) });

    private async Task<string> CallLlmWithMessagesAsync(
        AiSettings settings, List<LlmMessage> messages, CancellationToken ct = default)
    {
        using var httpRequest = BuildChatRequest(settings, messages, stream: false);
        using var response = await _httpClient.SendAsync(httpRequest, ct);
        await EnsureSuccessOrThrowAsync(response, ct);

        var body = await response.Content.ReadAsStringAsync(ct);
        return IsAnthropic(settings) ? ReadAnthropicReply(body) : ReadOpenAiReply(body);
    }

    private HttpRequestMessage BuildChatRequest(AiSettings settings, List<LlmMessage> messages, bool stream)
    {
        var body = new Dictionary<string, object?> { ["model"] = settings.Model };
        string url;

        if (IsAnthropic(settings))
        {
            // Anthropic takes the system prompt as a top-level field; a "system"
            // role inside messages is rejected with a 400.
            var system = string.Join("\n\n", messages.Where(m => m.Role == "system").Select(m => m.Content));
            body["max_tokens"] = AnthropicMaxTokens;
            body["messages"] = messages.Where(m => m.Role != "system").ToList();
            if (system.Length > 0) body["system"] = system;
            url = AnthropicApi + "/messages";
        }
        else
        {
            var baseUrl = ResolveOpenAiBaseUrl(settings);
            body["messages"] = messages;
            // OpenAI's own API wants max_completion_tokens (its reasoning models
            // refuse max_tokens); every other OpenAI-compatible server takes max_tokens.
            body[IsOpenAiHost(baseUrl) ? "max_completion_tokens" : "max_tokens"] = OpenAiMaxTokens;
            url = baseUrl + "/chat/completions";
        }

        if (stream) body["stream"] = true;

        var request = new HttpRequestMessage(HttpMethod.Post, url)
        {
            Content = new StringContent(JsonSerializer.Serialize(body), Encoding.UTF8, "application/json")
        };
        AddAuth(request, settings);
        return request;
    }

    private static void AddAuth(HttpRequestMessage request, AiSettings settings)
    {
        if (IsAnthropic(settings))
        {
            request.Headers.Add("x-api-key", settings.ApiKey ?? "");
            request.Headers.Add("anthropic-version", AnthropicVersion);
            return;
        }

        if (!string.IsNullOrWhiteSpace(settings.ApiKey))
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", settings.ApiKey.Trim());

        if (IsProvider(settings, "openrouter"))
            request.Headers.Add("X-Title", "SDRLogger+");
    }

    private static string ReadAnthropicReply(string body)
    {
        using var doc = JsonDocument.Parse(body);
        var root = doc.RootElement;

        var text = new StringBuilder();
        if (root.TryGetProperty("content", out var blocks) && blocks.ValueKind == JsonValueKind.Array)
        {
            foreach (var block in blocks.EnumerateArray())
            {
                if (block.TryGetProperty("text", out var t) && t.ValueKind == JsonValueKind.String)
                    text.Append(t.GetString());
            }
        }

        if (text.Length > 0) return text.ToString();

        var stop = root.TryGetProperty("stop_reason", out var sr) ? sr.GetString() : null;
        throw new AiProviderException(stop == "max_tokens"
            ? "The model ran out of room before answering. Pick a different model."
            : "The model returned an empty reply.");
    }

    private static string ReadOpenAiReply(string body)
    {
        using var doc = JsonDocument.Parse(body);
        var root = doc.RootElement;

        if (root.TryGetProperty("choices", out var choices) &&
            choices.ValueKind == JsonValueKind.Array && choices.GetArrayLength() > 0)
        {
            var choice = choices[0];
            string? content = null;
            if (choice.TryGetProperty("message", out var msg) &&
                msg.TryGetProperty("content", out var c) && c.ValueKind == JsonValueKind.String)
            {
                content = StripThinking(c.GetString());
            }

            if (!string.IsNullOrWhiteSpace(content)) return content;

            var finish = choice.TryGetProperty("finish_reason", out var fr) && fr.ValueKind == JsonValueKind.String
                ? fr.GetString() : null;
            if (finish == "length")
                throw new AiProviderException(
                    "The model used its whole reply budget thinking and wrote nothing. Pick a smaller or non-reasoning model.");
        }

        var error = TryExtractErrorMessage(body);
        throw new AiProviderException(error ?? "The model returned an empty reply.");
    }

    // Local reasoning models (DeepSeek-R1, Qwen3 on older Ollama builds) put their
    // chain of thought inline as <think>…</think>; the operator only wants the answer.
    private static string? StripThinking(string? text) =>
        text == null ? null : System.Text.RegularExpressions.Regex
            .Replace(text, @"<think>[\s\S]*?</think>", "").Trim();

    private static bool IsAnthropic(AiSettings s) => IsProvider(s, "anthropic");

    private static bool IsProvider(AiSettings s, string provider) =>
        string.Equals(s.Provider, provider, StringComparison.OrdinalIgnoreCase);

    // Cloud providers need a key. Local servers (Ollama, LM Studio via "custom")
    // usually don't, so for those the key is optional.
    private static bool RequiresApiKey(AiSettings s) =>
        (s.Provider?.ToLowerInvariant()) is "anthropic" or "openai" or "gemini" or "groq" or "openrouter";

    private static void EnsureConfigured(AiSettings s)
    {
        if (RequiresApiKey(s) && string.IsNullOrWhiteSpace(s.ApiKey))
            throw new InvalidOperationException("AI API key not configured. Add it in Settings → Chat AI.");
        if (string.IsNullOrWhiteSpace(s.Model))
            throw new InvalidOperationException("No AI model selected. Choose one in Settings → Chat AI.");
    }

    // The OpenAI-compatible base URL for this provider. An explicit BaseUrl wins;
    // otherwise fall back to the preset's known default.
    private static string ResolveOpenAiBaseUrl(AiSettings s)
    {
        if (!string.IsNullOrWhiteSpace(s.BaseUrl))
            return s.BaseUrl.Trim().TrimEnd('/');

        return (s.Provider?.ToLowerInvariant()) switch
        {
            "gemini" => "https://generativelanguage.googleapis.com/v1beta/openai",
            "groq" => "https://api.groq.com/openai/v1",
            "openrouter" => "https://openrouter.ai/api/v1",
            "ollama" => "http://localhost:11434/v1",
            _ => "https://api.openai.com/v1",
        };
    }

    private static bool IsOpenAiHost(string baseUrl) =>
        Uri.TryCreate(baseUrl, UriKind.Absolute, out var uri) &&
        uri.Host.Equals("api.openai.com", StringComparison.OrdinalIgnoreCase);

    // EnsureSuccessStatusCode() throws away the response body, so a bad model
    // name or parameter surfaces only as "400 Bad Request". Read the provider's
    // JSON error and put its message in the exception instead.
    private static async Task EnsureSuccessOrThrowAsync(HttpResponseMessage response, CancellationToken ct = default)
    {
        if (response.IsSuccessStatusCode) return;

        string body;
        try { body = await response.Content.ReadAsStringAsync(ct); }
        catch { body = string.Empty; }

        // Prefer the provider's structured message; fall back to a raw snippet.
        var detail = TryExtractErrorMessage(body);
        if (string.IsNullOrWhiteSpace(detail) && !string.IsNullOrWhiteSpace(body))
            detail = body.Length > 300 ? body[..300] + "…" : body;

        var status = (int)response.StatusCode;
        var lead = status switch
        {
            401 or 403 => "The provider rejected the API key",
            404 => "The provider doesn't recognise that model or address. Refresh the model list in Settings → Chat AI",
            429 => "Rate limit or quota reached. Free tiers allow only so many requests per minute/day",
            >= 500 => "The provider is having trouble right now. Try again shortly",
            _ => "The provider refused the request",
        };

        var suffix = string.IsNullOrWhiteSpace(detail) ? "" : $": {detail!.Trim()}";
        throw new AiProviderException($"{lead} ({status}){suffix}");
    }

    // Pull the human message from a provider error body. Handles the common
    // { "error": { "message": ... } } (OpenAI / Anthropic) and Google's variant
    // that wraps the error object in a top-level array. Never throws — a parsing
    // quirk must not mask the real HTTP failure.
    private static string? TryExtractErrorMessage(string body)
    {
        if (string.IsNullOrWhiteSpace(body)) return null;
        try
        {
            using var doc = JsonDocument.Parse(body);
            var root = doc.RootElement;
            if (root.ValueKind == JsonValueKind.Array && root.GetArrayLength() > 0)
                root = root[0];
            if (root.ValueKind != JsonValueKind.Object) return null;

            if (root.TryGetProperty("error", out var err))
            {
                if (err.ValueKind == JsonValueKind.Object && err.TryGetProperty("message", out var m))
                    return m.GetString();
                if (err.ValueKind == JsonValueKind.String)
                    return err.GetString();
            }
            if (root.TryGetProperty("message", out var rootMsg) && rootMsg.ValueKind == JsonValueKind.String)
                return rootMsg.GetString();
        }
        catch (Exception) { }
        return null;
    }

    /// <summary>Turn any failure into one sentence the operator can act on.</summary>
    private static string DescribeFailure(Exception ex, string? provider)
    {
        var isOllama = string.Equals(provider, "ollama", StringComparison.OrdinalIgnoreCase);
        return ex switch
        {
            AiProviderException or InvalidOperationException => ex.Message,
            TaskCanceledException or TimeoutException =>
                "The provider took too long to answer (over 60 seconds).",
            HttpRequestException { StatusCode: null } when isOllama =>
                "Can't reach Ollama on this PC. Install it from ollama.com and make sure it is running.",
            HttpRequestException { StatusCode: null } =>
                $"Can't reach the provider. Check your internet connection and the API Base URL ({ex.Message}).",
            JsonException => "The provider sent back something that isn't a valid reply. Check the API Base URL.",
            _ => ex.Message,
        };
    }

    // ── Live model lists ─────────────────────────────────────────────────────

    private async Task<List<AiModelInfo>> ListAnthropicModelsAsync(AiSettings settings, CancellationToken ct)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, AnthropicApi + "/models?limit=1000");
        AddAuth(request, settings);
        using var response = await _httpClient.SendAsync(request, ct);
        await EnsureSuccessOrThrowAsync(response, ct);

        using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync(ct));
        var models = new List<AiModelInfo>();
        if (doc.RootElement.TryGetProperty("data", out var data) && data.ValueKind == JsonValueKind.Array)
        {
            // Anthropic lists newest first — keep that order.
            foreach (var m in data.EnumerateArray())
            {
                var id = m.TryGetProperty("id", out var i) ? i.GetString() : null;
                if (string.IsNullOrWhiteSpace(id)) continue;
                var name = m.TryGetProperty("display_name", out var n) ? n.GetString() : null;
                models.Add(new AiModelInfo(id, name));
            }
        }
        return models;
    }

    // Model ids that are not text-chat models (embeddings, speech, images, video,
    // moderation…). Providers mix these into /models, and choosing one fails.
    private static readonly System.Text.RegularExpressions.Regex NonChatModel = new(
        @"embed|whisper|tts|transcri|speech|audio|realtime|dall-e|image|imagen|veo|sora|moderation|guard|aqa|davinci|babbage|computer-use|search-preview|deep-research",
        System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.Compiled);

    private async Task<List<AiModelInfo>> ListOpenAiCompatibleModelsAsync(AiSettings settings, CancellationToken ct)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, ResolveOpenAiBaseUrl(settings) + "/models");
        AddAuth(request, settings);
        using var response = await _httpClient.SendAsync(request, ct);
        await EnsureSuccessOrThrowAsync(response, ct);

        using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync(ct));
        var isOpenRouter = IsProvider(settings, "openrouter");
        var models = new List<AiModelInfo>();

        if (doc.RootElement.TryGetProperty("data", out var data) && data.ValueKind == JsonValueKind.Array)
        {
            foreach (var m in data.EnumerateArray())
            {
                var id = m.TryGetProperty("id", out var i) ? i.GetString() : null;
                if (string.IsNullOrWhiteSpace(id)) continue;

                // Gemini returns "models/gemini-2.5-flash"; chat calls take the bare name.
                if (id.StartsWith("models/", StringComparison.Ordinal)) id = id["models/".Length..];

                if (NonChatModel.IsMatch(id)) continue;

                // Groq flags retired models as inactive.
                if (m.TryGetProperty("active", out var active) && active.ValueKind == JsonValueKind.False) continue;

                // OpenRouter: skip models that can't produce text.
                if (m.TryGetProperty("architecture", out var arch) &&
                    arch.TryGetProperty("output_modalities", out var outs) && outs.ValueKind == JsonValueKind.Array &&
                    !outs.EnumerateArray().Any(o => o.GetString() == "text"))
                    continue;

                var name = m.TryGetProperty("name", out var n) && n.ValueKind == JsonValueKind.String ? n.GetString() : null;
                var free = isOpenRouter && (id.EndsWith(":free", StringComparison.Ordinal) || IsZeroPriced(m));
                models.Add(new AiModelInfo(id, name, free));
            }
        }

        return models
            .DistinctBy(m => m.Id)
            .OrderByDescending(m => m.Free)
            .ThenBy(m => m.Id, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static bool IsZeroPriced(JsonElement model)
    {
        if (!model.TryGetProperty("pricing", out var p) || p.ValueKind != JsonValueKind.Object) return false;
        static bool Zero(JsonElement pricing, string key) =>
            pricing.TryGetProperty(key, out var v) && v.ValueKind == JsonValueKind.String &&
            decimal.TryParse(v.GetString(), System.Globalization.NumberStyles.Float,
                System.Globalization.CultureInfo.InvariantCulture, out var d) && d == 0m;
        return Zero(p, "prompt") && Zero(p, "completion");
    }

    // ── Streaming ────────────────────────────────────────────────────────────

    private async IAsyncEnumerable<string> StreamLlmAsync(
        AiSettings settings, List<LlmMessage> messages,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using var httpRequest = BuildChatRequest(settings, messages, stream: true);
        using var response = await _httpClient.SendAsync(httpRequest, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        await EnsureSuccessOrThrowAsync(response, cancellationToken);

        using var stream = await response.Content.ReadAsStreamAsync(cancellationToken);
        using var reader = new StreamReader(stream);

        var anthropic = IsAnthropic(settings);
        var produced = false;
        string? finish = null;

        string? line;
        while ((line = await reader.ReadLineAsync(cancellationToken)) != null)
        {
            if (!line.StartsWith("data:", StringComparison.Ordinal)) continue;
            var data = line[5..].Trim();
            if (data.Length == 0) continue;
            if (data == "[DONE]") break;

            var chunk = anthropic ? ParseAnthropicChunk(data) : ParseOpenAiChunk(data);
            if (chunk.Error != null) throw new AiProviderException(chunk.Error);
            if (chunk.Finish != null) finish = chunk.Finish;
            if (!string.IsNullOrEmpty(chunk.Token))
            {
                produced = true;
                yield return chunk.Token;
            }
            if (chunk.Stop) break;
        }

        if (!produced)
            throw new AiProviderException(finish is "length" or "max_tokens"
                ? "The model used its whole reply budget thinking and wrote nothing. Pick a smaller or non-reasoning model."
                : "The model returned an empty reply.");
    }

    private readonly record struct StreamChunk(string? Token, string? Finish, string? Error, bool Stop);

    private static StreamChunk ParseAnthropicChunk(string data)
    {
        try
        {
            using var doc = JsonDocument.Parse(data);
            var root = doc.RootElement;
            var type = root.TryGetProperty("type", out var t) ? t.GetString() : null;
            switch (type)
            {
                case "content_block_delta":
                    var token = root.TryGetProperty("delta", out var d) && d.TryGetProperty("text", out var tx)
                        ? tx.GetString() : null;
                    return new StreamChunk(token, null, null, false);
                case "message_delta":
                    var stop = root.TryGetProperty("delta", out var md) && md.TryGetProperty("stop_reason", out var s)
                        ? s.GetString() : null;
                    return new StreamChunk(null, stop, null, false);
                case "message_stop":
                    return new StreamChunk(null, null, null, true);
                case "error":
                    return new StreamChunk(null, null, TryExtractErrorMessage(data) ?? "The provider reported an error.", true);
            }
        }
        catch (JsonException) { }
        return default;
    }

    private static StreamChunk ParseOpenAiChunk(string data)
    {
        try
        {
            using var doc = JsonDocument.Parse(data);
            var root = doc.RootElement;

            // OpenRouter and some local servers report mid-stream failures as an
            // error object instead of an HTTP status.
            if (root.TryGetProperty("error", out _))
                return new StreamChunk(null, null, TryExtractErrorMessage(data) ?? "The provider reported an error.", true);

            if (!root.TryGetProperty("choices", out var choices) ||
                choices.ValueKind != JsonValueKind.Array || choices.GetArrayLength() == 0)
                return default;

            var choice = choices[0];
            string? token = null;
            if (choice.TryGetProperty("delta", out var delta) &&
                delta.TryGetProperty("content", out var c) && c.ValueKind == JsonValueKind.String)
                token = c.GetString();

            var finish = choice.TryGetProperty("finish_reason", out var fr) && fr.ValueKind == JsonValueKind.String
                ? fr.GetString() : null;
            return new StreamChunk(token, finish, null, false);
        }
        catch (JsonException) { }
        return default;
    }

    private List<string> ParseTalkPoints(string response)
    {
        // Simple parsing: split by lines and look for bullet points or numbered items
        var lines = response.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var talkPoints = new List<string>();

        foreach (var line in lines)
        {
            // Look for lines that start with bullets, numbers, or just contain substantial text
            if (line.StartsWith("-") || line.StartsWith("•") ||
                line.StartsWith("*") || char.IsDigit(line[0]))
            {
                var point = line.TrimStart('-', '•', '*', ' ', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '.', ')');
                if (!string.IsNullOrWhiteSpace(point))
                {
                    talkPoints.Add(point.Trim());
                }
            }
        }

        // If no bullet points found, return the whole response as one point
        if (talkPoints.Count == 0 && !string.IsNullOrWhiteSpace(response))
        {
            talkPoints.Add(response);
        }

        return talkPoints;
    }
}
