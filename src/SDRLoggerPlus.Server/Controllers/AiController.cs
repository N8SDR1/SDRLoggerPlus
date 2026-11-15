using System.Text.Json;
using Microsoft.AspNetCore.Mvc;
using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Server.Services;

namespace SDRLoggerPlus.Server.Controllers;

[ApiController]
[Route("api/[controller]")]
[Produces("application/json")]
public class AiController : ControllerBase
{
    private readonly IAiService _aiService;
    private readonly ILogger<AiController> _logger;

    public AiController(IAiService aiService, ILogger<AiController> logger)
    {
        _aiService = aiService;
        _logger = logger;
    }

    /// <summary>
    /// Generate talk points for a callsign based on QSO history and QRZ profile
    /// </summary>
    [HttpPost("talk-points")]
    [ProducesResponseType(typeof(GenerateTalkPointsResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    [ProducesResponseType(StatusCodes.Status500InternalServerError)]
    public async Task<ActionResult<GenerateTalkPointsResponse>> GenerateTalkPoints(
        [FromBody] GenerateTalkPointsRequest request)
    {
        try
        {
            var response = await _aiService.GenerateTalkPointsAsync(request);
            return Ok(response);
        }
        catch (InvalidOperationException ex)
        {
            _logger.LogWarning(ex, "Invalid AI configuration");
            return BadRequest(new { error = ex.Message });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error generating talk points for {Callsign}", request.Callsign);
            return StatusCode(502, new { error = OperatorMessage(ex) });
        }
    }

    /// <summary>
    /// Chat with AI about a callsign
    /// </summary>
    [HttpPost("chat")]
    [ProducesResponseType(typeof(ChatResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    [ProducesResponseType(StatusCodes.Status500InternalServerError)]
    public async Task<ActionResult<ChatResponse>> Chat([FromBody] ChatRequest request)
    {
        try
        {
            var response = await _aiService.ChatAsync(request);
            return Ok(response);
        }
        catch (InvalidOperationException ex)
        {
            _logger.LogWarning(ex, "Invalid AI configuration");
            return BadRequest(new { error = ex.Message });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing chat for {Callsign}", request.Callsign);
            return StatusCode(502, new { error = OperatorMessage(ex) });
        }
    }

    /// <summary>
    /// Stream chat with AI about a callsign using Server-Sent Events
    /// </summary>
    [HttpPost("chat/stream")]
    [Produces("text/event-stream")]
    public async Task ChatStream([FromBody] ChatRequest request)
    {
        _logger.LogInformation("Starting chat stream for {Callsign}", request.Callsign);

        // Disable response buffering for SSE
        var bufferingFeature = HttpContext.Features.Get<Microsoft.AspNetCore.Http.Features.IHttpResponseBodyFeature>();
        bufferingFeature?.DisableBuffering();

        Response.ContentType = "text/event-stream";
        Response.Headers.CacheControl = "no-cache";
        Response.Headers["X-Accel-Buffering"] = "no";

        try
        {
            await foreach (var token in _aiService.ChatStreamAsync(request, HttpContext.RequestAborted))
            {
                var json = JsonSerializer.Serialize(new { token });
                await Response.WriteAsync($"data: {json}\n\n", HttpContext.RequestAborted);
                await Response.Body.FlushAsync(HttpContext.RequestAborted);
            }

            await Response.WriteAsync("data: [DONE]\n\n", HttpContext.RequestAborted);
            await Response.Body.FlushAsync(HttpContext.RequestAborted);
            _logger.LogInformation("Chat stream completed for {Callsign}", request.Callsign);
        }
        catch (InvalidOperationException ex)
        {
            _logger.LogWarning(ex, "Invalid AI configuration for stream");
            if (!Response.HasStarted)
            {
                Response.StatusCode = 400;
                var errorJson = JsonSerializer.Serialize(new { error = ex.Message });
                await Response.WriteAsync($"data: {errorJson}\n\n");
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Error streaming chat for {Callsign}", request.Callsign);
            // Mid-stream the status line is already sent, so report the failure as
            // an SSE error event either way; the client throws on it.
            if (!Response.HasStarted) Response.StatusCode = 502;
            var errorJson = JsonSerializer.Serialize(new { error = OperatorMessage(ex) });
            await Response.WriteAsync($"data: {errorJson}\n\n");
        }
    }

    /// <summary>
    /// List the chat models this provider offers for this key (or local install) right now.
    /// </summary>
    [HttpPost("models")]
    [ProducesResponseType(typeof(ListAiModelsResponse), StatusCodes.Status200OK)]
    public async Task<ActionResult<ListAiModelsResponse>> ListModels([FromBody] ListAiModelsRequest request)
    {
        var response = await _aiService.ListModelsAsync(request, HttpContext.RequestAborted);
        return Ok(response);
    }

    // Provider failures carry a message written for the operator; anything else
    // is an internal fault, so keep its details in the log.
    private static string OperatorMessage(Exception ex) => ex switch
    {
        AiProviderException => ex.Message,
        TaskCanceledException => "The AI provider took too long to answer (over 60 seconds).",
        HttpRequestException { StatusCode: null } =>
            "Can't reach the AI provider. Check your internet connection, or that Ollama is running.",
        _ => "Something went wrong talking to the AI provider. See the app log for details.",
    };

    /// <summary>
    /// Test an AI API key
    /// </summary>
    [HttpPost("test-key")]
    [ProducesResponseType(typeof(TestApiKeyResponse), StatusCodes.Status200OK)]
    public async Task<ActionResult<TestApiKeyResponse>> TestApiKey([FromBody] TestApiKeyRequest request)
    {
        var response = await _aiService.TestApiKeyAsync(request);
        return Ok(response);
    }
}
