using Microsoft.AspNetCore.Mvc;
using SDRLoggerPlus.Server.Services;

namespace SDRLoggerPlus.Server.Controllers;

/// <summary>
/// AD1C Country Files (cty.dat) status + update endpoint.
///
/// cty.dat is bundled with the app as an embedded resource, but the callsign
/// lookup fallback path (<see cref="CtyService.GetCentroidFromCallsign"/>)
/// prefers a user-supplied override at
/// <see cref="CtyService.GetUserOverridePath"/> so users can refresh to the
/// latest release from Settings → Country Files → Update.
/// </summary>
[ApiController]
[Route("api/[controller]")]
[Produces("application/json")]
public class CtyController : ControllerBase
{
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ILogger<CtyController> _logger;

    // Serialize concurrent updates so a click-happy user can't tear the file
    // by starting a second download before the first has renamed into place.
    private static readonly SemaphoreSlim _updateLock = new(1, 1);

    public CtyController(IHttpClientFactory httpClientFactory, ILogger<CtyController> logger)
    {
        _httpClientFactory = httpClientFactory;
        _logger = logger;
    }

    /// <summary>
    /// Current cty.dat status: prefix count, source (bundled vs user-updated),
    /// last-updated timestamp of any user override, detected version string,
    /// and the canonical download URL used by <see cref="Update"/>.
    /// </summary>
    [HttpGet("status")]
    public IActionResult Status()
    {
        var status = CtyService.GetStatus();
        return Ok(status);
    }

    /// <summary>
    /// Download the latest cty.dat from country-files.com, validate it,
    /// atomically replace the user override file, and reload the in-memory
    /// prefix map. The bundled resource is never touched.
    /// </summary>
    [HttpPost("update")]
    public async Task<IActionResult> Update(CancellationToken cancellationToken)
    {
        // Prevent concurrent updates from racing on the target file.
        if (!await _updateLock.WaitAsync(0, cancellationToken))
        {
            return Conflict(new { error = "A cty.dat update is already in progress" });
        }

        try
        {
            var http = _httpClientFactory.CreateClient();
            http.Timeout = TimeSpan.FromSeconds(30);
            http.DefaultRequestHeaders.UserAgent.ParseAdd("SDRLoggerPlus/2.0 (+https://github.com/N8SDR1/SDRLoggerPlus)");

            string content;
            try
            {
                using var resp = await http.GetAsync(CtyService.SourceUrl, cancellationToken);
                if (!resp.IsSuccessStatusCode)
                {
                    _logger.LogWarning("cty.dat download failed: HTTP {Status} from {Url}",
                        (int)resp.StatusCode, CtyService.SourceUrl);
                    return StatusCode(502, new { error = $"Download failed: HTTP {(int)resp.StatusCode}" });
                }
                content = await resp.Content.ReadAsStringAsync(cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "cty.dat download exception");
                return StatusCode(502, new { error = "Failed to reach country-files.com", detail = ex.Message });
            }

            // Sanity-check the downloaded content: a real cty.dat is ~100 KB,
            // has thousands of prefix entries, and always contains "United States".
            // Reject anything smaller than 50 KB or missing an entity that has
            // been in cty.dat for decades — protects against a captive-portal HTML
            // page or a truncated response landing on disk.
            if (content.Length < 50_000
                || !content.Contains(':')
                || !content.Contains("United States", StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogWarning("cty.dat validation failed: length={Length}", content.Length);
                return StatusCode(502, new { error = "Downloaded file did not look like a valid cty.dat" });
            }

            // Atomic replace: write to .new, then move over the live path so
            // an in-flight read can never see a half-written file.
            var targetPath = CtyService.GetUserOverridePath();
            var targetDir = Path.GetDirectoryName(targetPath)!;
            Directory.CreateDirectory(targetDir);
            var tempPath = targetPath + ".new";
            await System.IO.File.WriteAllTextAsync(tempPath, content, cancellationToken);
            System.IO.File.Move(tempPath, targetPath, overwrite: true);

            CtyService.ReloadFromDisk();

            var status = CtyService.GetStatus();
            _logger.LogInformation("cty.dat updated: version={Version}, prefixes={Count}",
                status.Version ?? "-", status.PrefixCount);

            return Ok(status);
        }
        finally
        {
            _updateLock.Release();
        }
    }
}
