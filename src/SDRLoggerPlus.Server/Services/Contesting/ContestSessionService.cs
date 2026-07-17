using SDRLoggerPlus.Contracts.Models.Contesting;
using SDRLoggerPlus.Server.Core.Database;

namespace SDRLoggerPlus.Server.Services.Contesting;

/// <summary>
/// Owns contest sessions (start / activate / stop) and hands out serial numbers.
/// Serial allocation is delegated to the pure <see cref="ContestSerials"/> helper;
/// this service persists the advanced counters. Scoped so it can use the scoped
/// session repository directly.
/// </summary>
public class ContestSessionService
{
    private readonly IContestSessionRepository _repo;
    private readonly ContestDefinitionService _definitions;
    private readonly ILogger<ContestSessionService> _logger;

    public ContestSessionService(
        IContestSessionRepository repo,
        ContestDefinitionService definitions,
        ILogger<ContestSessionService> logger)
    {
        _repo = repo;
        _definitions = definitions;
        _logger = logger;
    }

    public Task<List<ContestSession>> GetAllAsync() => _repo.GetAllAsync();

    public Task<ContestSession?> GetActiveAsync() => _repo.GetActiveAsync();

    public Task<ContestSession?> GetAsync(string id) => _repo.GetByIdAsync(id);

    /// <summary>Start a new session for a definition and make it the active one.</summary>
    public async Task<ContestSession> StartAsync(string definitionId, MyExchange me, string? label)
    {
        var def = _definitions.Get(definitionId)
            ?? throw new ContestDefinitionException($"Unknown contest '{definitionId}'.");

        await _repo.DeactivateAllAsync();

        var session = new ContestSession
        {
            DefinitionId = def.Id,
            Label = string.IsNullOrWhiteSpace(label) ? $"{def.Name} {DateTime.UtcNow:yyyy}" : label!,
            MyExchange = me,
            Role = ContestScoringEngine.DetermineRole(def, me),
            StartedAt = DateTime.UtcNow,
            Active = true,
        };
        await _repo.UpsertAsync(session);
        _logger.LogInformation("Started contest session {Id} ({Def})", session.Id, def.Id);
        return session;
    }

    /// <summary>Make an existing session the active one (deactivates the rest).</summary>
    public async Task<ContestSession> ActivateAsync(string id)
    {
        var session = await _repo.GetByIdAsync(id)
            ?? throw new ContestDefinitionException($"No session '{id}'.");
        await _repo.DeactivateAllAsync();
        session.Active = true;
        await _repo.UpsertAsync(session);
        return session;
    }

    /// <summary>Stop a session (marks it ended and inactive).</summary>
    public async Task StopAsync(string id)
    {
        var session = await _repo.GetByIdAsync(id);
        if (session == null) return;
        session.Active = false;
        session.EndedAt = DateTime.UtcNow;
        await _repo.UpsertAsync(session);
    }

    public Task<bool> DeleteAsync(string id) => _repo.DeleteAsync(id);

    /// <summary>
    /// Allocate the next serial for the active session on a band and persist the
    /// advanced counter. Returns 0 when there is no active session or the contest
    /// does not use serials.
    /// </summary>
    public async Task<int> AllocateSerialAsync(string band)
    {
        var session = await _repo.GetActiveAsync();
        if (session == null) return 0;
        var def = _definitions.Get(session.DefinitionId);
        if (def == null || def.Serial == SerialMode.None) return 0;

        var serial = ContestSerials.Allocate(session, def.Serial, band);
        await _repo.UpsertAsync(session);
        return serial;
    }

    /// <summary>The serial the next QSO on this band would get, without advancing.</summary>
    public async Task<int> PeekSerialAsync(string band)
    {
        var session = await _repo.GetActiveAsync();
        if (session == null) return 0;
        var def = _definitions.Get(session.DefinitionId);
        if (def == null) return 0;
        return ContestSerials.Peek(session, def.Serial, band);
    }
}
