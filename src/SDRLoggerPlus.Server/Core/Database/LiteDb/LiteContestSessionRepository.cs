using SDRLoggerPlus.Contracts.Models.Contesting;

namespace SDRLoggerPlus.Server.Core.Database.LiteDb;

public class LiteContestSessionRepository : IContestSessionRepository
{
    private readonly LiteDbContext _context;

    public LiteContestSessionRepository(LiteDbContext context)
    {
        _context = context;
    }

    public Task<List<ContestSession>> GetAllAsync()
        => Task.FromResult(_context.ContestSessions.FindAll().OrderByDescending(s => s.StartedAt).ToList());

    public Task<ContestSession?> GetByIdAsync(string id)
        => Task.FromResult<ContestSession?>(_context.ContestSessions.FindById(id));

    public Task<ContestSession?> GetActiveAsync()
        => Task.FromResult<ContestSession?>(_context.ContestSessions.FindOne(s => s.Active));

    public Task UpsertAsync(ContestSession session)
    {
        _context.ContestSessions.Upsert(session);
        _context.Database.Checkpoint();
        return Task.CompletedTask;
    }

    public Task DeactivateAllAsync()
    {
        var active = _context.ContestSessions.Find(s => s.Active).ToList();
        foreach (var s in active)
        {
            s.Active = false;
            _context.ContestSessions.Update(s);
        }
        if (active.Count > 0) _context.Database.Checkpoint();
        return Task.CompletedTask;
    }

    public Task<bool> DeleteAsync(string id)
    {
        var ok = _context.ContestSessions.Delete(id);
        if (ok) _context.Database.Checkpoint();
        return Task.FromResult(ok);
    }
}
