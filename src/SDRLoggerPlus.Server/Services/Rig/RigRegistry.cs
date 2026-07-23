using SDRLoggerPlus.Contracts.Events;

namespace SDRLoggerPlus.Server.Services.Rig;

/// <summary>
/// Default <see cref="IRigRegistry"/>. Backends are injected in precedence order
/// (TCI → Hamlib → flrig → FlexRadio); the order of the injected sequence IS the
/// precedence. Stateless beyond that list, so it is safe as a singleton.
/// </summary>
public sealed class RigRegistry : IRigRegistry
{
    private readonly IReadOnlyList<IRigBackend> _backends;

    public RigRegistry(IEnumerable<IRigBackend> backends)
    {
        _backends = backends.ToList();
    }

    public IReadOnlyList<IRigBackend> Backends => _backends;

    public IRigBackend? ResolveOwner(string radioId)
    {
        if (string.IsNullOrEmpty(radioId)) return null;
        // A backend that has actually connected the id wins; fall back to id-scheme
        // ownership so a not-yet-connected radio still routes to the right backend.
        return _backends.FirstOrDefault(b => b.IsRadioConnected(radioId))
            ?? _backends.FirstOrDefault(b => b.OwnsRadio(radioId));
    }

    public RigTarget? ActiveTuner()
    {
        foreach (var backend in _backends)
        {
            var id = backend.ConnectedRadioIds.FirstOrDefault();
            if (id is not null)
                return new RigTarget(backend, id);
        }
        return null;
    }

    public IEnumerable<RadioStateChangedEvent> AllRadioStates()
        => _backends.SelectMany(b => b.GetRadioStates());

    public IEnumerable<RadioConnectionStateChangedEvent> AllConnectionStates()
        => _backends.SelectMany(b => b.GetConnectionStates());

    public async Task<IEnumerable<RadioDiscoveredEvent>> AllDiscoveredRadiosAsync()
    {
        var all = new List<RadioDiscoveredEvent>();
        foreach (var backend in _backends)
            all.AddRange(await backend.GetDiscoveredRadiosAsync());
        return all;
    }
}
