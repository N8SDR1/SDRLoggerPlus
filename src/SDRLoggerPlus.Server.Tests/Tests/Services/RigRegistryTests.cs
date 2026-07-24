using FluentAssertions;
using SDRLoggerPlus.Contracts.Events;
using SDRLoggerPlus.Server.Services.Rig;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

[Trait("Category", "Unit")]
public class RigRegistryTests
{
    // Minimal IRigBackend for testing registry routing/precedence in isolation.
    private sealed class FakeBackend : IRigBackend
    {
        private readonly HashSet<string> _connected;
        public FakeBackend(RadioType type, string scheme, params string[] connected)
        {
            Type = type; IdScheme = scheme;
            _connected = new HashSet<string>(connected);
        }
        public RadioType Type { get; }
        public string IdScheme { get; }
        public bool OwnsRadio(string radioId) => radioId.StartsWith(IdScheme, StringComparison.Ordinal);
        public IReadOnlyList<string> ConnectedRadioIds => _connected.ToList();
        public bool IsRadioConnected(string radioId) => _connected.Contains(radioId);
        public Task<bool> ConnectAsync(string radioId, CancellationToken ct = default) { _connected.Add(radioId); return Task.FromResult(true); }
        public Task DisconnectAsync(string radioId, CancellationToken ct = default) { _connected.Remove(radioId); return Task.CompletedTask; }
        public Task<bool> TuneAsync(string radioId, long hz, string? mode, CancellationToken ct = default) => Task.FromResult(true);
        public Task<bool> SetFrequencyAsync(string radioId, long hz, CancellationToken ct = default) => Task.FromResult(true);
        public Task<bool> SetModeAsync(string radioId, string mode, long hz = 0, CancellationToken ct = default) => Task.FromResult(true);
        public IEnumerable<RadioStateChangedEvent> GetRadioStates() => Array.Empty<RadioStateChangedEvent>();
        public IEnumerable<RadioConnectionStateChangedEvent> GetConnectionStates() => Array.Empty<RadioConnectionStateChangedEvent>();
        /// <summary>Radios this backend would report to the UI on request.</summary>
        public List<RadioDiscoveredEvent> Discovered { get; } = new();
        public Task<IEnumerable<RadioDiscoveredEvent>> GetDiscoveredRadiosAsync() => Task.FromResult<IEnumerable<RadioDiscoveredEvent>>(Discovered);
    }

    [Fact]
    public async Task AllDiscoveredRadios_IncludesEveryBackend()
    {
        // The hub hydrates the UI from this. It used to ask TCI and Hamlib by name,
        // which silently omitted FlexRadio — a Flex announces itself on its own
        // schedule, so if that landed before the UI connected there was nothing left to
        // tell the UI it existed and the radio stayed invisible all session. Going
        // through the registry is what stops a backend being forgotten, so prove every
        // backend is represented.
        var tci = new FakeBackend(RadioType.Tci, "tci-");
        var hamlib = new FakeBackend(RadioType.Hamlib, "hamlib-");
        var flrig = new FakeBackend(RadioType.Flrig, "flrig");
        var flex = new FakeBackend(RadioType.Flex, "flex-");
        tci.Discovered.Add(new RadioDiscoveredEvent("tci-1", RadioType.Tci, "Lyra", "127.0.0.1", 40001, null));
        hamlib.Discovered.Add(new RadioDiscoveredEvent("hamlib-3073", RadioType.Hamlib, "IC-7300", "", 0, null));
        flrig.Discovered.Add(new RadioDiscoveredEvent("flrig", RadioType.Flrig, "flrig", "127.0.0.1", 12345, null));
        flex.Discovered.Add(new RadioDiscoveredEvent("flex-1234", RadioType.Flex, "FLEX-8400", "192.168.1.9", 4992, null));

        var registry = new RigRegistry(new IRigBackend[] { tci, hamlib, flrig, flex });

        var all = (await registry.AllDiscoveredRadiosAsync()).ToList();

        all.Select(r => r.Id).Should().BeEquivalentTo(new[] { "tci-1", "hamlib-3073", "flrig", "flex-1234" });
    }

    [Fact]
    public void ActiveTuner_PicksFirstConnectedBackendInPrecedenceOrder()
    {
        var tci = new FakeBackend(RadioType.Tci, "tci-");                 // not connected
        var hamlib = new FakeBackend(RadioType.Hamlib, "hamlib-", "hamlib-3073");
        var flrig = new FakeBackend(RadioType.Flrig, "flrig", "flrig");
        var reg = new RigRegistry(new IRigBackend[] { tci, hamlib, flrig });

        // TCI has precedence but nothing connected → Hamlib wins over flrig.
        var target = reg.ActiveTuner();
        target.Should().NotBeNull();
        target!.Value.Backend.Type.Should().Be(RadioType.Hamlib);
        target.Value.RadioId.Should().Be("hamlib-3073");
    }

    [Fact]
    public void ActiveTuner_PrefersHigherPrecedenceWhenBothConnected()
    {
        var tci = new FakeBackend(RadioType.Tci, "tci-", "tci-192.168.1.5:40001");
        var flrig = new FakeBackend(RadioType.Flrig, "flrig", "flrig");
        var reg = new RigRegistry(new IRigBackend[] { tci, flrig });

        reg.ActiveTuner()!.Value.Backend.Type.Should().Be(RadioType.Tci);
    }

    [Fact]
    public void ActiveTuner_NullWhenNothingConnected()
    {
        var reg = new RigRegistry(new IRigBackend[]
        {
            new FakeBackend(RadioType.Tci, "tci-"),
            new FakeBackend(RadioType.Hamlib, "hamlib-"),
        });
        reg.ActiveTuner().Should().BeNull();
    }

    [Fact]
    public void ResolveOwner_PrefersConnectedThenFallsBackToScheme()
    {
        var hamlib = new FakeBackend(RadioType.Hamlib, "hamlib-", "hamlib-3073");
        var flrig = new FakeBackend(RadioType.Flrig, "flrig");
        var reg = new RigRegistry(new IRigBackend[] { hamlib, flrig });

        // Connected id routes to its live owner.
        reg.ResolveOwner("hamlib-3073")!.Type.Should().Be(RadioType.Hamlib);
        // Not-yet-connected id still routes by scheme.
        reg.ResolveOwner("hamlib-1035")!.Type.Should().Be(RadioType.Hamlib);
        // Unknown id → null.
        reg.ResolveOwner("tci-1.2.3.4:5").Should().BeNull();
        reg.ResolveOwner("").Should().BeNull();
    }

    [Fact]
    public void EmptyRegistry_ReportsNothing()
    {
        var reg = new RigRegistry(Array.Empty<IRigBackend>());
        reg.ActiveTuner().Should().BeNull();
        reg.Backends.Should().BeEmpty();
        reg.AllRadioStates().Should().BeEmpty();
    }
}
