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
        public Task<IEnumerable<RadioDiscoveredEvent>> GetDiscoveredRadiosAsync() => Task.FromResult<IEnumerable<RadioDiscoveredEvent>>(Array.Empty<RadioDiscoveredEvent>());
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
