using System.Buffers.Binary;
using System.Text;
using FluentAssertions;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Contracts.Events;
using SDRLoggerPlus.Server.Hubs;
using SDRLoggerPlus.Server.Services;
using SDRLoggerPlus.Server.Services.Wsjtx;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

[Trait("Category", "Unit")]
public class WsjtxServiceTests
{
    private readonly Mock<IQsoService> _qsoService = new();
    private readonly Mock<ISpotStatusService> _spotStatus = new();
    private readonly Mock<ILogHubClient> _hubClient = new();
    private readonly WsjtxService _service;
    private readonly List<CreateQsoRequest> _created = new();
    private readonly List<WsjtxDecodeEvent> _decodes = new();
    private readonly List<SpotSelectedEvent> _spots = new();

    public WsjtxServiceTests()
    {
        _qsoService.Setup(q => q.CreateAsync(It.IsAny<CreateQsoRequest>()))
            .Callback<CreateQsoRequest>(r => _created.Add(r))
            .ReturnsAsync((CreateQsoRequest r) => new QsoResponse(
                "id1", r.Callsign, r.QsoDate, r.TimeOn, null, r.Band, r.Mode, r.Frequency,
                r.RstSent, r.RstRcvd, null, r.Comment, DateTime.UtcNow));

        _hubClient.Setup(c => c.OnWsjtxDecode(It.IsAny<WsjtxDecodeEvent>()))
            .Callback<WsjtxDecodeEvent>(e => _decodes.Add(e))
            .Returns(Task.CompletedTask);
        _hubClient.Setup(c => c.OnSpotSelected(It.IsAny<SpotSelectedEvent>()))
            .Callback<SpotSelectedEvent>(e => _spots.Add(e))
            .Returns(Task.CompletedTask);
        var clients = new Mock<IHubClients<ILogHubClient>>();
        clients.Setup(c => c.All).Returns(_hubClient.Object);
        var hub = new Mock<IHubContext<LogHub, ILogHubClient>>();
        hub.Setup(h => h.Clients).Returns(clients.Object);

        var services = new ServiceCollection();
        services.AddScoped(_ => _qsoService.Object);
        _service = new WsjtxService(
            services.BuildServiceProvider(), NullLogger<WsjtxService>.Instance,
            hub.Object, _spotStatus.Object);
    }

    private static byte[] BuildStatusDatagram(ulong dialFreqHz, string mode = "FT8", string? dxCall = null)
    {
        var bytes = new List<byte>();
        void U32(uint v) { Span<byte> b = stackalloc byte[4]; BinaryPrimitives.WriteUInt32BigEndian(b, v); bytes.AddRange(b.ToArray()); }
        void U64(ulong v) { Span<byte> b = stackalloc byte[8]; BinaryPrimitives.WriteUInt64BigEndian(b, v); bytes.AddRange(b.ToArray()); }
        void Utf8(string s) { var d = Encoding.UTF8.GetBytes(s); U32((uint)d.Length); bytes.AddRange(d); }
        U32(0xadbccbda); U32(2); U32(1); Utf8("WSJT-X"); U64(dialFreqHz); Utf8(mode);
        if (dxCall != null) Utf8(dxCall);
        return bytes.ToArray();
    }

    private static byte[] BuildDecodeDatagram(string message, uint audioOffsetHz, int snr = -10, string mode = "FT8")
    {
        var bytes = new List<byte>();
        void U8(byte v) => bytes.Add(v);
        void U32(uint v) { Span<byte> b = stackalloc byte[4]; BinaryPrimitives.WriteUInt32BigEndian(b, v); bytes.AddRange(b.ToArray()); }
        void I32(int v) { Span<byte> b = stackalloc byte[4]; BinaryPrimitives.WriteInt32BigEndian(b, v); bytes.AddRange(b.ToArray()); }
        void F64(double v) { Span<byte> b = stackalloc byte[8]; BinaryPrimitives.WriteDoubleBigEndian(b, v); bytes.AddRange(b.ToArray()); }
        void Utf8(string s) { var d = Encoding.UTF8.GetBytes(s); U32((uint)d.Length); bytes.AddRange(d); }
        U32(0xadbccbda); U32(2); U32(2); Utf8("WSJT-X");
        U8(1); U32(52_000_000); I32(snr); F64(0.2); U32(audioOffsetHz);
        Utf8(mode); Utf8(message); U8(0); U8(0);
        return bytes.ToArray();
    }

    private static byte[] BuildQsoLoggedDatagram(string dxCall, DateTime timeOff)
    {
        var bytes = new List<byte>();
        void U8(byte v) => bytes.Add(v);
        void U32(uint v) { Span<byte> b = stackalloc byte[4]; BinaryPrimitives.WriteUInt32BigEndian(b, v); bytes.AddRange(b.ToArray()); }
        void U64(ulong v) { Span<byte> b = stackalloc byte[8]; BinaryPrimitives.WriteUInt64BigEndian(b, v); bytes.AddRange(b.ToArray()); }
        void Utf8(string s) { var d = Encoding.UTF8.GetBytes(s); U32((uint)d.Length); bytes.AddRange(d); }
        void QDt(DateTime utc)
        {
            U64((ulong)(utc.Date - new DateTime(1970, 1, 1)).TotalDays + 2440588UL);
            U32((uint)utc.TimeOfDay.TotalMilliseconds);
            U8(1);
        }

        U32(0xadbccbda); U32(2); U32(5); Utf8("WSJT-X");
        QDt(timeOff); Utf8(dxCall); Utf8("PM95"); U64(14_074_000);
        Utf8("FT8"); Utf8("-10"); Utf8("-08"); Utf8("50"); Utf8(""); Utf8("");
        QDt(timeOff.AddMinutes(-2)); Utf8(""); Utf8("W8XYZ"); Utf8("EN82"); Utf8(""); Utf8("");
        return bytes.ToArray();
    }

    [Fact]
    public async Task QsoLoggedDatagram_CreatesQso()
    {
        var datagram = BuildQsoLoggedDatagram("JA1ABC", DateTime.UtcNow);

        await _service.HandleDatagramAsync(datagram);

        _created.Should().ContainSingle();
        _created[0].Callsign.Should().Be("JA1ABC");
        _created[0].Band.Should().Be("20m");
        _service.GetStatus().LastQsoCall.Should().Be("JA1ABC");
    }

    [Fact]
    public async Task DuplicateDatagram_LoggedOnce()
    {
        var datagram = BuildQsoLoggedDatagram("JA1ABC", DateTime.UtcNow);

        await _service.HandleDatagramAsync(datagram);
        await _service.HandleDatagramAsync(datagram);

        _created.Should().ContainSingle();
    }

    [Fact]
    public async Task GarbageDatagram_Ignored()
    {
        await _service.HandleDatagramAsync([0x01, 0x02, 0x03]);
        _created.Should().BeEmpty();
    }

    [Fact]
    public async Task Decode_AfterStatus_BroadcastsEnrichedDecode()
    {
        // Status supplies the dial frequency; the decode carries only the audio
        // offset. RF = 14.074 MHz + 1500 Hz = 14.0755 MHz → 20m.
        await _service.HandleDatagramAsync(BuildStatusDatagram(14_074_000));
        await _service.HandleDatagramAsync(BuildDecodeDatagram("CQ K1ABC FN42", audioOffsetHz: 1500));

        _decodes.Should().ContainSingle();
        var d = _decodes[0];
        d.Callsign.Should().Be("K1ABC");
        d.Grid.Should().Be("FN42");
        d.IsCq.Should().BeTrue();
        d.AudioOffsetHz.Should().Be(1500);
        d.FrequencyHz.Should().Be(14_075_500);
        d.Band.Should().Be("20m");
        _service.GetRecentDecodes().Should().ContainSingle();
    }

    [Fact]
    public async Task Decode_ModeCode_ResolvedFromStatus()
    {
        // On the wire a Decode carries a one-character mode CODE, not a name — "~" is
        // FT8. Status carries the real name, and that is what has to reach the
        // worked-before lookup, or the key ("USA:20m:~") can never match a logged QSO.
        await _service.HandleDatagramAsync(BuildStatusDatagram(14_074_000, mode: "FT8"));
        await _service.HandleDatagramAsync(BuildDecodeDatagram("CQ K1ABC FN42", audioOffsetHz: 1500, mode: "~"));

        _decodes.Should().ContainSingle();
        _decodes[0].Mode.Should().Be("FT8");
        _spotStatus.Verify(s => s.GetSpotStatus("K1ABC", It.IsAny<string?>(), It.IsAny<double>(), "FT8"), Times.Once);
    }

    [Fact]
    public async Task Decode_ModeCode_MappedWhenNoStatusYet()
    {
        // No Status yet, so fall back to mapping the code: "+" is FT4.
        await _service.HandleDatagramAsync(BuildDecodeDatagram("CQ DL1XYZ JO31", audioOffsetHz: 800, mode: "+"));

        _decodes.Should().ContainSingle();
        _decodes[0].Mode.Should().Be("FT4");
    }

    [Fact]
    public async Task Decode_UnknownModeCode_ReportsNoMode()
    {
        // An unrecognised code yields no mode rather than a guess — downstream already
        // treats an unknown mode as "no verdict", which is honest; a wrong one is not.
        await _service.HandleDatagramAsync(BuildDecodeDatagram("CQ VK2DEF QF56", audioOffsetHz: 900, mode: "§"));

        _decodes.Should().ContainSingle();
        _decodes[0].Mode.Should().BeNull();
    }

    [Fact]
    public async Task Decode_WithoutStatus_StillBroadcastsWithoutBand()
    {
        // No Status yet → no dial frequency → decode still surfaces (call/grid),
        // just without a band/frequency.
        await _service.HandleDatagramAsync(BuildDecodeDatagram("CQ DL1XYZ JO31", audioOffsetHz: 800));

        _decodes.Should().ContainSingle();
        _decodes[0].Callsign.Should().Be("DL1XYZ");
        _decodes[0].FrequencyHz.Should().Be(0);
        _decodes[0].Band.Should().BeNull();
    }

    [Fact]
    public async Task Decode_FreeText_NotBroadcast()
    {
        await _service.HandleDatagramAsync(BuildStatusDatagram(14_074_000));
        await _service.HandleDatagramAsync(BuildDecodeDatagram("TNX 73 GL", audioOffsetHz: 1500));

        _decodes.Should().BeEmpty();
    }

    [Fact]
    public async Task StatusWithDxCall_PopulatesLogEntry()
    {
        await _service.HandleDatagramAsync(BuildStatusDatagram(14_074_000, dxCall: "K1ABC"));

        _spots.Should().ContainSingle();
        _spots[0].DxCall.Should().Be("K1ABC");
        _spots[0].Frequency.Should().Be(14_074.0); // kHz
    }

    [Fact]
    public async Task Status_UnchangedDxCall_PopulatesOnce()
    {
        await _service.HandleDatagramAsync(BuildStatusDatagram(14_074_000, dxCall: "K1ABC"));
        await _service.HandleDatagramAsync(BuildStatusDatagram(14_074_000, dxCall: "K1ABC"));

        _spots.Should().ContainSingle(); // Status arrives constantly; only fire on change
    }

    [Fact]
    public async Task Status_EmptyDxCall_DoesNotPopulate()
    {
        await _service.HandleDatagramAsync(BuildStatusDatagram(14_074_000));

        _spots.Should().BeEmpty();
    }

    private static byte[] BuildHeartbeatDatagram(string id = "JTDX", string version = "2.2.160")
    {
        var bytes = new List<byte>();
        void U32(uint v) { Span<byte> b = stackalloc byte[4]; BinaryPrimitives.WriteUInt32BigEndian(b, v); bytes.AddRange(b.ToArray()); }
        void Utf8(string s) { var d = Encoding.UTF8.GetBytes(s); U32((uint)d.Length); bytes.AddRange(d); }
        U32(0xadbccbda); U32(2); U32(0); Utf8(id); U32(3); Utf8(version); Utf8("rev");
        return bytes.ToArray();
    }

    [Fact]
    public async Task Heartbeat_TracksClient()
    {
        await _service.HandleDatagramAsync(BuildHeartbeatDatagram());

        var status = _service.GetStatus();
        status.Clients.Should().ContainSingle(c => c.Id == "JTDX" && c.Version == "2.2.160");
    }

    [Fact]
    public async Task Disable_ClearsClientTable()
    {
        await _service.HandleDatagramAsync(BuildHeartbeatDatagram());
        _service.GetStatus().Clients.Should().NotBeEmpty();

        _service.Reconcile(source: 1, enabled: false);

        // A disabled source must not keep advertising decoders it heard while
        // it was alive — their ever-aging LastHeardUtc would read as a stale
        // link to anything judging heartbeat freshness.
        _service.GetStatus().Clients.Should().BeEmpty();
    }

    [Fact]
    public void Disable_ClearsBindError_EvenWithoutASocket()
    {
        // Port -1 makes the bind throw, leaving Udp null with LastError set —
        // the enabled-but-failing state.
        _service.Reconcile(source: 1, enabled: true, port: -1);
        _service.GetStatus().Error.Should().NotBeNullOrEmpty();

        _service.Reconcile(source: 1, enabled: false);

        _service.GetStatus().Error.Should().BeNull();
    }

    [Fact]
    public async Task ReceivedDatagram_ClearsTransientError()
    {
        _service.Reconcile(source: 1, enabled: true, port: -1);
        _service.GetStatus().Error.Should().NotBeNullOrEmpty();

        // Any datagram proves the socket is receiving, so the recorded error
        // is over. Otherwise one transient receive fault (e.g. WSAECONNRESET
        // from ICMP port-unreachable) would flag the source as failed forever.
        await _service.HandleDatagramAsync(BuildHeartbeatDatagram());

        _service.GetStatus().Error.Should().BeNull();
    }
}
