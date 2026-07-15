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

    private static byte[] BuildStatusDatagram(ulong dialFreqHz, string mode = "FT8")
    {
        var bytes = new List<byte>();
        void U32(uint v) { Span<byte> b = stackalloc byte[4]; BinaryPrimitives.WriteUInt32BigEndian(b, v); bytes.AddRange(b.ToArray()); }
        void U64(ulong v) { Span<byte> b = stackalloc byte[8]; BinaryPrimitives.WriteUInt64BigEndian(b, v); bytes.AddRange(b.ToArray()); }
        void Utf8(string s) { var d = Encoding.UTF8.GetBytes(s); U32((uint)d.Length); bytes.AddRange(d); }
        U32(0xadbccbda); U32(2); U32(1); Utf8("WSJT-X"); U64(dialFreqHz); Utf8(mode);
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
    public async Task Heartbeat_TracksClient()
    {
        var bytes = new List<byte>();
        void U32(uint v) { Span<byte> b = stackalloc byte[4]; BinaryPrimitives.WriteUInt32BigEndian(b, v); bytes.AddRange(b.ToArray()); }
        void Utf8(string s) { var d = Encoding.UTF8.GetBytes(s); U32((uint)d.Length); bytes.AddRange(d); }
        U32(0xadbccbda); U32(2); U32(0); Utf8("JTDX"); U32(3); Utf8("2.2.160"); Utf8("rev");

        await _service.HandleDatagramAsync(bytes.ToArray());

        var status = _service.GetStatus();
        status.Clients.Should().ContainSingle(c => c.Id == "JTDX" && c.Version == "2.2.160");
    }
}
