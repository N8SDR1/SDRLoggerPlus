using System.Buffers.Binary;
using System.Text;
using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Server.Services;
using SDRLoggerPlus.Server.Services.Wsjtx;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

[Trait("Category", "Unit")]
public class WsjtxServiceTests
{
    private readonly Mock<IQsoService> _qsoService = new();
    private readonly WsjtxService _service;
    private readonly List<CreateQsoRequest> _created = new();

    public WsjtxServiceTests()
    {
        _qsoService.Setup(q => q.CreateAsync(It.IsAny<CreateQsoRequest>()))
            .Callback<CreateQsoRequest>(r => _created.Add(r))
            .ReturnsAsync((CreateQsoRequest r) => new QsoResponse(
                "id1", r.Callsign, r.QsoDate, r.TimeOn, null, r.Band, r.Mode, r.Frequency,
                r.RstSent, r.RstRcvd, null, r.Comment, DateTime.UtcNow));

        var services = new ServiceCollection();
        services.AddScoped(_ => _qsoService.Object);
        _service = new WsjtxService(services.BuildServiceProvider(), NullLogger<WsjtxService>.Instance);
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
