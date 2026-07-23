using System.Buffers.Binary;
using System.Text;
using FluentAssertions;
using SDRLoggerPlus.Server.Services.Wsjtx;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

[Trait("Category", "Unit")]
public class WsjtxMessageReaderTests
{
    private const uint Magic = 0xadbccbda;

    // ─── datagram builder helpers ────────────────────────────────────────

    private sealed class DatagramBuilder
    {
        private readonly List<byte> _bytes = new();

        public DatagramBuilder U8(byte v) { _bytes.Add(v); return this; }

        public DatagramBuilder U32(uint v)
        {
            Span<byte> b = stackalloc byte[4];
            BinaryPrimitives.WriteUInt32BigEndian(b, v);
            _bytes.AddRange(b.ToArray());
            return this;
        }

        public DatagramBuilder U64(ulong v)
        {
            Span<byte> b = stackalloc byte[8];
            BinaryPrimitives.WriteUInt64BigEndian(b, v);
            _bytes.AddRange(b.ToArray());
            return this;
        }

        public DatagramBuilder I32(int v)
        {
            Span<byte> b = stackalloc byte[4];
            BinaryPrimitives.WriteInt32BigEndian(b, v);
            _bytes.AddRange(b.ToArray());
            return this;
        }

        public DatagramBuilder F64(double v)
        {
            Span<byte> b = stackalloc byte[8];
            BinaryPrimitives.WriteDoubleBigEndian(b, v);
            _bytes.AddRange(b.ToArray());
            return this;
        }

        public DatagramBuilder Bool(bool v) => U8(v ? (byte)1 : (byte)0);

        public DatagramBuilder Utf8(string? s)
        {
            if (s == null) return U32(0xFFFFFFFF);
            var bytes = Encoding.UTF8.GetBytes(s);
            U32((uint)bytes.Length);
            _bytes.AddRange(bytes);
            return this;
        }

        /// <summary>QDateTime: julian day (u64) + ms since midnight (u32) + timespec (u8)</summary>
        public DatagramBuilder QDateTime(DateTime utc)
        {
            var julianDay = (ulong)(utc.Date - new DateTime(1970, 1, 1)).TotalDays + 2440588UL;
            var msecs = (uint)utc.TimeOfDay.TotalMilliseconds;
            return U64(julianDay).U32(msecs).U8(1); // timespec 1 = UTC
        }

        public byte[] Build() => _bytes.ToArray();
    }

    private static DatagramBuilder Header(uint messageType, uint schema = 2, string id = "WSJT-X") =>
        new DatagramBuilder().U32(Magic).U32(schema).U32(messageType).Utf8(id);

    // ─── tests ───────────────────────────────────────────────────────────

    [Fact]
    public void Parse_Heartbeat_ReturnsClientInfo()
    {
        var data = Header(0).U32(3).Utf8("2.6.1").Utf8("rev").Build();

        var msg = WsjtxMessageReader.Parse(data);

        var hb = msg.Should().BeOfType<WsjtxHeartbeat>().Subject;
        hb.Id.Should().Be("WSJT-X");
        hb.Version.Should().Be("2.6.1");
    }

    [Fact]
    public void Parse_QsoLogged_MapsAllFields()
    {
        var timeOff = new DateTime(2026, 6, 10, 14, 30, 15, DateTimeKind.Utc);
        var timeOn = new DateTime(2026, 6, 10, 14, 28, 0, DateTimeKind.Utc);
        var data = Header(5)
            .QDateTime(timeOff)
            .Utf8("JA1ABC")          // dx call
            .Utf8("PM95")            // dx grid
            .U64(14_074_000)         // tx freq Hz
            .Utf8("FT8")             // mode
            .Utf8("-10")             // report sent
            .Utf8("-08")             // report received
            .Utf8("50")              // tx power
            .Utf8("tnx 73")          // comments
            .Utf8("Hiro")            // name
            .QDateTime(timeOn)
            .Utf8("")                // operator call
            .Utf8("W8XYZ")           // my call
            .Utf8("EN82")            // my grid
            .Utf8("")                // exchange sent
            .Utf8("")                // exchange received
            .Build();

        var msg = WsjtxMessageReader.Parse(data);

        var qso = msg.Should().BeOfType<WsjtxQsoLogged>().Subject;
        qso.DxCall.Should().Be("JA1ABC");
        qso.DxGrid.Should().Be("PM95");
        qso.TxFrequencyHz.Should().Be(14_074_000);
        qso.Mode.Should().Be("FT8");
        qso.ReportSent.Should().Be("-10");
        qso.ReportReceived.Should().Be("-08");
        qso.Comments.Should().Be("tnx 73");
        qso.Name.Should().Be("Hiro");
        qso.DateTimeOff.Should().Be(timeOff);
        qso.DateTimeOn.Should().Be(timeOn);
        qso.MyCall.Should().Be("W8XYZ");
    }

    [Fact]
    public void Parse_QsoLogged_Schema3_ReadsPropMode()
    {
        var t = new DateTime(2026, 6, 10, 14, 30, 0, DateTimeKind.Utc);
        var data = Header(5, schema: 3)
            .QDateTime(t).Utf8("JA1ABC").Utf8("PM95").U64(14_074_000)
            .Utf8("FT8").Utf8("-10").Utf8("-08").Utf8("50").Utf8("").Utf8("")
            .QDateTime(t).Utf8("").Utf8("W8XYZ").Utf8("EN82").Utf8("").Utf8("")
            .Utf8("ES")              // ADIF propagation mode (schema 3)
            .Build();

        var msg = WsjtxMessageReader.Parse(data);

        msg.Should().BeOfType<WsjtxQsoLogged>().Which.AdifPropagationMode.Should().Be("ES");
    }

    [Fact]
    public void Parse_Decode_MapsAllFields()
    {
        var data = Header(2)
            .Bool(true)              // New
            .U32(52_215_000)         // Time (ms since midnight)
            .I32(-11)                // snr (signed)
            .F64(0.2)                // delta time (8-byte double)
            .U32(1523)               // delta frequency Hz
            .Utf8("FT8")             // mode
            .Utf8("CQ K1ABC FN42")   // message
            .Bool(false)             // low confidence
            .Bool(false)             // off air
            .Build();

        var d = WsjtxMessageReader.Parse(data).Should().BeOfType<WsjtxDecode>().Subject;
        d.New.Should().BeTrue();
        d.TimeMsSinceMidnight.Should().Be(52_215_000);
        d.Snr.Should().Be(-11);
        d.DeltaTimeSeconds.Should().BeApproximately(0.2, 1e-9);
        d.DeltaFrequencyHz.Should().Be(1523);
        d.Mode.Should().Be("FT8");
        d.Message.Should().Be("CQ K1ABC FN42");
        d.LowConfidence.Should().BeFalse();
        d.OffAir.Should().BeFalse();
    }

    [Fact]
    public void Parse_Decode_MissingTrailingBooleans_StillParses()
    {
        // A sender that omits low-confidence/off-air must still yield a decode
        // (Message is the field we actually need).
        var data = Header(2)
            .Bool(false).U32(1000).I32(3).F64(-0.1).U32(800)
            .Utf8("FT4").Utf8("W9XYZ K1ABC -07")
            .Build();

        var d = WsjtxMessageReader.Parse(data).Should().BeOfType<WsjtxDecode>().Subject;
        d.Message.Should().Be("W9XYZ K1ABC -07");
        d.LowConfidence.Should().BeFalse();
        d.OffAir.Should().BeFalse();
    }

    [Fact]
    public void Parse_LoggedAdif_ReturnsAdifText()
    {
        var adif = "<call:6>JA1ABC <band:3>20m <mode:3>FT8 <eor>";
        var data = Header(12).Utf8(adif).Build();

        var msg = WsjtxMessageReader.Parse(data);

        msg.Should().BeOfType<WsjtxLoggedAdif>().Which.Adif.Should().Be(adif);
    }

    [Fact]
    public void Parse_Close_ReturnsClose()
    {
        var msg = WsjtxMessageReader.Parse(Header(6).Build());
        msg.Should().BeOfType<WsjtxClose>().Which.Id.Should().Be("WSJT-X");
    }

    [Fact]
    public void Parse_NullStrings_HandledAsNull()
    {
        var data = Header(0).U32(3).Utf8(null).Utf8(null).Build();
        var hb = WsjtxMessageReader.Parse(data).Should().BeOfType<WsjtxHeartbeat>().Subject;
        hb.Version.Should().BeNull();
    }

    [Theory]
    [InlineData(new byte[0])]
    [InlineData(new byte[] { 0x01, 0x02, 0x03 })]
    public void Parse_TooShort_ReturnsNull(byte[] data)
        => WsjtxMessageReader.Parse(data).Should().BeNull();

    [Fact]
    public void Parse_WrongMagic_ReturnsNull()
    {
        var data = new DatagramBuilder().U32(0xdeadbeef).U32(2).U32(0).Utf8("x").Build();
        WsjtxMessageReader.Parse(data).Should().BeNull();
    }

    [Fact]
    public void Parse_UnknownMessageType_ReturnsNull()
    {
        var data = Header(99).Build();
        WsjtxMessageReader.Parse(data).Should().BeNull();
    }

    [Fact]
    public void Parse_TruncatedQsoLogged_ReturnsNull()
    {
        var full = Header(5).QDateTime(DateTime.UtcNow).Utf8("JA1ABC").Build();
        WsjtxMessageReader.Parse(full).Should().BeNull(); // missing remaining fields
    }

    [Fact]
    public void Parse_JtdxQsoLogged_OmitsTrailingExchangeFields_StillLogs()
    {
        // JTDX (older WSJT-X fork) ends the QSOLogged frame after my_grid — it does NOT
        // append exchange_sent / exchange_received / prop_mode. Must still parse + log.
        var timeOff = new DateTime(2026, 6, 10, 14, 30, 15, DateTimeKind.Utc);
        var timeOn = new DateTime(2026, 6, 10, 14, 28, 0, DateTimeKind.Utc);
        var data = Header(5)
            .QDateTime(timeOff)
            .Utf8("YO8RFS")          // dx call
            .Utf8("KN27")            // dx grid
            .U64(14_074_000)         // tx freq Hz
            .Utf8("FT8")             // mode
            .Utf8("-12")             // report sent
            .Utf8("-15")             // report received
            .Utf8("30")              // tx power
            .Utf8("")                // comments
            .Utf8("Tic")             // name
            .QDateTime(timeOn)
            .Utf8("")                // operator call
            .Utf8("W8XYZ")           // my call
            .Utf8("EN82")            // my grid
            .Build();                // <-- NO exchange_sent / exchange_received / prop_mode

        var qso = WsjtxMessageReader.Parse(data).Should().BeOfType<WsjtxQsoLogged>().Subject;
        qso.DxCall.Should().Be("YO8RFS");
        qso.DxGrid.Should().Be("KN27");
        qso.TxFrequencyHz.Should().Be(14_074_000);
        qso.Mode.Should().Be("FT8");
        qso.MyCall.Should().Be("W8XYZ");
        qso.ExchangeSent.Should().Be("");
        qso.AdifPropagationMode.Should().BeNull();
    }
}
