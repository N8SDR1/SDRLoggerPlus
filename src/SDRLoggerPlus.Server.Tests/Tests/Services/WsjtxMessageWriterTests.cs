using System.Buffers.Binary;
using System.Text;
using FluentAssertions;
using SDRLoggerPlus.Server.Services.Wsjtx;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

[Trait("Category", "Unit")]
public class WsjtxMessageWriterTests
{
    [Fact]
    public void BuildReply_ProducesWellFormedFrame()
    {
        var bytes = WsjtxMessageWriter.BuildReply(
            clientId: "WSJT-X",
            timeMsSinceMidnight: 52_215_000,
            snr: -11,
            deltaTimeSeconds: 0.2,
            deltaFrequencyHz: 1500,
            mode: "FT8",
            message: "CQ K1ABC FN42",
            lowConfidence: false);

        var r = new Cursor(bytes);
        r.U32().Should().Be(0xADBCCBDA);      // magic
        r.U32().Should().Be(3u);              // schema
        r.U32().Should().Be(4u);              // type = Reply
        r.Utf8().Should().Be("WSJT-X");       // client id
        r.U32().Should().Be(52_215_000u);     // time
        r.I32().Should().Be(-11);             // snr
        r.F64().Should().BeApproximately(0.2, 1e-9); // delta time
        r.U32().Should().Be(1500u);           // delta frequency
        r.Utf8().Should().Be("FT8");          // mode
        r.Utf8().Should().Be("CQ K1ABC FN42");// message
        r.U8().Should().Be(0);                // low confidence
        r.U8().Should().Be(0);                // modifiers
        r.AtEnd.Should().BeTrue();
    }

    private sealed class Cursor(byte[] data)
    {
        private int _pos;
        public bool AtEnd => _pos == data.Length;
        private ReadOnlySpan<byte> Take(int n) { var s = data.AsSpan(_pos, n); _pos += n; return s; }
        public byte U8() => Take(1)[0];
        public uint U32() => BinaryPrimitives.ReadUInt32BigEndian(Take(4));
        public int I32() => BinaryPrimitives.ReadInt32BigEndian(Take(4));
        public double F64() => BinaryPrimitives.ReadDoubleBigEndian(Take(8));
        public string Utf8() { var len = (int)U32(); return Encoding.UTF8.GetString(Take(len)); }
    }
}
