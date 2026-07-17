using System.Buffers.Binary;
using System.Text;

namespace SDRLoggerPlus.Server.Services.Wsjtx;

/// <summary>
/// Builds outbound WSJT-X UDP messages. Currently the Reply (type 4) message,
/// which tells the decoder (WSJT-X/JTDX) to answer a decoded CQ — the "call this
/// station" action. The Reply echoes the original Decode's fields so the decoder
/// can identify which transmission to respond to. Same framing as the reader:
/// magic + schema + type + id, big-endian ints, u32-length utf-8 strings.
/// </summary>
public static class WsjtxMessageWriter
{
    private const uint Magic = 0xadbccbda;
    private const uint Schema = 3;
    private const uint TypeReply = 4;

    public static byte[] BuildReply(
        string clientId,
        uint timeMsSinceMidnight,
        int snr,
        double deltaTimeSeconds,
        uint deltaFrequencyHz,
        string? mode,
        string? message,
        bool lowConfidence,
        byte modifiers = 0)
    {
        var w = new Writer();
        w.U32(Magic);
        w.U32(Schema);
        w.U32(TypeReply);
        w.Utf8(clientId);
        // Reply payload — mirrors the Decode message it answers.
        w.U32(timeMsSinceMidnight);
        w.I32(snr);
        w.F64(deltaTimeSeconds);
        w.U32(deltaFrequencyHz);
        w.Utf8(mode);
        w.Utf8(message);
        w.Bool(lowConfidence);
        w.U8(modifiers); // keyboard modifiers held during the reply (none)
        return w.ToArray();
    }

    private sealed class Writer
    {
        private readonly List<byte> _bytes = new();

        public void U8(byte v) => _bytes.Add(v);
        public void Bool(bool v) => _bytes.Add(v ? (byte)1 : (byte)0);

        public void U32(uint v)
        {
            Span<byte> b = stackalloc byte[4];
            BinaryPrimitives.WriteUInt32BigEndian(b, v);
            _bytes.AddRange(b.ToArray());
        }

        public void I32(int v)
        {
            Span<byte> b = stackalloc byte[4];
            BinaryPrimitives.WriteInt32BigEndian(b, v);
            _bytes.AddRange(b.ToArray());
        }

        public void F64(double v)
        {
            Span<byte> b = stackalloc byte[8];
            BinaryPrimitives.WriteDoubleBigEndian(b, v);
            _bytes.AddRange(b.ToArray());
        }

        public void Utf8(string? s)
        {
            if (s == null) { U32(0xFFFFFFFF); return; }
            var bytes = Encoding.UTF8.GetBytes(s);
            U32((uint)bytes.Length);
            _bytes.AddRange(bytes);
        }

        public byte[] ToArray() => _bytes.ToArray();
    }
}
