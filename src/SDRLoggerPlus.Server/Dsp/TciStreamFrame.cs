using System.Buffers.Binary;

namespace SDRLoggerPlus.Server.Dsp;

/// <summary>
/// TCI 2.0 binary stream frame: a 64-byte little-endian header followed by
/// <see cref="Length"/> little-endian float32 values. Used by Thetis for the
/// IQ panadapter stream (and RX/TX audio). Header layout (offset → field):
/// 0 receiver, 4 sampleRate, 8 format (3 = FLOAT32), 12 codec, 16 crc,
/// 20 length (total float count), 24 type, 28 channels, 32–63 reserved.
/// </summary>
public readonly record struct TciStreamFrame(
    uint Receiver,
    uint SampleRate,
    uint Format,
    uint Codec,
    uint Crc,
    uint Length,
    uint Type,
    uint Channels)
{
    public const int HeaderBytes = 64;

    public const uint TypeIq = 0;
    public const uint TypeRxAudio = 1;
    public const uint TypeTxAudio = 2;

    /// <summary>Parse the 64-byte header. Returns null when the buffer is too short.</summary>
    public static TciStreamFrame? Parse(ReadOnlySpan<byte> buffer)
    {
        if (buffer.Length < HeaderBytes) return null;
        return new TciStreamFrame(
            BinaryPrimitives.ReadUInt32LittleEndian(buffer[0..]),
            BinaryPrimitives.ReadUInt32LittleEndian(buffer[4..]),
            BinaryPrimitives.ReadUInt32LittleEndian(buffer[8..]),
            BinaryPrimitives.ReadUInt32LittleEndian(buffer[12..]),
            BinaryPrimitives.ReadUInt32LittleEndian(buffer[16..]),
            BinaryPrimitives.ReadUInt32LittleEndian(buffer[20..]),
            BinaryPrimitives.ReadUInt32LittleEndian(buffer[24..]),
            BinaryPrimitives.ReadUInt32LittleEndian(buffer[28..]));
    }

    /// <summary>
    /// Float32 payload values actually present, clamped to both the declared
    /// <see cref="Length"/> and the bytes available after the header. A truncated
    /// or oversized frame therefore can't make callers read past the buffer.
    /// </summary>
    public int AvailableFloats(int frameByteLength)
    {
        int bytesAfterHeader = Math.Max(0, frameByteLength - HeaderBytes);
        int floatsAvail = bytesAfterHeader / 4;
        return Math.Min((int)Length, floatsAvail);
    }

    /// <summary>
    /// Copy the float32 payload from <paramref name="buffer"/> into
    /// <paramref name="dest"/>; returns the number of floats copied.
    /// </summary>
    public int ReadPayload(ReadOnlySpan<byte> buffer, Span<float> dest)
    {
        int count = Math.Min(AvailableFloats(buffer.Length), dest.Length);
        for (int i = 0; i < count; i++)
            dest[i] = BinaryPrimitives.ReadSingleLittleEndian(buffer[(HeaderBytes + i * 4)..]);
        return count;
    }
}
