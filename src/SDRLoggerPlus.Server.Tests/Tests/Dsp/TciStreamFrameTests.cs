using System.Buffers.Binary;
using FluentAssertions;
using SDRLoggerPlus.Server.Dsp;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Dsp;

/// <summary>
/// Parser for the TCI 2.0 binary stream frame (64-byte little-endian header +
/// float32 payload) used by the IQ panadapter path. A truncated or oversized
/// frame must never read past its buffer.
/// </summary>
[Trait("Category", "Unit")]
public class TciStreamFrameTests
{
    private static byte[] BuildFrame(uint receiver, uint sampleRate, uint type, uint channels, float[] payload)
    {
        var buf = new byte[TciStreamFrame.HeaderBytes + payload.Length * 4];
        var span = buf.AsSpan();
        BinaryPrimitives.WriteUInt32LittleEndian(span[0..], receiver);
        BinaryPrimitives.WriteUInt32LittleEndian(span[4..], sampleRate);
        BinaryPrimitives.WriteUInt32LittleEndian(span[8..], 3);   // format FLOAT32
        BinaryPrimitives.WriteUInt32LittleEndian(span[12..], 0);  // codec
        BinaryPrimitives.WriteUInt32LittleEndian(span[16..], 0);  // crc
        BinaryPrimitives.WriteUInt32LittleEndian(span[20..], (uint)payload.Length); // length
        BinaryPrimitives.WriteUInt32LittleEndian(span[24..], type);
        BinaryPrimitives.WriteUInt32LittleEndian(span[28..], channels);
        for (int i = 0; i < payload.Length; i++)
            BinaryPrimitives.WriteSingleLittleEndian(span[(TciStreamFrame.HeaderBytes + i * 4)..], payload[i]);
        return buf;
    }

    [Fact]
    public void Parse_ValidIqFrame_ReadsHeaderAndPayload()
    {
        var payload = new[] { 1f, -1f, 0.5f, -0.5f };
        var buf = BuildFrame(receiver: 0, sampleRate: 192000, type: TciStreamFrame.TypeIq, channels: 2, payload);

        var frame = TciStreamFrame.Parse(buf);

        frame.Should().NotBeNull();
        frame!.Value.Receiver.Should().Be(0u);
        frame.Value.SampleRate.Should().Be(192000u);
        frame.Value.Type.Should().Be(TciStreamFrame.TypeIq);
        frame.Value.Channels.Should().Be(2u);
        frame.Value.Length.Should().Be(4u);

        var dest = new float[4];
        int n = frame.Value.ReadPayload(buf, dest);
        n.Should().Be(4);
        dest.Should().Equal(1f, -1f, 0.5f, -0.5f);
    }

    [Fact]
    public void Parse_BufferShorterThanHeader_ReturnsNull()
    {
        TciStreamFrame.Parse(new byte[32]).Should().BeNull();
    }

    [Fact]
    public void AvailableFloats_ClampsToBytesActuallyPresent()
    {
        // Header declares 8 floats but the frame only carries 2 (truncated).
        var buf = new byte[TciStreamFrame.HeaderBytes + 2 * 4];
        BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(20), 8u);

        var frame = TciStreamFrame.Parse(buf)!.Value;

        frame.AvailableFloats(buf.Length).Should().Be(2);
    }

    [Fact]
    public void ReadPayload_NeverExceedsDestinationOrAvailableFloats()
    {
        var buf = BuildFrame(0, 192000, TciStreamFrame.TypeIq, 2, new[] { 1f, 2f, 3f, 4f });
        var frame = TciStreamFrame.Parse(buf)!.Value;

        var dest = new float[2];
        int n = frame.ReadPayload(buf, dest);

        n.Should().Be(2);
        dest.Should().Equal(1f, 2f);
    }

    [Fact]
    public void NonIqType_IsDistinguishableFromIq()
    {
        var buf = BuildFrame(0, 48000, TciStreamFrame.TypeRxAudio, 2, new[] { 0.1f, 0.2f });
        var frame = TciStreamFrame.Parse(buf)!.Value;

        frame.Type.Should().Be(TciStreamFrame.TypeRxAudio);
        frame.Type.Should().NotBe(TciStreamFrame.TypeIq);
    }
}
