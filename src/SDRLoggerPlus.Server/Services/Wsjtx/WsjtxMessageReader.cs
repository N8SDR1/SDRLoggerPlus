using System.Buffers.Binary;
using System.Text;

namespace SDRLoggerPlus.Server.Services.Wsjtx;

public abstract record WsjtxMessage(string Id);

public record WsjtxHeartbeat(string Id, uint MaxSchema, string? Version, string? Revision) : WsjtxMessage(Id);

public record WsjtxQsoLogged(
    string Id,
    DateTime DateTimeOff,
    string? DxCall,
    string? DxGrid,
    ulong TxFrequencyHz,
    string? Mode,
    string? ReportSent,
    string? ReportReceived,
    string? TxPower,
    string? Comments,
    string? Name,
    DateTime DateTimeOn,
    string? OperatorCall,
    string? MyCall,
    string? MyGrid,
    string? ExchangeSent,
    string? ExchangeReceived,
    string? AdifPropagationMode) : WsjtxMessage(Id);

/// <summary>
/// Status (type 1) — carries the current dial frequency and the DX call (the
/// station being worked in WSJT-X's DX Call box). We track the dial to
/// reconstruct each decode's RF frequency, and the DX call to populate the log
/// entry / QRZ / map when it changes. Only the leading fields we need are read;
/// the rest of the (long) Status payload is ignored.
/// </summary>
public record WsjtxStatusMessage(string Id, ulong DialFrequencyHz, string? Mode, string? DxCall) : WsjtxMessage(Id);

public record WsjtxClose(string Id) : WsjtxMessage(Id);

public record WsjtxLoggedAdif(string Id, string? Adif) : WsjtxMessage(Id);

/// <summary>
/// A single decode line from WSJT-X/JTDX/MSHV (message type 2) — one decoded
/// transmission per FT8/FT4 cycle. <see cref="Message"/> holds the raw decoded
/// text (e.g. "CQ K1ABC FN42"); parse it with WsjtxDecodeParser for the caller
/// and grid.
/// </summary>
public record WsjtxDecode(
    string Id,
    bool New,
    uint TimeMsSinceMidnight,
    int Snr,
    double DeltaTimeSeconds,
    uint DeltaFrequencyHz,
    string? Mode,
    string? Message,
    bool LowConfidence,
    bool OffAir) : WsjtxMessage(Id);

/// <summary>
/// Parser for the WSJT-X UDP protocol (NetworkMessage.hpp). All integers are
/// big-endian; strings are u32 length + utf-8 bytes with 0xFFFFFFFF = null;
/// QDateTime is julian day (u64) + ms-since-midnight (u32) + timespec (u8,
/// +u32 offset when timespec == 2). Returns null for frames we don't care
/// about or can't parse — the listener just drops them.
/// </summary>
public static class WsjtxMessageReader
{
    private const uint Magic = 0xadbccbda;

    public static WsjtxMessage? Parse(ReadOnlySpan<byte> data)
    {
        try
        {
            var r = new Reader(data);
            if (r.ReadU32() != Magic) return null;
            var schema = r.ReadU32();
            if (schema < 2) return null;
            var type = r.ReadU32();
            var id = r.ReadUtf8() ?? "";

            return type switch
            {
                0 => new WsjtxHeartbeat(id, r.ReadU32(), r.ReadUtf8(), r.ReadUtf8()),
                1 => ReadStatus(ref r, id),
                2 => ReadDecode(ref r, id),
                5 => ReadQsoLogged(ref r, id, schema),
                6 => new WsjtxClose(id),
                12 => new WsjtxLoggedAdif(id, r.ReadUtf8()),
                _ => null,
            };
        }
        catch (EndOfDatagramException)
        {
            return null;
        }
    }

    // Status (type 1), after the common id: Dial (u64) · Mode (utf8) · DX call
    // (utf8) · … (many more fields we ignore). The DX call is read defensively.
    private static WsjtxStatusMessage ReadStatus(ref Reader r, string id)
    {
        var dial = r.ReadU64();
        var mode = r.ReadUtf8();
        var dxCall = r.Remaining >= 4 ? r.ReadUtf8() : null;
        return new WsjtxStatusMessage(id, dial, mode, dxCall);
    }

    // Decode (type 2), after the common id:
    //   New (bool) · Time (u32 ms-since-midnight) · snr (i32) ·
    //   Delta time (f64 — QDataStream defaults to DoublePrecision, so WSJT-X's
    //   `float` delta_time goes on the wire as an 8-byte double) ·
    //   Delta frequency (u32 Hz) · Mode (utf8) · Message (utf8) ·
    //   Low confidence (bool) · Off air (bool).
    // The two trailing booleans are read defensively so a sender that omits
    // them still yields a usable decode (Message is already parsed by then).
    private static WsjtxDecode ReadDecode(ref Reader r, string id)
    {
        var isNew = r.ReadBool();
        var time = r.ReadU32();
        var snr = r.ReadI32();
        var deltaTime = r.ReadDouble();
        var deltaFreq = r.ReadU32();
        var mode = r.ReadUtf8();
        var message = r.ReadUtf8();
        var lowConfidence = r.Remaining >= 1 && r.ReadBool();
        var offAir = r.Remaining >= 1 && r.ReadBool();
        return new WsjtxDecode(id, isNew, time, snr, deltaTime, deltaFreq, mode, message, lowConfidence, offAir);
    }

    private static WsjtxQsoLogged ReadQsoLogged(ref Reader r, string id, uint schema)
    {
        var dateTimeOff = r.ReadQDateTime();
        var dxCall = r.ReadUtf8();
        var dxGrid = r.ReadUtf8();
        var txFreq = r.ReadU64();
        var mode = r.ReadUtf8();
        var reportSent = r.ReadUtf8();
        var reportReceived = r.ReadUtf8();
        var txPower = r.ReadUtf8();
        var comments = r.ReadUtf8();
        var name = r.ReadUtf8();
        var dateTimeOn = r.ReadQDateTime();
        var operatorCall = r.ReadUtf8();
        var myCall = r.ReadUtf8();
        var myGrid = r.ReadUtf8();
        // JTDX (an older WSJT-X fork) ends the QSOLogged frame after my_grid — it does
        // NOT append exchange_sent / exchange_received / prop_mode. Read them defensively
        // (like ReadDecode's trailing booleans) so a truncated JTDX frame still logs the
        // QSO instead of throwing EndOfDatagram → the whole message being dropped.
        var exchangeSent = r.Remaining >= 4 ? r.ReadUtf8() : "";
        var exchangeReceived = r.Remaining >= 4 ? r.ReadUtf8() : "";
        var propMode = schema >= 3 && r.Remaining >= 4 ? r.ReadUtf8() : null;

        return new WsjtxQsoLogged(id, dateTimeOff, dxCall, dxGrid, txFreq, mode,
            reportSent, reportReceived, txPower, comments, name, dateTimeOn,
            operatorCall, myCall, myGrid, exchangeSent, exchangeReceived, propMode);
    }

    private sealed class EndOfDatagramException : Exception;

    private ref struct Reader(ReadOnlySpan<byte> data)
    {
        private ReadOnlySpan<byte> _data = data;

        private ReadOnlySpan<byte> Take(int count)
        {
            if (_data.Length < count) throw new EndOfDatagramException();
            var slice = _data[..count];
            _data = _data[count..];
            return slice;
        }

        public int Remaining => _data.Length;

        public byte ReadU8() => Take(1)[0];
        public bool ReadBool() => Take(1)[0] != 0;
        public int ReadI32() => BinaryPrimitives.ReadInt32BigEndian(Take(4));
        public uint ReadU32() => BinaryPrimitives.ReadUInt32BigEndian(Take(4));
        public ulong ReadU64() => BinaryPrimitives.ReadUInt64BigEndian(Take(8));
        public double ReadDouble() => BinaryPrimitives.ReadDoubleBigEndian(Take(8));

        public string? ReadUtf8()
        {
            var len = ReadU32();
            if (len == 0xFFFFFFFF) return null;
            if (len == 0) return "";
            return Encoding.UTF8.GetString(Take(checked((int)len)));
        }

        public DateTime ReadQDateTime()
        {
            var julianDay = ReadU64();
            var msecs = ReadU32();
            var timespec = ReadU8();
            if (timespec == 2) ReadU32(); // offset from UTC — consume and ignore

            // Julian Day Number 2440588 == 1970-01-01
            var date = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)
                .AddDays(julianDay - 2440588.0);
            return date.AddMilliseconds(msecs);
        }
    }
}
