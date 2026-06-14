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

public record WsjtxClose(string Id) : WsjtxMessage(Id);

public record WsjtxLoggedAdif(string Id, string? Adif) : WsjtxMessage(Id);

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
        var exchangeSent = r.ReadUtf8();
        var exchangeReceived = r.ReadUtf8();
        var propMode = schema >= 3 ? r.ReadUtf8() : null;

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

        public byte ReadU8() => Take(1)[0];
        public uint ReadU32() => BinaryPrimitives.ReadUInt32BigEndian(Take(4));
        public ulong ReadU64() => BinaryPrimitives.ReadUInt64BigEndian(Take(8));

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
