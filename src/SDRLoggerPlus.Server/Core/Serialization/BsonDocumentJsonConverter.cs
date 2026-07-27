using System.Text.Json;
using System.Text.Json.Serialization;
using MongoDB.Bson;
using MongoDB.Bson.IO;

namespace SDRLoggerPlus.Server.Core.Serialization;

/// <summary>
/// System.Text.Json converter for <see cref="BsonDocument"/> (MongoDB.Bson), used for
/// <c>Qso.AdifExtra</c> — the bag of custom/uncommon ADIF fields (IOTA, SOTA, contest extras…).
///
/// Without this, System.Text.Json serializes a BsonDocument's .NET internals rather than its
/// key/values, so those fields were silently corrupted whenever a Qso crossed the entity API — the
/// multi-op shared-log path (DataSyncController / RemoteApiQsoRepository). Using MongoDB's relaxed
/// extended JSON keeps ordinary string/number ADIF values as plain JSON and round-trips cleanly.
/// </summary>
public sealed class BsonDocumentJsonConverter : JsonConverter<BsonDocument>
{
    private static readonly JsonWriterSettings RelaxedJson =
        new() { OutputMode = JsonOutputMode.RelaxedExtendedJson };

    public override BsonDocument? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType == System.Text.Json.JsonTokenType.Null) return null;
        using var doc = JsonDocument.ParseValue(ref reader);
        var json = doc.RootElement.GetRawText();
        return string.IsNullOrWhiteSpace(json) ? null : BsonDocument.Parse(json);
    }

    public override void Write(Utf8JsonWriter writer, BsonDocument value, JsonSerializerOptions options)
    {
        // BsonExtensionMethods.ToJson → a JSON object; write it raw so it nests as a real object,
        // not a quoted string.
        writer.WriteRawValue(value.ToJson(RelaxedJson));
    }
}
