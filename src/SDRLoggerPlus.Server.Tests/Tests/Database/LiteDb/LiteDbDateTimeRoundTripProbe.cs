using FluentAssertions;
using LiteDB;
using Xunit;
using Xunit.Abstractions;

namespace SDRLoggerPlus.Server.Tests.Tests.Database.LiteDb;

/// <summary>
/// PHASE 0 PROBE — see docs/design/timezone-architecture.md.
///
/// Before we build the "canonical UTC" fix on top of a custom BsonMapper DateTime
/// serializer, we must EMPIRICALLY confirm LiteDB 5.0.21's actual behaviour — the
/// version already surprised the team once (commit 1daa146: UtcDate=true is inert).
///
/// This probe answers three questions with hard assertions:
///   1. Does a DateTime round-trip preserve the INSTANT (bytes), and what Kind comes back
///      out of a DEFAULT mapper? (hypothesis: same instant, Kind=Local)
///   2. Is a custom RegisterType&lt;DateTime&gt; deserializer HONORED for a DateTime field,
///      or does LiteDB special-case DateTime before reaching custom type handlers?
///   3. Does the custom deserializer recover Kind=Utc + the correct instant WITHOUT
///      changing what is stored on disk (so the fix needs no data migration)?
///
/// The whole read-projection plan depends on Q2/Q3 being yes. If this probe fails,
/// we pivot the mechanism (per-field serializer / stored-as-ISO) before Phase 1.
///
/// It runs off-UTC deliberately where it can: the assertions compare instants, not
/// wall-clock, so they are valid in any server time zone (the CI runner is UTC, which
/// is exactly why this class of bug has hidden before).
/// </summary>
[Trait("Category", "Integration")]
public class LiteDbDateTimeRoundTripProbe : IDisposable
{
    private readonly ITestOutputHelper _out;
    private readonly string _dir;

    public LiteDbDateTimeRoundTripProbe(ITestOutputHelper output)
    {
        _out = output;
        _dir = Path.Combine(Path.GetTempPath(), "sdrl-tz-probe-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_dir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
    }

    private sealed class Row
    {
        public int Id { get; set; }
        public DateTime When { get; set; }
    }

    // A UTC instant near midnight — the exact case that breaks dedupe for evening
    // QSOs west of UTC. 02:30Z is the previous local day for any negative offset.
    private static readonly DateTime UtcInstant =
        new(2026, 3, 10, 2, 30, 0, DateTimeKind.Utc);

    /// <summary>Q1: default mapper — instant preserved, Kind observed.</summary>
    [Fact]
    public void Default_mapper_preserves_instant_and_reveals_read_kind()
    {
        var path = Path.Combine(_dir, "default.db");

        using (var db = new LiteDatabase($"Filename={path};Connection=shared", new BsonMapper()))
        {
            db.GetCollection<Row>("rows").Insert(new Row { Id = 1, When = UtcInstant });
        }

        Row read;
        using (var db = new LiteDatabase($"Filename={path};Connection=shared", new BsonMapper()))
        {
            read = db.GetCollection<Row>("rows").FindById(1);
        }

        _out.WriteLine($"stored (UTC):   {UtcInstant:o}");
        _out.WriteLine($"read.When:      {read.When:o}");
        _out.WriteLine($"read.When.Kind: {read.When.Kind}");

        // The INSTANT must survive regardless of Kind labelling — this is what makes the
        // fix a read-projection, not a data migration.
        read.When.ToUniversalTime().Should().Be(UtcInstant,
            "LiteDB stores the UTC instant on disk; only the read-side Kind/label differs");

        // Document the observed read Kind. Hypothesis: Local (the whole root cause).
        // If this ever comes back Utc, the platform behaviour changed — investigate.
        read.When.Kind.Should().Be(DateTimeKind.Local,
            "LiteDB 5.0.21 converts to local on read (UtcDate flag is inert) — the documented quirk");
    }

    /// <summary>Q2/Q3: custom DateTime deserializer honored, recovers UTC, no data change.</summary>
    [Fact]
    public void Custom_datetime_serializer_recovers_utc_without_rewriting_data()
    {
        var path = Path.Combine(_dir, "custom.db");

        // Write with a DEFAULT mapper (as production data was written).
        using (var db = new LiteDatabase($"Filename={path};Connection=shared", new BsonMapper()))
        {
            db.GetCollection<Row>("rows").Insert(new Row { Id = 1, When = UtcInstant });
        }

        // Read with a mapper carrying the proposed canonical-UTC serializer.
        // deserialize uses .ToUniversalTime() (NOT SpecifyKind): whatever Kind LiteDB
        // hands us, converting to UTC yields the correct instant labelled Kind=Utc —
        // robust whether the callback receives a Local or Utc value.
        var utcMapper = new BsonMapper();
        utcMapper.RegisterType<DateTime>(
            serialize: dt => dt.ToUniversalTime(),
            deserialize: bson => bson.AsDateTime.ToUniversalTime());

        Row read;
        using (var db = new LiteDatabase($"Filename={path};Connection=shared", utcMapper))
        {
            read = db.GetCollection<Row>("rows").FindById(1);
        }

        _out.WriteLine($"custom read.When:      {read.When:o}");
        _out.WriteLine($"custom read.When.Kind: {read.When.Kind}");

        // Q2: the callback was honored (otherwise Kind would be Local like the default read).
        read.When.Kind.Should().Be(DateTimeKind.Utc,
            "custom RegisterType<DateTime> deserializer must be honored for the read-projection fix to work");

        // Q3a: correct instant.
        read.When.Should().Be(UtcInstant);

        // Q3b: the value equals the raw stored bytes read by the DEFAULT mapper, converted
        // to UTC — i.e. the on-disk data was NOT changed; only the projection differs.
        DateTime defaultRead;
        using (var db = new LiteDatabase($"Filename={path};Connection=shared", new BsonMapper()))
        {
            defaultRead = db.GetCollection<Row>("rows").FindById(1).When;
        }
        read.When.Should().Be(defaultRead.ToUniversalTime(),
            "read-projection change must not alter the stored instant");
    }

    /// <summary>
    /// The concrete payoff: the dedupe key's date component agrees on both sides only
    /// under the UTC projection. Mirrors AdifService.GetQsoKey's `qsoDate.Date:yyyyMMdd`.
    /// </summary>
    [Fact]
    public void Dedupe_date_component_agrees_only_under_utc_projection()
    {
        var path = Path.Combine(_dir, "dedupe.db");
        using (var db = new LiteDatabase($"Filename={path};Connection=shared", new BsonMapper()))
        {
            db.GetCollection<Row>("rows").Insert(new Row { Id = 1, When = UtcInstant });
        }

        // "Incoming" side: freshly parsed, Kind=Utc (as ADIF import produces).
        var incomingKey = UtcInstant.Date.ToString("yyyyMMdd");

        // "Existing" side under the UTC-projection mapper.
        var utcMapper = new BsonMapper();
        utcMapper.RegisterType<DateTime>(
            serialize: dt => dt.ToUniversalTime(),
            deserialize: bson => bson.AsDateTime.ToUniversalTime());
        using (var db = new LiteDatabase($"Filename={path};Connection=shared", utcMapper))
        {
            var existingUtc = db.GetCollection<Row>("rows").FindById(1).When;
            existingUtc.Date.ToString("yyyyMMdd").Should().Be(incomingKey,
                "under the UTC projection both sides key on the same calendar day → dupe caught");
        }
    }
}
