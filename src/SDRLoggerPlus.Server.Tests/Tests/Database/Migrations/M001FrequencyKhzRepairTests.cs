using FluentAssertions;
using LiteDB;
using Microsoft.Extensions.Logging.Abstractions;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database.Migrations;
using SDRLoggerPlus.Server.Tests.Fixtures;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Database.Migrations;

[Trait("Category", "Unit")]
public class M001FrequencyKhzRepairPredicateTests
{
    // --- Rows that ARE the bug: MHz sitting in the kHz field -----------------

    [Theory]
    [InlineData("20m", 14.075287)]   // the reported FT8 case
    [InlineData("20m", 14.074)]
    [InlineData("17m", 18.1)]
    [InlineData("6m", 50.125)]       // the 6m case from the bug report
    [InlineData("40m", 7.074)]
    [InlineData("2m", 144.174)]
    public void Repairs_MhzStoredInKhzField(string band, double stored)
    {
        M001FrequencyKhzRepair.NeedsRepair(band, stored).Should().BeTrue();
    }

    // --- Rows that are already correct ---------------------------------------

    [Theory]
    [InlineData("20m", 14075.287)]
    [InlineData("40m", 7074)]
    [InlineData("6m", 50313)]        // the real 6m rows, per the bug report
    [InlineData("160m", 1810)]
    public void LeavesCorrectKhzAlone(string band, double stored)
    {
        M001FrequencyKhzRepair.NeedsRepair(band, stored).Should().BeFalse();
    }

    // --- The cases a value-magnitude rule destroys ---------------------------
    //
    // 630m and 2200m store legitimate kHz values BELOW 1000. A naive
    // "under 1000 means MHz" rule would multiply these by 1000 and silently
    // corrupt every one of them. No LF rows exist in the author's log today
    // (checked), so this is protection for imports and future LF operating
    // rather than a case already in the wild — which is exactly why it needs
    // a test instead of a live check.

    [Theory]
    [InlineData("630m", 474)]        // correct 630m kHz, looks like MHz by magnitude
    [InlineData("630m", 472)]        // band edge
    [InlineData("630m", 479)]        // band edge
    [InlineData("2200m", 137)]       // correct 2200m kHz
    [InlineData("2200m", 135.7)]     // band edge
    [InlineData("2200m", 137.8)]     // band edge
    public void NeverTouchesCorrectSubMegahertzBands(string band, double stored)
    {
        M001FrequencyKhzRepair.NeedsRepair(band, stored).Should().BeFalse();
    }

    [Theory]
    [InlineData("630m", 0.474)]      // genuinely MHz-in-kHz on 630m
    [InlineData("2200m", 0.137)]
    public void StillRepairsSubMegahertzBandsWhenActuallyWrong(string band, double stored)
    {
        M001FrequencyKhzRepair.NeedsRepair(band, stored).Should().BeTrue();
    }

    // --- Rows the predicate must decline to judge ----------------------------

    [Theory]
    [InlineData(null, 14.074)]
    [InlineData("", 14.074)]
    [InlineData("   ", 14.074)]
    [InlineData("Unknown", 14.074)]
    [InlineData("11m", 27.005)]      // not an amateur band in the frozen table
    public void SkipsRowsWithNoUsableBand(string? band, double stored)
    {
        M001FrequencyKhzRepair.NeedsRepair(band, stored).Should().BeFalse();
    }

    [Theory]
    [InlineData("20m", null)]
    [InlineData("20m", 0d)]
    [InlineData("20m", -14.074)]
    public void SkipsMissingOrNonPositiveFrequencies(string band, double? stored)
    {
        M001FrequencyKhzRepair.NeedsRepair(band, stored).Should().BeFalse();
    }

    [Fact]
    public void SkipsRowsThatAreWrongInSomeOtherWay()
    {
        // 20m band, but the frequency is nowhere near 20m in either scale.
        // Scaling would not fix it, so the migration must not guess — these
        // are the rows the outlier report is for, not the repair.
        M001FrequencyKhzRepair.NeedsRepair("20m", 7074).Should().BeFalse();
        M001FrequencyKhzRepair.NeedsRepair("20m", 999999).Should().BeFalse();
    }

    [Theory]
    [InlineData("20M")]
    [InlineData(" 20m ")]
    [InlineData("20m")]
    public void MatchesBandNamesCaseAndWhitespaceInsensitively(string band)
    {
        M001FrequencyKhzRepair.NeedsRepair(band, 14.074).Should().BeTrue();
    }

    // --- Idempotence, the property the whole design rests on -----------------

    [Fact]
    public void RepairedValueNoLongerNeedsRepair()
    {
        const double stored = 14.075287;
        M001FrequencyKhzRepair.NeedsRepair("20m", stored).Should().BeTrue();

        var repaired = Math.Round(stored * 1000.0, 6);
        M001FrequencyKhzRepair.NeedsRepair("20m", repaired).Should().BeFalse();
    }
}

[Trait("Category", "Integration")]
[Collection("LiteDbMapper")]
public class M001FrequencyKhzRepairApplyTests : IDisposable
{
    private readonly string _dir;
    private readonly LiteDatabase _db;
    private readonly ILiteCollection<Qso> _qsos;

    public M001FrequencyKhzRepairApplyTests()
    {
        _dir = Path.Combine(Path.GetTempPath(), $"slp_mig_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_dir);
        _db = new LiteDatabase(Path.Combine(_dir, "test.db"));
        _qsos = _db.GetCollection<Qso>("qsos");
    }

    public void Dispose()
    {
        _db.Dispose();
        try { Directory.Delete(_dir, recursive: true); } catch { }
    }

    private Qso Insert(string callsign, string band, double? frequency,
        SyncStatus sync = SyncStatus.Synced)
    {
        var qso = new Qso
        {
            // Qso.Id has no default — LiteQsoRepository assigns it on create,
            // and these tests write to the collection directly.
            Id = Guid.NewGuid().ToString(),
            Callsign = callsign,
            Band = band,
            Mode = "FT8",
            Frequency = frequency,
            QsoDate = new DateTime(2026, 6, 20, 0, 0, 0, DateTimeKind.Utc),
            TimeOn = "1200",
            QrzSyncStatus = sync,
            UpdatedAt = new DateTime(2026, 6, 20, 12, 0, 0, DateTimeKind.Utc)
        };
        _qsos.Insert(qso);
        return qso;
    }

    private MigrationResult Apply() =>
        new M001FrequencyKhzRepair().Apply(_db, NullLogger.Instance);

    [Fact]
    public void RepairsOnlyTheBrokenRows()
    {
        var broken = Insert("W1AW", "20m", 14.075287);
        var fine = Insert("K2ABC", "20m", 14074);
        var lf = Insert("N9BC", "630m", 474);

        var result = Apply();

        result.Examined.Should().Be(3);
        result.Changed.Should().Be(1);

        _qsos.FindById(broken.Id).Frequency.Should().Be(14075.287);
        _qsos.FindById(fine.Id).Frequency.Should().Be(14074);
        _qsos.FindById(lf.Id).Frequency.Should().Be(474);
    }

    [Fact]
    public void IsIdempotent_SecondPassChangesNothing()
    {
        Insert("W1AW", "20m", 14.075287);
        Insert("K2ABC", "17m", 18.1);

        Apply().Changed.Should().Be(2);

        var second = Apply();
        second.Examined.Should().Be(2);
        second.Changed.Should().Be(0);
    }

    [Fact]
    public void DoesNotFlipSyncStatusOrBumpUpdatedAt()
    {
        // The repair corrects a storage-unit bug, not QSO content. Bumping
        // UpdatedAt or flipping Synced -> Modified would queue a re-upload of
        // every repaired QSO to QRZ, which is exactly the noise the live
        // repair was careful to avoid.
        var qso = Insert("W1AW", "20m", 14.075287);
        // Read the STORED value back rather than reusing the in-memory one:
        // LiteDB returns DateTimes with Kind=Local, so a naive comparison
        // against the UTC value we wrote fails on the round-trip alone and
        // would say nothing about whether the migration touched the field.
        var updatedAtBefore = _qsos.FindById(qso.Id).UpdatedAt;

        Apply().Changed.Should().Be(1);

        var after = _qsos.FindById(qso.Id);
        after.Frequency.Should().Be(14075.287);
        after.QrzSyncStatus.Should().Be(SyncStatus.Synced);
        after.UpdatedAt.Should().Be(updatedAtBefore);
    }

    [Fact]
    public void ReportsPerRowDetailForTheChangeLog()
    {
        Insert("W1AW", "20m", 14.075287);

        var result = Apply();

        result.Details.Should().ContainSingle()
            .Which.Should().Contain("W1AW").And.Contain("20m")
            .And.Contain("14.075287").And.Contain("14075.287");
    }

    [Fact]
    public void CleanDatabaseIsUntouched()
    {
        Insert("W1AW", "20m", 14074);
        Insert("K2ABC", "630m", 474);

        var result = Apply();

        result.Changed.Should().Be(0);
        result.Details.Should().BeEmpty();
    }
}
