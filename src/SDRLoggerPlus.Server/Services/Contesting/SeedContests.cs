using SDRLoggerPlus.Contracts.Models.Contesting;

namespace SDRLoggerPlus.Server.Services.Contesting;

/// <summary>
/// Built-in contest definitions shipped with the app (read-only, clone-only).
/// Authored as objects rather than bundled JSON so there is no packaging/copy
/// step; the engine still consumes them generically. User contests live as JSON
/// files under %APPDATA%\SDRLoggerPlus\contests\ and are merged in by
/// <see cref="ContestDefinitionService"/>.
///
/// Where a contest's real scoring has a nuance the declarative model can't fully
/// express (e.g. CQ WW's "same-continent-NA = 2 pts" exception), the seed uses the
/// closest declarative approximation; a named scoring strategy can refine it later.
/// </summary>
public static class SeedContests
{
    private static readonly List<string> HfBands = new() { "160M", "80M", "40M", "20M", "15M", "10M" };

    public static IReadOnlyList<ContestDefinition> All { get; } = Build();

    // -- field helpers ------------------------------------------------------
    private static ContestField Rst() => new() { Key = "rst", Label = "RST", Type = ContestFieldType.Rst, Width = 3, Required = true };
    private static ContestField Serial() => new() { Key = "serial", Label = "Nr", Type = ContestFieldType.Serial, Width = 5, Required = true };
    private static ContestField Zone() => new() { Key = "zone", Label = "Zone", Type = ContestFieldType.Zone, Width = 3, Required = true, Validate = "cqZone" };
    private static ContestField StateField() => new() { Key = "state", Label = "St", Type = ContestFieldType.State, Width = 3, PrefillFrom = "state" };
    private static ContestField Section() => new() { Key = "section", Label = "Sec", Type = ContestFieldType.Section, Width = 4, Required = true, Validate = "arrlSection" };
    private static ContestField Grid() => new() { Key = "grid", Label = "Grid", Type = ContestFieldType.Grid, Width = 6, Required = true, PrefillFrom = "grid" };
    private static ContestField Name() => new() { Key = "name", Label = "Name", Type = ContestFieldType.Name, Width = 10, PrefillFrom = "name" };
    private static ContestField Power() => new() { Key = "power", Label = "Pwr", Type = ContestFieldType.Power, Width = 4 };
    private static ContestField ClassField() => new() { Key = "class", Label = "Class", Type = ContestFieldType.Text, Width = 4, Required = true };

    private static List<ContestDefinition> Build()
    {
        var defs = new List<ContestDefinition>();

        // CQ WW DX — RST + CQ Zone; mult = DXCC + CQ zone per band.
        foreach (var (mode, cab) in new[] { ("CW", "CQ-WW-CW"), ("SSB", "CQ-WW-SSB") })
        {
            defs.Add(new ContestDefinition
            {
                Id = $"cq-ww-{mode.ToLowerInvariant()}",
                Name = $"CQ WW DX {mode}",
                CabrilloName = cab,
                Builtin = true,
                Bands = new(HfBands),
                Modes = { mode },
                SentExchange = { Rst(), Zone() },
                RcvdExchange = { Rst(), Zone() },
                QsoPoints = new PointsRule { SameCountry = 0, SameContinent = 1, OtherContinent = 3, Default = 1 },
                MultiplierRules =
                {
                    new MultRule { Source = MultSource.Dxcc, PerBand = true },
                    new MultRule { Source = MultSource.CqZone, PerBand = true },
                },
                DupeRule = DupeRule.PerBandMode,
                Serial = SerialMode.None,
            });
        }

        // CQ WPX — RST + serial; mult = WPX prefixes (once, all-band).
        foreach (var (mode, cab) in new[] { ("CW", "CQ-WPX-CW"), ("SSB", "CQ-WPX-SSB") })
        {
            defs.Add(new ContestDefinition
            {
                Id = $"cq-wpx-{mode.ToLowerInvariant()}",
                Name = $"CQ WPX {mode}",
                CabrilloName = cab,
                Builtin = true,
                Bands = new(HfBands),
                Modes = { mode },
                SentExchange = { Rst(), Serial() },
                RcvdExchange = { Rst(), Serial() },
                QsoPoints = new PointsRule { SameCountry = 1, SameContinent = 1, OtherContinent = 3, Default = 1 },
                MultiplierRules = { new MultRule { Source = MultSource.WpxPrefix, PerBand = false } },
                DupeRule = DupeRule.PerBandMode,
                Serial = SerialMode.AllBand,
            });
        }

        // ARRL DX (W/VE working DX side) — send state, receive power; 3 pts; mult = DXCC per band.
        defs.Add(new ContestDefinition
        {
            Id = "arrl-dx-cw",
            Name = "ARRL DX CW",
            CabrilloName = "ARRL-DX-CW",
            Builtin = true,
            Bands = new(HfBands),
            Modes = { "CW" },
            SentExchange = { Rst(), StateField() },
            RcvdExchange = { Rst(), Power() },
            QsoPoints = new PointsRule { Default = 3 },
            MultiplierRules = { new MultRule { Source = MultSource.Dxcc, PerBand = true } },
            DupeRule = DupeRule.PerBandMode,
            Serial = SerialMode.None,
        });

        // NAQP CW — name + state; 1 pt; mult = states/provinces + countries per band.
        defs.Add(new ContestDefinition
        {
            Id = "naqp-cw",
            Name = "NAQP CW",
            CabrilloName = "NAQP-CW",
            Builtin = true,
            Bands = new() { "160M", "80M", "40M", "20M", "15M", "10M" },
            Modes = { "CW" },
            SentExchange = { Name(), StateField() },
            RcvdExchange = { Name(), StateField() },
            QsoPoints = new PointsRule { Default = 1 },
            MultiplierRules =
            {
                new MultRule { Source = MultSource.State, PerBand = true },
                new MultRule { Source = MultSource.Dxcc, PerBand = true },
            },
            DupeRule = DupeRule.PerBandMode,
            Serial = SerialMode.None,
        });

        // ARRL Field Day — class + ARRL section; simple 1 pt default.
        defs.Add(new ContestDefinition
        {
            Id = "arrl-field-day",
            Name = "ARRL Field Day",
            CabrilloName = "ARRL-FIELD-DAY",
            Builtin = true,
            Bands = new(HfBands) { "6M", "2M" },
            Modes = { "CW", "SSB", "FT8" },
            SentExchange = { ClassField(), Section() },
            RcvdExchange = { ClassField(), Section() },
            QsoPoints = new PointsRule { Default = 1 },
            MultiplierRules = new(),
            DupeRule = DupeRule.PerBandMode,
            Serial = SerialMode.None,
        });

        // Generic fallbacks so any unlisted contest is still loggable.
        defs.Add(new ContestDefinition
        {
            Id = "generic-serial",
            Name = "Generic (RST + Serial)",
            CabrilloName = "OTHER",
            Builtin = true,
            Bands = new(HfBands) { "6M", "2M" },
            Modes = { "CW", "SSB", "FT8" },
            SentExchange = { Rst(), Serial() },
            RcvdExchange = { Rst(), Serial() },
            QsoPoints = new PointsRule { Default = 1 },
            MultiplierRules = new(),
            DupeRule = DupeRule.PerBandMode,
            Serial = SerialMode.AllBand,
        });

        defs.Add(new ContestDefinition
        {
            Id = "generic-grid",
            Name = "Generic (RST + Grid)",
            CabrilloName = "OTHER",
            Builtin = true,
            Bands = new(HfBands) { "6M", "2M" },
            Modes = { "CW", "SSB", "FT8" },
            SentExchange = { Rst(), Grid() },
            RcvdExchange = { Rst(), Grid() },
            QsoPoints = new PointsRule { Default = 1 },
            MultiplierRules = { new MultRule { Source = MultSource.Grid, PerBand = true } },
            DupeRule = DupeRule.PerBandMode,
            Serial = SerialMode.None,
        });

        return defs;
    }
}
