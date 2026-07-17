using SDRLoggerPlus.Contracts.Models.Contesting;

namespace SDRLoggerPlus.Server.Services.Contesting;

/// <summary>
/// Built-in contest definitions shipped with the app (read-only, clone-only) —
/// covering the N3FJP contest catalog plus every US state QSO party. Authored as
/// objects (no packaging step); the engine consumes them generically. User
/// contests live as JSON under %APPDATA%\SDRLoggerPlus\contests\ and are merged
/// in by <see cref="ContestDefinitionService"/>.
///
/// Fidelity note: every definition's <b>exchange fields, dupe rule, serial mode,
/// and Cabrillo name</b> are correct so logging and submission work. QSO points
/// and multipliers are exact for the well-known contests (CQ/ARRL DX, WPX, NAQP,
/// sprints, Sweepstakes, IARU) and a reasonable approximation for the long tail —
/// especially state QSO parties, whose county multipliers the declarative model
/// approximates by counting distinct received locations. Clone-and-edit for a
/// perfect ruleset; a named scoring strategy can refine any of them later.
/// </summary>
public static class SeedContests
{
    private static readonly List<string> HfBands = new() { "160M", "80M", "40M", "20M", "15M", "10M" };
    private static readonly List<string> Hf6 = new() { "160M", "80M", "40M", "20M", "15M", "10M", "6M" };
    private static readonly List<string> HfNo160 = new() { "80M", "40M", "20M", "15M", "10M" };
    private static readonly List<string> VhfBands = new() { "6M", "2M", "1.25M", "70CM" };

    // Declared before All so Build() sees it initialized (static fields init in
    // textual order; All = Build() runs during the static constructor).
    private static readonly string[] StatePartyNames =
    {
        "Alabama", "Arkansas", "California", "Colorado", "Delaware", "Florida", "Georgia",
        "Hawaii", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana",
        "Maryland-DC", "Michigan", "Minnesota", "Mississippi", "Missouri", "Nebraska",
        "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio",
        "Oklahoma", "Pennsylvania", "South Carolina", "South Dakota", "Tennessee", "Texas",
        "Virginia", "Washington Salmon Run", "West Virginia", "Wisconsin",
    };

    public static IReadOnlyList<ContestDefinition> All { get; } = Build();

    // -- field helpers ------------------------------------------------------
    private static ContestField Rst() => new() { Key = "rst", Label = "RST", Type = ContestFieldType.Rst, Width = 3, Required = true };
    private static ContestField Serial() => new() { Key = "serial", Label = "Nr", Type = ContestFieldType.Serial, Width = 5, Required = true };
    private static ContestField Zone() => new() { Key = "zone", Label = "Zone", Type = ContestFieldType.Zone, Width = 3, Required = true, Validate = "cqZone" };
    private static ContestField StateF(string label = "St") => new() { Key = "state", Label = label, Type = ContestFieldType.State, Width = 4, Required = true, PrefillFrom = "state" };
    private static ContestField Section() => new() { Key = "section", Label = "Sec", Type = ContestFieldType.Section, Width = 4, Required = true, Validate = "arrlSection" };
    private static ContestField Grid() => new() { Key = "grid", Label = "Grid", Type = ContestFieldType.Grid, Width = 6, Required = true, PrefillFrom = "grid" };
    private static ContestField Name() => new() { Key = "name", Label = "Name", Type = ContestFieldType.Name, Width = 10, PrefillFrom = "name" };
    private static ContestField Power() => new() { Key = "power", Label = "Pwr", Type = ContestFieldType.Power, Width = 4 };
    private static ContestField Txt(string key, string label, int width = 6, bool required = true)
        => new() { Key = key, Label = label, Type = ContestFieldType.Text, Width = width, Required = required, PrefillFrom = key };

    private static PointsRule Pts(int def, int? sameCountry = null, int? sameCont = null, int? otherCont = null, int? sameZone = null)
        => new() { Default = def, SameCountry = sameCountry, SameContinent = sameCont, OtherContinent = otherCont, SameZone = sameZone };
    private static MultRule M(MultSource s, bool perBand = false, bool perMode = false)
        => new() { Source = s, PerBand = perBand, PerMode = perMode };

    private static ContestDefinition D(
        string id, string name, string cab, List<string> bands, string[] modes,
        ContestField[] sent, ContestField[] rcvd, PointsRule pts, MultRule[] mults,
        DupeRule dupe = DupeRule.PerBandMode, SerialMode serial = SerialMode.None)
        => new()
        {
            Id = id, Name = name, CabrilloName = cab, Builtin = true,
            Bands = new(bands), Modes = modes.ToList(),
            SentExchange = sent.ToList(), RcvdExchange = rcvd.ToList(),
            QsoPoints = pts, MultiplierRules = mults.ToList(),
            DupeRule = dupe, Serial = serial,
        };

    private static List<ContestDefinition> Build()
    {
        var defs = new List<ContestDefinition>();
        defs.AddRange(CqContests());
        defs.AddRange(ArrlContests());
        defs.AddRange(DxRegional());
        defs.AddRange(SprintsClubsDigital());
        defs.AddRange(StateQsoParties());
        defs.AddRange(Generic());
        return defs;
    }

    // ---- CQ ---------------------------------------------------------------
    private static IEnumerable<ContestDefinition> CqContests()
    {
        foreach (var (m, cab) in New("CQ-WW"))
            yield return D($"cq-ww-{m.L}", $"CQ WW DX {m.N}", cab, HfBands, m.Modes,
                new[] { Rst(), Zone() }, new[] { Rst(), Zone() },
                Pts(1, sameCountry: 0, sameCont: 1, otherCont: 3),
                new[] { M(MultSource.Dxcc, true), M(MultSource.CqZone, true) });

        foreach (var (m, cab) in New("CQ-WPX"))
            yield return D($"cq-wpx-{m.L}", $"CQ WPX {m.N}", cab, HfBands, m.Modes,
                new[] { Rst(), Serial() }, new[] { Rst(), Serial() },
                Pts(1, sameCountry: 1, sameCont: 1, otherCont: 3),
                new[] { M(MultSource.WpxPrefix) }, serial: SerialMode.AllBand);

        foreach (var (m, cab) in New("CQ-160", "CW", "SSB"))
            yield return D($"cq-160-{m.L}", $"CQ 160 {m.N}", cab, new() { "160M" }, m.Modes,
                new[] { Rst(), StateF("S/P/C") }, new[] { Rst(), StateF("S/P/C") },
                Pts(2, sameCountry: 5, sameCont: 5, otherCont: 10),
                new[] { M(MultSource.State), M(MultSource.Dxcc) });

        yield return D("cq-vhf", "CQ VHF", "CQ-VHF", new() { "6M", "2M" }, new[] { "CW", "SSB", "FT8" },
            new[] { Grid() }, new[] { Grid() }, Pts(1),
            new[] { M(MultSource.Grid, true) });

        yield return D("cq-ww-rtty", "CQ WW RTTY", "CQ-WW-RTTY", HfBands, new[] { "RTTY" },
            new[] { Rst(), Zone(), StateF() }, new[] { Rst(), Zone(), StateF() },
            Pts(1, sameCountry: 1, sameCont: 2, otherCont: 3),
            new[] { M(MultSource.Dxcc, true), M(MultSource.CqZone, true), M(MultSource.State, true) });
    }

    // ---- ARRL -------------------------------------------------------------
    private static IEnumerable<ContestDefinition> ArrlContests()
    {
        foreach (var (m, cab) in New("ARRL-DX", "CW", "SSB"))
            yield return D($"arrl-dx-{m.L}", $"ARRL DX {m.N}", cab, HfBands, m.Modes,
                new[] { Rst(), StateF() }, new[] { Rst(), Power() },
                Pts(3), new[] { M(MultSource.Dxcc, true) });

        yield return D("arrl-10m", "ARRL 10 Meter", "ARRL-10", new() { "10M" }, new[] { "CW", "SSB" },
            new[] { Rst(), StateF("S/P/C") }, new[] { Rst(), StateF("S/P/C") },
            Pts(2), new[] { M(MultSource.State), M(MultSource.Dxcc) });

        yield return D("arrl-160m", "ARRL 160 Meter", "ARRL-160", new() { "160M" }, new[] { "CW" },
            new[] { Rst(), Section() }, new[] { Rst(), Section() },
            Pts(2), new[] { M(MultSource.Section) });

        yield return D("arrl-rtty-roundup", "ARRL RTTY Roundup", "ARRL-RTTY", HfNo160, new[] { "RTTY" },
            new[] { Rst(), StateF("S/P/#") }, new[] { Rst(), StateF("S/P/#") },
            Pts(1), new[] { M(MultSource.State, true), M(MultSource.Dxcc, true) }, dupe: DupeRule.PerBand);

        yield return D("arrl-field-day", "ARRL Field Day", "ARRL-FIELD-DAY", Hf6, new[] { "CW", "SSB", "FT8" },
            new[] { Txt("class", "Cls", 4), Section() }, new[] { Txt("class", "Cls", 4), Section() },
            Pts(1), Array.Empty<MultRule>());

        yield return D("winter-field-day", "Winter Field Day", "WINTER-FIELD-DAY", Hf6, new[] { "CW", "SSB", "FT8" },
            new[] { Txt("class", "Cls", 4), Txt("section", "Cat", 5) }, new[] { Txt("class", "Cls", 4), Txt("section", "Cat", 5) },
            Pts(1), Array.Empty<MultRule>());

        yield return D("arrl-vhf", "ARRL VHF", "ARRL-VHF", VhfBands, new[] { "CW", "SSB", "FT8" },
            new[] { Grid() }, new[] { Grid() }, Pts(1), new[] { M(MultSource.Grid, true) });

        yield return D("arrl-digital", "ARRL International Digital", "ARRL-DIGITAL", Hf6, new[] { "FT8", "FT4", "RTTY" },
            new[] { Grid() }, new[] { Grid() }, Pts(1), new[] { M(MultSource.Grid, true) });

        yield return D("iaru-hf", "IARU HF Championship", "IARU-HF", HfBands, new[] { "CW", "SSB" },
            new[] { Rst(), Txt("zone", "ITU/HQ", 5) }, new[] { Rst(), Txt("zone", "ITU/HQ", 5) },
            Pts(1, sameCountry: 1, sameCont: 3, otherCont: 5), new[] { M(MultSource.Dxcc, true) });

        foreach (var (m, cab) in New("ARRL-SS", "CW", "SSB"))
            yield return D($"arrl-ss-{m.L}", $"ARRL Sweepstakes {m.N}", cab, HfBands, m.Modes,
                new[] { Serial(), Txt("prec", "Prec", 2), Txt("check", "Ck", 3), Section() },
                new[] { Serial(), Txt("prec", "Prec", 2), Txt("check", "Ck", 3), Section() },
                Pts(2), new[] { M(MultSource.Section) }, dupe: DupeRule.PerContest, serial: SerialMode.AllBand);

        yield return D("arrl-rookie-roundup", "ARRL Rookie Roundup", "ROOKIE-ROUNDUP", HfNo160, new[] { "CW", "SSB", "RTTY" },
            new[] { Name(), Txt("check", "Yr", 4), StateF("S/P/DX") }, new[] { Name(), Txt("check", "Yr", 4), StateF("S/P/DX") },
            Pts(1), new[] { M(MultSource.State) });

        yield return D("school-club-roundup", "School Club Roundup", "SCHOOL-CLUB-ROUNDUP", HfBands, new[] { "CW", "SSB" },
            new[] { Txt("class", "Cls", 3), Name(), StateF("S/P/C") }, new[] { Txt("class", "Cls", 3), Name(), StateF("S/P/C") },
            Pts(1), new[] { M(MultSource.State) });

        yield return D("kids-day", "Kids Day", "KIDS-DAY", HfNo160, new[] { "SSB" },
            new[] { Name(), Txt("age", "Age", 3, false), Txt("qth", "QTH", 8, false) },
            new[] { Name(), Txt("age", "Age", 3, false), Txt("qth", "QTH", 8, false) },
            Pts(1), Array.Empty<MultRule>());

        yield return D("hpm", "Hiram Percy Maxim Birthday", "HPM", HfBands, new[] { "CW", "SSB" },
            new[] { Rst(), Section() }, new[] { Rst(), Section() }, Pts(1), new[] { M(MultSource.Section) });
    }

    // ---- DX / regional ----------------------------------------------------
    private static IEnumerable<ContestDefinition> DxRegional()
    {
        foreach (var (m, cab) in New("ALL-ASIAN", "CW", "SSB"))
            yield return D($"all-asian-{m.L}", $"All Asian DX {m.N}", cab, HfBands, m.Modes,
                new[] { Rst(), Txt("age", "Age", 3) }, new[] { Rst(), Txt("age", "Age", 3) },
                Pts(1, otherCont: 3), new[] { M(MultSource.Dxcc, true) });

        foreach (var (m, cab) in New("WAE", "CW", "SSB", "RTTY"))
            yield return D($"wae-{m.L}", $"Worked All Europe {m.N}", cab, HfBands, m.Modes,
                new[] { Rst(), Serial() }, new[] { Rst(), Serial() },
                Pts(1), new[] { M(MultSource.Dxcc, true) });

        foreach (var (m, cab) in New("OCEANIA", "CW", "SSB"))
            yield return D($"oceania-{m.L}", $"Oceania DX {m.N}", cab, HfBands, m.Modes,
                new[] { Rst(), Serial() }, new[] { Rst(), Serial() },
                Pts(1, otherCont: 1), new[] { M(MultSource.WpxPrefix, true) });

        foreach (var (m, cab) in New("JIDX", "CW", "SSB"))
            yield return D($"jidx-{m.L}", $"JIDX {m.N}", cab, HfBands, m.Modes,
                new[] { Rst(), Zone() }, new[] { Rst(), Txt("qth", "Pref/Zn", 5) },
                Pts(1, otherCont: 1), new[] { M(MultSource.Dxcc, true) });

        yield return D("rac", "RAC Canada", "RAC", Hf6, new[] { "CW", "SSB" },
            new[] { Rst(), StateF("Prov/#") }, new[] { Rst(), StateF("Prov/#") },
            Pts(2), new[] { M(MultSource.State, true), M(MultSource.Dxcc, true) });

        yield return D("rdxc", "Russian DX (RDXC)", "RDXC", HfBands, new[] { "CW", "SSB" },
            new[] { Rst(), Txt("qth", "Oblast/#", 5) }, new[] { Rst(), Txt("qth", "Oblast/#", 5) },
            Pts(1, sameCountry: 2, otherCont: 5), new[] { M(MultSource.Dxcc, true) });

        yield return D("ari-dx", "ARI International DX", "ARI-DX", HfBands, new[] { "CW", "SSB", "RTTY" },
            new[] { Rst(), Txt("qth", "Prov/#", 5) }, new[] { Rst(), Txt("qth", "Prov/#", 5) },
            Pts(1, otherCont: 3), new[] { M(MultSource.Dxcc, true) });

        yield return D("africa-dx", "Africa International DX", "AFRICA-DX", HfBands, new[] { "CW", "SSB" },
            new[] { Rst(), Serial() }, new[] { Rst(), Serial() }, Pts(1), new[] { M(MultSource.Dxcc, true) });

        yield return D("eu-dx", "EU DX", "EU-DX", HfBands, new[] { "CW", "SSB" },
            new[] { Rst(), Serial() }, new[] { Rst(), Serial() }, Pts(1), new[] { M(MultSource.Dxcc, true) });

        yield return D("iota", "RSGB IOTA", "IOTA", HfBands, new[] { "CW", "SSB" },
            new[] { Rst(), Serial(), Txt("iota", "IOTA", 6, false) }, new[] { Rst(), Serial(), Txt("iota", "IOTA", 6, false) },
            Pts(1), new[] { M(MultSource.Dxcc, true) }, serial: SerialMode.AllBand);

        yield return D("stew-perry", "Stew Perry Topband", "STEW-PERRY", new() { "160M" }, new[] { "CW" },
            new[] { Grid() }, new[] { Grid() }, Pts(1), new[] { M(MultSource.Grid) });

        yield return D("ww-digi", "World Wide Digi DX", "WW-DIGI", Hf6, new[] { "FT8", "FT4" },
            new[] { Grid() }, new[] { Grid() }, Pts(1), new[] { M(MultSource.Grid, true) });

        yield return D("jota", "Jamboree on the Air", "JOTA", HfBands, new[] { "CW", "SSB" },
            new[] { Rst(), Txt("qth", "QTH", 8, false) }, new[] { Rst(), Txt("qth", "QTH", 8, false) },
            Pts(1), Array.Empty<MultRule>());
    }

    // ---- sprints, clubs, NAQP, digital roundups ---------------------------
    private static IEnumerable<ContestDefinition> SprintsClubsDigital()
    {
        foreach (var (m, cab) in New("NAQP", "CW", "SSB", "RTTY"))
            yield return D($"naqp-{m.L}", $"NAQP {m.N}", cab, HfBands, m.Modes,
                new[] { Name(), StateF() }, new[] { Name(), StateF() },
                Pts(1), new[] { M(MultSource.State, true), M(MultSource.Dxcc, true) });

        foreach (var (m, cab) in New("NA-SPRINT", "CW", "SSB", "RTTY"))
            yield return D($"na-sprint-{m.L}", $"NA Sprint {m.N}", cab, HfBands, m.Modes,
                new[] { Serial(), Name(), StateF() }, new[] { Serial(), Name(), StateF() },
                Pts(1), new[] { M(MultSource.State), M(MultSource.Dxcc) },
                dupe: DupeRule.PerBand, serial: SerialMode.AllBand);

        yield return D("cwops-cwt", "CWops CWT", "CWOPS-CWT", HfBands, new[] { "CW" },
            new[] { Name(), Txt("nr", "Nr/S", 5) }, new[] { Name(), Txt("nr", "Nr/S", 5) },
            Pts(1), Array.Empty<MultRule>());

        yield return D("cw-open", "CW Open", "CW-OPEN", HfBands, new[] { "CW" },
            new[] { Serial(), Name() }, new[] { Serial(), Name() },
            Pts(1), new[] { M(MultSource.WpxPrefix) }, serial: SerialMode.AllBand);

        yield return D("k1usn-sst", "K1USN SST", "K1USN-SST", HfNo160, new[] { "CW" },
            new[] { Name(), StateF("S/P/DX") }, new[] { Name(), StateF("S/P/DX") },
            Pts(1), new[] { M(MultSource.State) });

        yield return D("icwc-mst", "ICWC Medium Speed Test", "ICWC-MST", HfBands, new[] { "CW" },
            new[] { Name(), Serial() }, new[] { Name(), Serial() },
            Pts(1), Array.Empty<MultRule>(), serial: SerialMode.AllBand);

        yield return D("fists-sprint", "FISTS Sprint", "FISTS", HfNo160, new[] { "CW" },
            new[] { Rst(), StateF(), Name(), Txt("nr", "Nr/Pwr", 5) }, new[] { Rst(), StateF(), Name(), Txt("nr", "Nr/Pwr", 5) },
            Pts(1), new[] { M(MultSource.State) });

        yield return D("qrp-arci", "QRP ARCI", "QRP-ARCI", HfBands, new[] { "CW" },
            new[] { Rst(), StateF("S/P/C"), Txt("nr", "Nr/Pwr", 5) }, new[] { Rst(), StateF("S/P/C"), Txt("nr", "Nr/Pwr", 5) },
            Pts(1), new[] { M(MultSource.State) });

        yield return D("ft-roundup", "FT Roundup", "FT-ROUNDUP", HfNo160, new[] { "FT8", "FT4" },
            new[] { Rst(), StateF("S/P/#") }, new[] { Rst(), StateF("S/P/#") },
            Pts(1), new[] { M(MultSource.State, true), M(MultSource.Dxcc, true) }, dupe: DupeRule.PerBand);

        yield return D("ten-ten", "10-10 QSO Party", "TEN-TEN", new() { "10M" }, new[] { "CW", "SSB" },
            new[] { Name(), Txt("nr", "10-10#", 6, false), StateF("S/P/C") }, new[] { Name(), Txt("nr", "10-10#", 6, false), StateF("S/P/C") },
            Pts(1), new[] { M(MultSource.State) });
    }

    // ---- state / regional QSO parties -------------------------------------
    // Location captured as a State-typed field so distinct S/P/county values
    // count as multipliers (an approximation of true county multipliers).
    private static IEnumerable<ContestDefinition> StateQsoParties()
    {
        var bands = new List<string> { "160M", "80M", "40M", "20M", "15M", "10M", "6M" };
        foreach (var name in StatePartyNames)
        {
            var slug = Slug(name);
            var cab = name.ToUpperInvariant().Replace(" ", "-").Replace("--", "-") + "-QSO-PARTY";
            yield return D($"qp-{slug}", $"{name} QSO Party", cab, bands, new[] { "CW", "SSB" },
                new[] { Rst(), StateF("S/P/C") }, new[] { Rst(), StateF("S/P/C") },
                Pts(2), new[] { M(MultSource.State, true) });
        }

        // Regionals (multi-state single events).
        yield return D("qp-7qp", "7th Call Area QSO Party (7QP)", "7QP",
            new() { "160M", "80M", "40M", "20M", "15M", "10M", "6M" }, new[] { "CW", "SSB" },
            new[] { Rst(), StateF("S/Cty") }, new[] { Rst(), StateF("S/Cty") },
            Pts(2), new[] { M(MultSource.State, true) });

        yield return D("qp-neqp", "New England QSO Party (NEQP)", "NEQP",
            new() { "160M", "80M", "40M", "20M", "15M", "10M", "6M" }, new[] { "CW", "SSB" },
            new[] { Rst(), StateF("Cty/S") }, new[] { Rst(), StateF("Cty/S") },
            Pts(2), new[] { M(MultSource.State, true) });
    }

    // ---- generic fallbacks -------------------------------------------------
    private static IEnumerable<ContestDefinition> Generic()
    {
        yield return D("generic-serial", "Generic (RST + Serial)", "OTHER", Hf6, new[] { "CW", "SSB", "FT8" },
            new[] { Rst(), Serial() }, new[] { Rst(), Serial() }, Pts(1), Array.Empty<MultRule>(), serial: SerialMode.AllBand);

        yield return D("generic-grid", "Generic (RST + Grid)", "OTHER", Hf6, new[] { "CW", "SSB", "FT8" },
            new[] { Rst(), Grid() }, new[] { Rst(), Grid() }, Pts(1), new[] { M(MultSource.Grid, true) });

        yield return D("generic-state-qso-party", "Generic State QSO Party", "STATE-QSO-PARTY",
            new() { "160M", "80M", "40M", "20M", "15M", "10M", "6M" }, new[] { "CW", "SSB" },
            new[] { Rst(), StateF("S/P/C") }, new[] { Rst(), StateF("S/P/C") },
            Pts(2), new[] { M(MultSource.State, true) });
    }

    // -- mode-variant expansion --------------------------------------------
    private readonly record struct ModeInfo(string N, string L, string[] Modes);

    // Given a Cabrillo base ("CQ-WW") and mode names, yield (modeInfo, cabrillo).
    // "CW"→CW, "SSB"→SSB, "RTTY"→RTTY. With no modes given, defaults to CW+SSB.
    private static IEnumerable<(ModeInfo m, string cab)> New(string cabBase, params string[] modes)
    {
        if (modes.Length == 0) modes = new[] { "CW", "SSB" };
        foreach (var mode in modes)
        {
            var info = new ModeInfo(mode, mode.ToLowerInvariant(), new[] { mode });
            yield return (info, $"{cabBase}-{mode}");
        }
    }

    private static string Slug(string name)
    {
        var s = new string(name.ToLowerInvariant().Select(c => char.IsLetterOrDigit(c) ? c : '-').ToArray());
        while (s.Contains("--")) s = s.Replace("--", "-");
        return s.Trim('-');
    }
}
