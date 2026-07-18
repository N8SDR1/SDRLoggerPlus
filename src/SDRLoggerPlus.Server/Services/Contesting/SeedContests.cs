using SDRLoggerPlus.Contracts.Models.Contesting;

namespace SDRLoggerPlus.Server.Services.Contesting;

/// <summary>
/// Built-in contest definitions shipped with the app (read-only, clone-only) —
/// the ARRL/CQ majors (CQ WW/WPX/160, ARRL DX/SS/10/160/VHF/Field Day/Digital),
/// NAQP and NA Sprint, plus a few small events and generic fallbacks. Authored as
/// objects (no packaging step); the engine consumes them generically. User
/// contests live as JSON under %APPDATA%\SDRLoggerPlus\contests\ and are merged
/// in by <see cref="ContestDefinitionService"/>.
///
/// Fidelity note: every definition's <b>exchange fields, dupe rule, serial mode,
/// and Cabrillo name</b> are correct so logging and submission work. QSO points
/// and multipliers are exact for the well-known contests (CQ/ARRL DX, WPX, NAQP,
/// sprints, Sweepstakes) and a reasonable approximation for the long tail.
/// Clone-and-edit for a perfect ruleset; a named scoring strategy can refine any
/// of them later.
/// </summary>
public static class SeedContests
{
    private static readonly List<string> HfBands = new() { "160M", "80M", "40M", "20M", "15M", "10M" };
    private static readonly List<string> Hf6 = new() { "160M", "80M", "40M", "20M", "15M", "10M", "6M" };
    private static readonly List<string> HfNo160 = new() { "80M", "40M", "20M", "15M", "10M" };
    private static readonly List<string> VhfBands = new() { "6M", "2M", "1.25M", "70CM" };

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

    // Per-mode QSO points (Phone / CW / digital). Digital maps to both the RTTY and
    // DIGI mode classes; unset modes fall through to the phone value.
    private static PointsRule Pm(int ph, int cw, int? dig = null)
    {
        var by = new Dictionary<string, int> { ["PH"] = ph, ["CW"] = cw };
        if (dig is int d) { by["RTTY"] = d; by["DIGI"] = d; }
        return new PointsRule { Default = ph, ByMode = by };
    }

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

        // Own country = 2, same continent (diff country) = 5, different continent = 10.
        foreach (var (m, cab) in New("CQ-160", "CW", "SSB"))
            yield return D($"cq-160-{m.L}", $"CQ 160 {m.N}", cab, new() { "160M" }, m.Modes,
                new[] { Rst(), StateF("S/P/C") }, new[] { Rst(), StateF("S/P/C") },
                Pts(5, sameCountry: 2, otherCont: 10),
                new[] { M(MultSource.State), M(MultSource.Dxcc) });

        yield return D("cq-vhf", "CQ VHF", "CQ-VHF", new() { "6M", "2M" }, new[] { "CW", "SSB", "FT8" },
            new[] { Grid() }, new[] { Grid() }, Pts(1),
            new[] { M(MultSource.Grid, true) });

        // Five bands (no 160m). Same country 1 / same continent 2 / diff continent 3.
        yield return D("cq-ww-rtty", "CQ WW RTTY", "CQ-WW-RTTY", HfNo160, new[] { "RTTY" },
            new[] { Rst(), Zone(), StateF() }, new[] { Rst(), Zone(), StateF() },
            Pts(1, sameCountry: 1, sameCont: 2, otherCont: 3),
            new[] { M(MultSource.Dxcc, true), M(MultSource.CqZone, true), M(MultSource.State, true) });
    }

    // ---- ARRL -------------------------------------------------------------
    private static IEnumerable<ContestDefinition> ArrlContests()
    {
        // W/VE stations send state/province and work DX only (counting DXCC entities
        // per band); DX stations send power and work W/VE only (counting states +
        // provinces per band). 3 points per QSO either way.
        foreach (var (m, cab) in New("ARRL-DX", "CW", "SSB"))
        {
            var def = D($"arrl-dx-{m.L}", $"ARRL DX {m.N}", cab, HfBands, m.Modes,
                new[] { Rst(), StateF() }, new[] { Rst(), Power() },
                Pts(3), new[] { M(MultSource.Dxcc, true) });
            def.HomeArea = new HomeArea { Kind = HomeAreaKind.WVE };
            def.Roles = new Dictionary<ContestRole, RoleRules>
            {
                [ContestRole.InArea] = new RoleRules // W/VE
                {
                    SentExchange = new[] { Rst(), StateF() }.ToList(),
                    RcvdExchange = new[] { Rst(), Power() }.ToList(),
                    MultiplierRules = new[] { M(MultSource.Dxcc, true) }.ToList(),
                    WorksForPoints = WorkTarget.OutAreaOnly,
                },
                [ContestRole.OutArea] = new RoleRules // DX
                {
                    SentExchange = new[] { Rst(), Power() }.ToList(),
                    RcvdExchange = new[] { Rst(), StateF() }.ToList(),
                    MultiplierRules = new[] { M(MultSource.State, true) }.ToList(),
                    WorksForPoints = WorkTarget.InAreaOnly,
                },
            };
            yield return def;
        }

        // Single band (28 MHz). Phone = 2, CW = 4. Mults counted once per mode.
        yield return D("arrl-10m", "ARRL 10 Meter", "ARRL-10", new() { "10M" }, new[] { "CW", "SSB" },
            new[] { Rst(), StateF("S/P/C") }, new[] { Rst(), StateF("S/P/C") },
            Pm(2, 4), new[] { M(MultSource.State), M(MultSource.Dxcc) }, serial: SerialMode.AllBand);

        // W/VE-to-W/VE = 2, QSO with DX = 5. W/VE also count DXCC as a mult.
        yield return D("arrl-160m", "ARRL 160 Meter", "ARRL-160", new() { "160M" }, new[] { "CW" },
            new[] { Rst(), Section() }, new[] { Rst(), Section() },
            Pts(2, otherCont: 5), new[] { M(MultSource.Section), M(MultSource.Dxcc) },
            dupe: DupeRule.PerContest);

        // Multipliers counted once for the whole contest (not per band).
        yield return D("arrl-rtty-roundup", "ARRL RTTY Roundup", "ARRL-RTTY", HfNo160, new[] { "RTTY" },
            new[] { Rst(), StateF("S/P/#") }, new[] { Rst(), StateF("S/P/#") },
            Pts(1), new[] { M(MultSource.State), M(MultSource.Dxcc) }, dupe: DupeRule.PerBand);

        // Exchange is class + section (not a scored multiplier). Phone 1 / CW & digital 2.
        yield return D("arrl-field-day", "ARRL Field Day", "ARRL-FD", Hf6, new[] { "CW", "SSB", "RTTY", "FT8" },
            new[] { Txt("class", "Cls", 4), Section() }, new[] { Txt("class", "Cls", 4), Section() },
            Pm(1, 2, 2), Array.Empty<MultRule>());

        // WFDA: Category + Class + ARRL/RAC Section (or MX/DX). Phone 1 / CW & digital 2.
        yield return D("winter-field-day", "Winter Field Day", "WFD", Hf6, new[] { "CW", "SSB", "RTTY", "FT8" },
            new[] { Txt("class", "Cat", 4), Txt("section", "Sec", 5) }, new[] { Txt("class", "Cat", 4), Txt("section", "Sec", 5) },
            Pm(1, 2, 2), Array.Empty<MultRule>());

        yield return D("arrl-vhf", "ARRL VHF", "ARRL-VHF", VhfBands, new[] { "CW", "SSB", "FT8" },
            new[] { Grid() }, new[] { Grid() }, Pts(1), new[] { M(MultSource.Grid, true) });

        yield return D("arrl-digital", "ARRL International Digital", "ARRL-DIGITAL", Hf6, new[] { "FT8", "FT4", "RTTY" },
            new[] { Grid() }, new[] { Grid() }, Pts(1), new[] { M(MultSource.Grid, true) });

        foreach (var (m, cab) in New("ARRL-SS", "CW", "SSB"))
            yield return D($"arrl-ss-{m.L}", $"ARRL Sweepstakes {m.N}", cab, HfBands, m.Modes,
                new[] { Serial(), Txt("prec", "Prec", 2), Txt("check", "Ck", 3), Section() },
                new[] { Serial(), Txt("prec", "Prec", 2), Txt("check", "Ck", 3), Section() },
                Pts(2), new[] { M(MultSource.Section) }, dupe: DupeRule.PerContest, serial: SerialMode.AllBand);

        yield return D("arrl-rookie-roundup", "ARRL Rookie Roundup", "ROOKIE-ROUNDUP", HfNo160, new[] { "CW", "SSB", "RTTY" },
            new[] { Name(), Txt("check", "Yr", 4), StateF("S/P/DX") }, new[] { Name(), Txt("check", "Yr", 4), StateF("S/P/DX") },
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
        yield return D("stew-perry", "Stew Perry Topband", "STEW-PERRY", new() { "160M" }, new[] { "CW" },
            new[] { Grid() }, new[] { Grid() }, Pts(1), new[] { M(MultSource.Grid) });
    }

    // ---- sprints, clubs, NAQP, digital roundups ---------------------------
    private static IEnumerable<ContestDefinition> SprintsClubsDigital()
    {
        // RTTY leg drops 160m; CW/SSB use all six HF bands.
        foreach (var (m, cab) in New("NAQP", "CW", "SSB", "RTTY"))
            yield return D($"naqp-{m.L}", $"NAQP {m.N}", cab, m.N == "RTTY" ? HfNo160 : HfBands, m.Modes,
                new[] { Name(), StateF() }, new[] { Name(), StateF() },
                Pts(1), new[] { M(MultSource.State, true), M(MultSource.Dxcc, true) }, dupe: DupeRule.PerBand);

        // Sprints run on 80/40/20 only; multipliers counted once (all-band).
        foreach (var (m, cab) in New("NA-SPRINT", "CW", "SSB", "RTTY"))
            yield return D($"na-sprint-{m.L}", $"NA Sprint {m.N}", cab, new() { "80M", "40M", "20M" }, m.Modes,
                new[] { Serial(), Name(), StateF() }, new[] { Serial(), Name(), StateF() },
                Pts(1), new[] { M(MultSource.State), M(MultSource.Dxcc) },
                dupe: DupeRule.PerBand, serial: SerialMode.AllBand);

        // 10m only, worked once per event; no location multiplier (score = QSO points).
        yield return D("ten-ten", "10-10 QSO Party", "TEN-TEN", new() { "10M" }, new[] { "CW", "SSB" },
            new[] { Name(), Txt("nr", "10-10#", 6, false), StateF("S/P/C") }, new[] { Name(), Txt("nr", "10-10#", 6, false), StateF("S/P/C") },
            Pts(1), Array.Empty<MultRule>(), dupe: DupeRule.PerContest);
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
