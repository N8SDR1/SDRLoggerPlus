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
/// and Cabrillo name</b> are correct so logging and submission work. State QSO
/// party exchanges are verified against the WA7BNM contest calendar
/// (contestcalendar.com) — including the parties that use a serial number
/// (California, Pennsylvania, Virginia), an operator name (Minnesota, Colorado),
/// or location only with no RST (Maryland-DC, North Carolina, Wisconsin,
/// Nebraska). QSO points and multipliers are exact for the well-known contests
/// (CQ/ARRL DX, WPX, NAQP, sprints, Sweepstakes, IARU) and a reasonable
/// approximation for the long tail — especially state QSO parties, whose county
/// multipliers the declarative model approximates by counting distinct received
/// locations. Clone-and-edit for a perfect ruleset; a named scoring strategy can
/// refine any of them later.
/// </summary>
public static class SeedContests
{
    private static readonly List<string> HfBands = new() { "160M", "80M", "40M", "20M", "15M", "10M" };
    private static readonly List<string> Hf6 = new() { "160M", "80M", "40M", "20M", "15M", "10M", "6M" };
    private static readonly List<string> HfNo160 = new() { "80M", "40M", "20M", "15M", "10M" };
    private static readonly List<string> VhfBands = new() { "6M", "2M", "1.25M", "70CM" };

    // Exchange lead field per QSO party: RS(T), a serial number, an operator name,
    // or none (location-only, no signal report). See the per-party table in
    // StateQsoParties().
    private enum QpEx { Rst, Serial, Name, Loc }

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

    // The operator's own county (in-area sent exchange). Key "county" so the
    // Cabrillo exporter emits MyExchange.County.
    private static ContestField County(string label = "Co") => new() { Key = "county", Label = label, Type = ContestFieldType.Text, Width = 4, Required = true, PrefillFrom = "county" };

    // Lead exchange field for a QSO party: RS(T) / serial / name / (none).
    private static ContestField? QpLead(QpEx ex) => ex switch
    {
        QpEx.Serial => Serial(),
        QpEx.Name => Name(),
        QpEx.Rst => Rst(),
        _ => null, // QpEx.Loc — location only, no signal report
    };

    // Exchange = optional lead field + one location field.
    private static ContestField[] QpEx2(QpEx ex, ContestField loc)
    {
        var lead = QpLead(ex);
        return lead == null ? new[] { loc } : new[] { lead, loc };
    }

    // Per-mode QSO points (Phone / CW / digital). Digital maps to both the RTTY and
    // DIGI mode classes; unset modes fall through to the phone value.
    private static PointsRule Pm(int ph, int cw, int? dig = null)
    {
        var by = new Dictionary<string, int> { ["PH"] = ph, ["CW"] = cw };
        if (dig is int d) { by["RTTY"] = d; by["DIGI"] = d; }
        return new PointsRule { Default = ph, ByMode = by };
    }

    // Flat points regardless of mode.
    private static PointsRule Flat(int n) => new() { Default = n };

    // How a QSO party counts a given multiplier.
    private enum MultScope { Once, PerMode, PerBand, PerBandMode }

    private static MultRule[] QpMults(MultScope s, bool withDxcc)
    {
        var pb = s is MultScope.PerBand or MultScope.PerBandMode;
        var pm = s is MultScope.PerMode or MultScope.PerBandMode;
        var list = new List<MultRule> { M(MultSource.State, pb, pm) };
        if (withDxcc) list.Add(M(MultSource.Dxcc, pb, pm));
        return list.ToArray();
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

        // RS(T) + Class (I/C/S) + (state/province/country). Phone 1 / CW & digital 2.
        yield return D("school-club-roundup", "School Club Roundup", "ARRL-SCR", HfBands, new[] { "CW", "SSB", "RTTY" },
            new[] { Rst(), Txt("class", "Cls", 3), StateF("S/P/C") }, new[] { Rst(), Txt("class", "Cls", 3), StateF("S/P/C") },
            Pm(1, 2, 2), new[] { M(MultSource.State), M(MultSource.Dxcc) });

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

        // Five bands (no 160m); serials restart per band. QTC traffic points aren't
        // expressible in the declarative model (base QSO scoring only).
        foreach (var (m, cab) in New("WAE", "CW", "SSB", "RTTY"))
            yield return D($"wae-{m.L}", $"Worked All Europe {m.N}", cab, HfNo160, m.Modes,
                new[] { Rst(), Serial() }, new[] { Rst(), Serial() },
                Pts(1), new[] { M(MultSource.Dxcc, true) }, serial: SerialMode.PerBand);

        foreach (var (m, cab) in New("OCEANIA", "CW", "SSB"))
            yield return D($"oceania-{m.L}", $"Oceania DX {m.N}", cab, HfBands, m.Modes,
                new[] { Rst(), Serial() }, new[] { Rst(), Serial() },
                Pts(1, otherCont: 1), new[] { M(MultSource.WpxPrefix, true) });

        foreach (var (m, cab) in New("JIDX", "CW", "SSB"))
            yield return D($"jidx-{m.L}", $"JIDX {m.N}", cab, HfBands, m.Modes,
                new[] { Rst(), Zone() }, new[] { Rst(), Txt("qth", "Pref/Zn", 5) },
                Pts(1, otherCont: 1), new[] { M(MultSource.Dxcc, true) });

        // Includes 6m and 2m; provinces counted per band and mode.
        yield return D("rac", "RAC Canada", "RAC", new() { "160M", "80M", "40M", "20M", "15M", "10M", "6M", "2M" }, new[] { "CW", "SSB" },
            new[] { Rst(), StateF("Prov/#") }, new[] { Rst(), StateF("Prov/#") },
            Pts(2), new[] { M(MultSource.State, true, true), M(MultSource.Dxcc, true, true) });

        // Own country 2 / same continent 3 / different continent 5. (Contacts with
        // Russian stations score 10 — not expressible by geography alone.)
        yield return D("rdxc", "Russian DX (RDXC)", "RDXC", HfBands, new[] { "CW", "SSB" },
            new[] { Rst(), Txt("qth", "Oblast/#", 5) }, new[] { Rst(), Txt("qth", "Oblast/#", 5) },
            Pts(3, sameCountry: 2, sameCont: 3, otherCont: 5), new[] { M(MultSource.Dxcc, true) });

        // Own country 0 / same continent 1 / different continent 3. Five bands.
        yield return D("ari-dx", "ARI International DX", "ARI-DX", HfNo160, new[] { "CW", "SSB", "RTTY" },
            new[] { Rst(), Txt("qth", "Prov/#", 5) }, new[] { Rst(), Txt("qth", "Prov/#", 5) },
            Pts(1, sameCountry: 0, sameCont: 1, otherCont: 3), new[] { M(MultSource.Dxcc, true) });

        yield return D("africa-dx", "Africa International DX", "AFRICA-DX", HfBands, new[] { "CW", "SSB" },
            new[] { Rst(), Serial() }, new[] { Rst(), Serial() }, Pts(1), new[] { M(MultSource.Dxcc, true) });

        yield return D("eu-dx", "EU DX", "EU-DX", HfBands, new[] { "CW", "SSB" },
            new[] { Rst(), Serial() }, new[] { Rst(), Serial() }, Pts(1), new[] { M(MultSource.Dxcc, true) });

        // Five bands (no 160m). IOTA island references are the true multiplier
        // (asymmetric island/world points); approximated here by DXCC per band.
        yield return D("iota", "RSGB IOTA", "IOTA", HfNo160, new[] { "CW", "SSB" },
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

        yield return D("cwops-cwt", "CWops CWT", "CWOPS-CWT", HfBands, new[] { "CW" },
            new[] { Name(), Txt("nr", "Nr/S", 5) }, new[] { Name(), Txt("nr", "Nr/S", 5) },
            Pts(1), Array.Empty<MultRule>());

        yield return D("cw-open", "CW Open", "CW-OPEN", HfBands, new[] { "CW" },
            new[] { Serial(), Name() }, new[] { Serial(), Name() },
            Pts(1), new[] { M(MultSource.WpxPrefix) }, serial: SerialMode.AllBand);

        yield return D("k1usn-sst", "K1USN SST", "K1USN-SST", HfBands, new[] { "CW" },
            new[] { Name(), StateF("S/P/DX") }, new[] { Name(), StateF("S/P/DX") },
            Pts(1), new[] { M(MultSource.State, true) }, dupe: DupeRule.PerBand);

        yield return D("icwc-mst", "ICWC Medium Speed Test", "ICWC-MST", HfBands, new[] { "CW" },
            new[] { Name(), Serial() }, new[] { Name(), Serial() },
            Pts(1), Array.Empty<MultRule>(), serial: SerialMode.AllBand);

        yield return D("fists-sprint", "FISTS Sprint", "FISTS", HfNo160, new[] { "CW" },
            new[] { Rst(), StateF(), Name(), Txt("nr", "Nr/Pwr", 5) }, new[] { Rst(), StateF(), Name(), Txt("nr", "Nr/Pwr", 5) },
            Pts(1), new[] { M(MultSource.State) });

        // Non-member same continent 2 / different continent 4 (members score 5).
        yield return D("qrp-arci", "QRP ARCI", "QRP-ARCI", HfBands, new[] { "CW" },
            new[] { Rst(), StateF("S/P/C"), Txt("nr", "Nr/Pwr", 5) }, new[] { Rst(), StateF("S/P/C"), Txt("nr", "Nr/Pwr", 5) },
            Pts(2, sameCont: 2, otherCont: 4), new[] { M(MultSource.State) });

        // Multipliers counted once (all-band), not per band.
        yield return D("ft-roundup", "FT Roundup", "FT-ROUNDUP", HfNo160, new[] { "FT8", "FT4" },
            new[] { Rst(), StateF("S/P/#") }, new[] { Rst(), StateF("S/P/#") },
            Pts(1), new[] { M(MultSource.State), M(MultSource.Dxcc) }, dupe: DupeRule.PerBand);

        // 10m only, worked once per event; no location multiplier (score = QSO points).
        yield return D("ten-ten", "10-10 QSO Party", "TEN-TEN", new() { "10M" }, new[] { "CW", "SSB" },
            new[] { Name(), Txt("nr", "10-10#", 6, false), StateF("S/P/C") }, new[] { Name(), Txt("nr", "10-10#", 6, false), StateF("S/P/C") },
            Pts(1), Array.Empty<MultRule>(), dupe: DupeRule.PerContest);
    }

    // ---- state / regional QSO parties -------------------------------------
    // Each party is role-split: the in-area operator sends its county and works
    // everyone; the out-of-area operator sends its state/province and works only
    // in-area stations (WorksForPoints = InAreaOnly). Bands, modes, per-mode
    // points, exchange style, and multiplier scope are per-party, verified against
    // each sponsor's official rules (docs/design/contest-research/batch-5..7).
    //
    // Approximations kept simple by the declarative model: the received location
    // is a single State-typed field (distinct values ≈ county / S/P multipliers),
    // and power / station-category multipliers and working-bonus points (not
    // expressible here) are omitted. Clone-and-edit for a bonus-exact ruleset.
    private sealed record Qp(
        string Name, string[] Codes, List<string> Bands, string[] Modes,
        QpEx Ex, PointsRule Points, MultScope Scope = MultScope.Once,
        string? Cab = null, string? Id = null);

    private static List<string> Bnd(params string[] b) => b.ToList();

    private static IEnumerable<ContestDefinition> StateQsoParties()
    {
        // Common band sets (WARC always excluded).
        var b160_10 = Bnd("160M", "80M", "40M", "20M", "15M", "10M");
        var b80_10 = Bnd("80M", "40M", "20M", "15M", "10M");           // no 160, no 6
        var b160_6 = Bnd("160M", "80M", "40M", "20M", "15M", "10M", "6M");
        var b160_2 = Bnd("160M", "80M", "40M", "20M", "15M", "10M", "6M", "2M");
        var b160_uhf = Bnd("160M", "80M", "40M", "20M", "15M", "10M", "6M", "2M", "1.25M", "70CM");
        var cwSsb = new[] { "CW", "SSB" };
        var cwSsbDig = new[] { "CW", "SSB", "RTTY" };

        var table = new List<Qp>
        {
            new("Alabama", new[]{"AL"}, b80_10, cwSsb, QpEx.Rst, Flat(2), MultScope.PerMode),
            new("Arkansas", new[]{"AR"}, b160_2, cwSsbDig, QpEx.Rst, Flat(1)),
            new("California", new[]{"CA"}, b160_10, cwSsb, QpEx.Serial, Pm(2, 3)),
            new("Colorado", new[]{"CO"}, b160_2, cwSsbDig, QpEx.Name, Pm(1, 2, 2), MultScope.PerMode),
            new("Delaware", new[]{"DE"}, b160_6, cwSsbDig, QpEx.Rst, Pm(1, 2, 2)),
            new("Florida", new[]{"FL"}, Bnd("40M", "20M", "15M", "10M"), cwSsb, QpEx.Rst, Pm(1, 2)),
            new("Georgia", new[]{"GA"}, b160_6, cwSsb, QpEx.Rst, Pm(1, 2), MultScope.PerMode),
            new("Hawaii", new[]{"HI"}, b160_10, new[]{"CW", "SSB", "RTTY", "FT8", "FT4"}, QpEx.Rst, Pm(2, 3, 3)),
            new("Illinois", new[]{"IL"}, b160_2, cwSsbDig, QpEx.Rst, Pm(1, 2, 2)),
            new("Indiana", new[]{"IN"}, b160_10, cwSsb, QpEx.Rst, Pm(2, 3)),
            new("Iowa", new[]{"IA"}, b160_uhf, cwSsbDig, QpEx.Rst, Pm(1, 2, 2)),
            new("Kansas", new[]{"KS"}, Bnd("80M", "40M", "20M", "15M", "10M", "6M"), cwSsbDig, QpEx.Rst, Pm(2, 3, 3)),
            new("Kentucky", new[]{"KY"}, Bnd("80M", "40M", "20M", "15M", "10M", "6M", "2M"), cwSsb, QpEx.Rst, Pm(1, 2)),
            new("Louisiana", new[]{"LA"}, b160_2, cwSsbDig, QpEx.Rst, Pm(2, 4, 4), MultScope.PerBandMode),
            new("Maryland-DC", new[]{"MD", "DC"}, b160_10, cwSsb, QpEx.Loc, Pm(1, 3), MultScope.Once, "MDC-QSO-PARTY"),
            new("Michigan", new[]{"MI"}, b80_10, cwSsb, QpEx.Rst, Pm(1, 2), MultScope.PerMode),
            new("Minnesota", new[]{"MN"}, b160_10, cwSsb, QpEx.Name, Pm(2, 3), MultScope.PerMode),
            new("Mississippi", new[]{"MS"}, b160_2, new[]{"CW", "SSB", "RTTY", "FT8", "FT4"}, QpEx.Rst, Pm(1, 2, 2)),
            new("Missouri", new[]{"MO"}, b160_uhf, cwSsbDig, QpEx.Rst, Pm(1, 2, 2)),
            new("Nebraska", new[]{"NE"}, b160_2, cwSsbDig, QpEx.Loc, Pm(2, 3, 1)),
            new("New Jersey", new[]{"NJ"}, b80_10, cwSsbDig, QpEx.Rst, Pm(1, 2, 2)),
            new("New Mexico", new[]{"NM"}, b160_2, cwSsbDig, QpEx.Rst, Pm(1, 2, 2)),
            new("New York", new[]{"NY"}, b160_2, cwSsbDig, QpEx.Rst, Pm(1, 2, 3)),
            new("North Carolina", new[]{"NC"}, Bnd("80M", "40M", "20M", "15M", "10M", "6M", "2M"), cwSsbDig, QpEx.Loc, Pm(2, 3, 5)),
            new("North Dakota", new[]{"ND"}, b160_2, cwSsbDig, QpEx.Rst, Flat(1)),
            new("Ohio", new[]{"OH"}, b160_10, cwSsb, QpEx.Rst, Pm(1, 2), MultScope.PerMode),
            new("Oklahoma", new[]{"OK"}, Bnd("80M", "40M", "20M", "15M", "10M", "6M"), cwSsbDig, QpEx.Rst, Pm(2, 3, 3)),
            new("Pennsylvania", new[]{"PA"}, b160_2, cwSsb, QpEx.Serial, Pm(1, 2)),
            new("South Carolina", new[]{"SC"}, b160_2, new[]{"CW", "SSB", "RTTY", "FT8", "FT4"}, QpEx.Rst, Flat(2), MultScope.PerBandMode),
            new("South Dakota", new[]{"SD"}, b160_uhf, cwSsb, QpEx.Rst, Pm(1, 2)),
            new("Tennessee", new[]{"TN"}, b160_6, cwSsbDig, QpEx.Rst, Flat(3), MultScope.PerBand),
            new("Texas", new[]{"TX"}, b160_2, cwSsbDig, QpEx.Rst, Pm(2, 3, 3)),
            new("Virginia", new[]{"VA"}, b160_uhf, new[]{"CW", "SSB", "RTTY", "FT8"}, QpEx.Serial, Pm(1, 2, 2), MultScope.PerBandMode),
            new("Washington Salmon Run", new[]{"WA"}, b160_6, cwSsb, QpEx.Rst, Pm(2, 3), MultScope.Once, "WA-SALMON-RUN"),
            new("West Virginia", new[]{"WV"}, b80_10, cwSsbDig, QpEx.Rst, Pm(1, 2, 2)),
            new("Wisconsin", new[]{"WI"}, b160_2, cwSsbDig, QpEx.Rst, Pm(1, 2, 2)),
            // Regionals (multi-state in-area side).
            new("7th Call Area QSO Party (7QP)", new[]{"WA", "OR", "ID", "MT", "WY", "NV", "UT"},
                b160_10, cwSsbDig, QpEx.Rst, Pm(2, 3, 4), MultScope.Once, "7QP", "qp-7qp"),
            new("New England QSO Party (NEQP)", new[]{"CT", "ME", "MA", "NH", "RI", "VT"},
                b80_10, cwSsbDig, QpEx.Rst, Pm(1, 2, 2), MultScope.Once, "NEQP", "qp-neqp"),
        };

        foreach (var p in table)
            yield return BuildQp(p);
    }

    // Turn a party's data row into a role-aware definition.
    private static ContestDefinition BuildQp(Qp p)
    {
        var id = p.Id ?? $"qp-{Slug(p.Name)}";
        var cab = p.Cab ?? $"{p.Codes[0]}-QSO-PARTY";
        // State rows carry a bare state name ("Ohio"); regional rows carry a full
        // title ("New England QSO Party (NEQP)") — only append for the former.
        var name = p.Name.Contains("QSO Party") ? p.Name : $"{p.Name} QSO Party";
        var serial = p.Ex == QpEx.Serial ? SerialMode.AllBand : SerialMode.None;

        // What the operator captures from the other station: their location plus the
        // lead field (received serial / name / RST). One field regardless of role.
        var rcvd = QpEx2(p.Ex, StateF("S/P/C"));
        // Out-of-area default (also the All-role fallback): send RST + own state.
        var sentOut = QpEx2(p.Ex, StateF("S/P/DX"));
        // In-area: send RST + own county.
        var sentIn = QpEx2(p.Ex, County());

        var def = D(id, name, cab, p.Bands, p.Modes,
            sentOut, rcvd, p.Points, QpMults(p.Scope, withDxcc: false), serial: serial);

        def.HomeArea = new HomeArea { Kind = HomeAreaKind.StateCounty, States = p.Codes.ToList() };
        def.Roles = new Dictionary<ContestRole, RoleRules>
        {
            [ContestRole.InArea] = new RoleRules
            {
                SentExchange = sentIn.ToList(),
                MultiplierRules = QpMults(p.Scope, withDxcc: true).ToList(),
                WorksForPoints = WorkTarget.Everyone,
            },
            [ContestRole.OutArea] = new RoleRules
            {
                SentExchange = sentOut.ToList(),
                MultiplierRules = QpMults(p.Scope, withDxcc: false).ToList(),
                WorksForPoints = WorkTarget.InAreaOnly,
            },
        };
        return def;
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
