using SDRLoggerPlus.Contracts.Models.Contesting;

namespace SDRLoggerPlus.Server.Services.Contesting;

/// <summary>
/// Built-in contest definitions shipped with the app (read-only, clone-only) —
/// the ARRL/CQ majors (CQ WW/WPX/160, ARRL DX/SS/10/160/VHF/Field Day/Digital),
/// NAQP and NA Sprint, plus a few small events. Every definition models a real,
/// named contest — there are deliberately no generic "RST + serial" placeholders;
/// an unlisted contest is served by cloning the closest match or authoring a user
/// contest. Authored as objects (no packaging step); the engine consumes them
/// generically. User contests live as JSON under
/// %APPDATA%\SDRLoggerPlus\contests\ and are merged in by
/// <see cref="ContestDefinitionService"/>.
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

    // Tag a field so it's only shown/collected for a given worked-station class
    // (per-QSO exchange branching): e.g. in RTTY Roundup a W/VE station sends a
    // state (InArea) while a DX station sends a serial (Dx).
    private static ContestField When(ContestField f, ContestRole role) { f.AppliesTo = role; return f; }

    // Per-mode QSO points (Phone / CW / digital). Digital maps to both the RTTY and
    // DIGI mode classes; unset modes fall through to the phone value.
    private static PointsRule Pm(int ph, int cw, int? dig = null)
    {
        var by = new Dictionary<string, int> { ["PH"] = ph, ["CW"] = cw };
        if (dig is int d) { by["RTTY"] = d; by["DIGI"] = d; }
        return new PointsRule { Default = ph, ByMode = by };
    }

    private static PointsRule Pts(int def, int? sameCountry = null, int? sameCont = null, int? otherCont = null,
        int? sameZone = null, int? sameContNa = null, int? lowBandFactor = null, int? dxPoints = null)
        => new() { Default = def, SameCountry = sameCountry, SameContinent = sameCont, OtherContinent = otherCont,
            SameZone = sameZone, SameContinentNa = sameContNa, LowBandFactor = lowBandFactor, DxPoints = dxPoints };

    // Flat per-band points (VHF+ contests). e.g. PtsBand(("6M",1),("2M",2)).
    private static PointsRule PtsBand(params (string band, int pts)[] rows)
        => new() { ByBand = rows.ToDictionary(r => r.band, r => r.pts, StringComparer.OrdinalIgnoreCase) };
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
        return defs;
    }

    // ---- CQ ---------------------------------------------------------------
    private static IEnumerable<ContestDefinition> CqContests()
    {
        // Different continent 3, same continent 1, same country 0; NA↔NA = 2.
        foreach (var (m, cab) in New("CQ-WW"))
            yield return D($"cq-ww-{m.L}", $"CQ WW DX {m.N}", cab, HfBands, m.Modes,
                new[] { Rst(), Zone() }, new[] { Rst(), Zone() },
                Pts(1, sameCountry: 0, sameCont: 1, otherCont: 3, sameContNa: 2),
                new[] { M(MultSource.Dxcc, true), M(MultSource.CqZone, true) });

        // Band-weighted: 40/80/160 m double. Diff cont 3/6, same cont 1/2,
        // NA↔NA 2/4, same country 1 (flat, all bands).
        foreach (var (m, cab) in New("CQ-WPX"))
            yield return D($"cq-wpx-{m.L}", $"CQ WPX {m.N}", cab, HfBands, m.Modes,
                new[] { Rst(), Serial() }, new[] { Rst(), Serial() },
                Pts(1, sameCountry: 1, sameCont: 1, otherCont: 3, sameContNa: 2, lowBandFactor: 2),
                new[] { M(MultSource.WpxPrefix) }, serial: SerialMode.AllBand);

        // Own country = 2, same continent (diff country) = 5, different continent = 10.
        // Multipliers: US states (48) + DC + Canadian areas + DXCC "DX country" — all
        // for every entrant, matching the S/P/C + DXCC rules here. DX stations actually
        // send a CQ zone (or country/prefix, varying by rule year) that is INFORMATIONAL
        // ONLY — it never counts as a multiplier, so the S/P/C field label is a harmless
        // simplification (the DX multiplier is derived from DXCC, not the typed value).
        // W/VE send RST + State/Province; DX send RST + CQ Zone. Received exchange is
        // role-split so a DX contact captures its zone rather than being asked for a state.
        foreach (var (m, cab) in New("CQ-160", "CW", "SSB"))
        {
            var cq160 = D($"cq-160-{m.L}", $"CQ 160 {m.N}", cab, new() { "160M" }, m.Modes,
                // Sent is role-split too: a W/VE op sends State/Prov, a DX op sends CQ Zone.
                // Without the DX branch a DX operator's sent exchange was a blank state.
                new[] { Rst(), When(StateF("S/P/C"), ContestRole.InArea), When(Zone(), ContestRole.Dx) },
                new[] { Rst(), When(StateF("S/P/C"), ContestRole.InArea), When(Zone(), ContestRole.Dx) },
                Pts(5, sameCountry: 2, otherCont: 10),
                // Mults are US states + VE provinces (State) + DX countries — USA/Canada must NOT
                // also count as DX countries (they're the states/provinces), so DxccExceptHome.
                new[] { M(MultSource.State), M(MultSource.DxccExceptHome) });
            cq160.HomeArea = new HomeArea { Kind = HomeAreaKind.WVE };
            yield return cq160;
        }

        // Per-band points: 6 m = 1, 2 m = 2.
        yield return D("cq-vhf", "CQ VHF", "CQ-VHF", new() { "6M", "2M" }, new[] { "CW", "SSB", "FM", "FT8" },
            new[] { Grid() }, new[] { Grid() }, PtsBand(("6M", 1), ("2M", 2)),
            new[] { M(MultSource.Grid, true) });

        // Five bands (no 160m). Same country 1 / same continent 2 / diff continent 3.
        // Only stateside/Canadian stations send state/province (Rule III); a DX station sends
        // RST+Zone only — so the received state is InArea-conditional, not required for DX.
        var cqRtty = D("cq-ww-rtty", "CQ WW RTTY", "CQ-WW-RTTY", HfNo160, new[] { "RTTY" },
            new[] { Rst(), Zone(), StateF() }, new[] { Rst(), Zone(), When(StateF(), ContestRole.InArea) },
            Pts(1, sameCountry: 1, sameCont: 2, otherCont: 3),
            new[] { M(MultSource.Dxcc, true), M(MultSource.CqZone, true), M(MultSource.State, true) });
        cqRtty.HomeArea = new HomeArea { Kind = HomeAreaKind.WVE };
        yield return cqRtty;
    }

    // ---- ARRL -------------------------------------------------------------
    private static IEnumerable<ContestDefinition> ArrlContests()
    {
        // W/VE stations send state/province and work DX only (counting DXCC entities
        // per band); DX stations send power and work W/VE only (counting states +
        // provinces per band). 3 points per QSO either way.
        foreach (var (m, cab) in New("ARRL-DX", "CW", "SSB"))
        {
            // Top-level exchange carries both role branches via AppliesTo so the entry/setup
            // UI can render the operator's side (W/VE sends State, DX sends Power) and capture
            // the worked station's side. Scoring/Cabrillo still resolve through the Roles dict
            // below — the AppliesTo union here is the display/entry view of the same split.
            var def = D($"arrl-dx-{m.L}", $"ARRL DX {m.N}", cab, HfBands, m.Modes,
                new[] { Rst(), When(StateF(), ContestRole.InArea), When(Power(), ContestRole.Dx) },
                new[] { Rst(), When(Power(), ContestRole.Dx), When(StateF(), ContestRole.InArea) },
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
                [ContestRole.Dx] = new RoleRules // DX
                {
                    SentExchange = new[] { Rst(), Power() }.ToList(),
                    RcvdExchange = new[] { Rst(), StateF() }.ToList(),
                    MultiplierRules = new[] { M(MultSource.State, true) }.ToList(),
                    WorksForPoints = WorkTarget.InAreaOnly,
                },
            };
            yield return def;
        }

        // Single band (28 MHz). Phone = 2, CW = 4. W/VE + Mexican stations send a
        // state; DX stations send a serial (per-QSO branching). Multipliers (states +
        // DC + VE + DXCC) are counted ONCE PER MODE — a state worked on both CW and
        // phone is two mults (PerMode). Simplifications: Mexican-state mults are
        // approximated as states, and the ITU-region mults (maritime/aeronautical
        // mobile only) are not counted — both rare edge cases.
        var tenM = D("arrl-10m", "ARRL 10 Meter", "ARRL-10", new() { "10M" }, new[] { "CW", "SSB" },
            // Sent is role-split like the received side: W/VE/XE send state/prov, DX send a serial.
            new[] { Rst(), When(StateF("S/P/C"), ContestRole.InArea), When(Serial(), ContestRole.Dx) },
            new[] { Rst(), When(StateF("S/P/C"), ContestRole.InArea), When(Serial(), ContestRole.Dx) },
            Pm(2, 4), new[] { M(MultSource.State, perMode: true), M(MultSource.Dxcc, perMode: true) },
            serial: SerialMode.AllBand);
        tenM.HomeArea = new HomeArea { Kind = HomeAreaKind.WVE };
        yield return tenM;

        // W/VE-to-W/VE = 2, QSO with DX = 5. W/VE also count DXCC as a mult. W/VE
        // stations send an ARRL/RAC section; DX stations send a signal report only.
        var oneSixty = D("arrl-160m", "ARRL 160 Meter", "ARRL-160", new() { "160M" }, new[] { "CW" },
            // W/VE send an ARRL/RAC section; DX send RST only (no section) — so the section is
            // InArea-conditional on both sides. A DX operator sends just the report.
            new[] { Rst(), When(Section(), ContestRole.InArea) },
            new[] { Rst(), When(Section(), ContestRole.InArea) },
            Pts(2, dxPoints: 5), new[] { M(MultSource.Section), M(MultSource.Dxcc) },
            dupe: DupeRule.PerContest);
        oneSixty.HomeArea = new HomeArea { Kind = HomeAreaKind.WVE };
        yield return oneSixty;

        // Multipliers (states + provinces + DXCC entities) counted once for the whole
        // contest (not per band). US/VE stations send a state/province; DX stations
        // send a serial (per-QSO branching).
        var rttyRu = D("arrl-rtty-roundup", "ARRL RTTY Roundup", "ARRL-RTTY", HfNo160, new[] { "RTTY" },
            // Sent role-split: US/VE send a state/prov, DX send a serial (one sequence, all bands).
            new[] { Rst(), When(StateF("S/P/#"), ContestRole.InArea), When(Serial(), ContestRole.Dx) },
            new[] { Rst(), When(StateF("St"), ContestRole.InArea), When(Serial(), ContestRole.Dx) },
            Pts(1), new[] { M(MultSource.State), M(MultSource.Dxcc) },
            dupe: DupeRule.PerBand, serial: SerialMode.AllBand);
        rttyRu.HomeArea = new HomeArea { Kind = HomeAreaKind.WVE };
        yield return rttyRu;

        // Exchange is class + section (not a scored multiplier). Phone 1 / CW & digital 2.
        // Final score is scaled by a transmitter power multiplier (§7.1): >150 W = ×1,
        // ≤150 W = ×2, and QRP (≤5 W on non-commercial power) = ×5.
        var fieldDay = D("arrl-field-day", "ARRL Field Day", "ARRL-FD", Hf6, new[] { "CW", "SSB", "RTTY", "FT8" },
            new[] { Txt("class", "Cls", 4), Section() }, new[] { Txt("class", "Cls", 4), Section() },
            Pm(1, 2, 2), Array.Empty<MultRule>());
        fieldDay.PowerMultipliers = new() { ["HIGH"] = 1, ["LOW"] = 2, ["QRP"] = 5 };
        yield return fieldDay;

        // WFDA: Category + Class + ARRL/RAC Section (or MX/DX). Phone 1 / CW & digital 2.
        // Multiplier: one per mode (Phone/CW/Digital) per band. Final score is scaled
        // by a power multiplier — QRP (≤5 W) ×4, Low (≤100 W) ×2, High ×1 (WFD caps at
        // 100 W PEP) — then the operator's declared objective/bonus points are added.
        // WFD restructures its objective bonuses year to year, so those are entered as
        // a self-declared total (see current WFDA rules) rather than auto-derived.
        var wfd = D("winter-field-day", "Winter Field Day", "WFD", Hf6, new[] { "CW", "SSB", "RTTY", "FT8" },
            new[] { Txt("class", "Cat", 4), Txt("section", "Sec", 5) }, new[] { Txt("class", "Cat", 4), Txt("section", "Sec", 5) },
            Pm(1, 2, 2), new[] { M(MultSource.BandMode, perBand: true) });
        wfd.PowerMultipliers = new() { ["HIGH"] = 1, ["LOW"] = 2, ["QRP"] = 4 };
        wfd.BonusPointsHint = "WFD objective bonuses (see current WFDA rules)";
        yield return wfd;

        // Per-band points: 50/144 MHz = 1, 222/432 MHz = 2.
        yield return D("arrl-vhf", "ARRL VHF", "ARRL-VHF", VhfBands, new[] { "CW", "SSB", "FT8" },
            new[] { Grid() }, new[] { Grid() },
            PtsBand(("6M", 1), ("2M", 1), ("1.25M", 2), ("70CM", 2)), new[] { M(MultSource.Grid, true) });

        // RTTY is explicitly EXCLUDED. Distance-scored: 1 + 1 point per 500 km
        // between the grids; grid-square multipliers per band.
        yield return D("arrl-digital", "ARRL International Digital", "ARRL-DIGITAL", Hf6, new[] { "FT8", "FT4" },
            new[] { Grid() }, new[] { Grid() },
            new PointsRule { DistanceKmPerPoint = 500 }, new[] { M(MultSource.Grid, true) });

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
        // Distance-scored: 1 + 1 point per 500 km, times the operator's own power
        // multiplier (>100 W ×1, ≤100 W ×1.5, QRP ≤5 W ×3). No grid multiplier —
        // grids only feed the distance. The WORKED station's power multiplier is a
        // log-checker concern (power isn't in the grid-only exchange), so it's not
        // applied here.
        var stew = D("stew-perry", "Stew Perry Topband", "STEW-PERRY", new() { "160M" }, new[] { "CW" },
            new[] { Grid() }, new[] { Grid() },
            new PointsRule { DistanceKmPerPoint = 500 }, Array.Empty<MultRule>());
        stew.PowerMultipliers = new() { ["HIGH"] = 1, ["LOW"] = 1.5, ["QRP"] = 3 };
        yield return stew;
    }

    // ---- sprints, clubs, NAQP, digital roundups ---------------------------
    private static IEnumerable<ContestDefinition> SprintsClubsDigital()
    {
        // RTTY leg drops 160m; CW/SSB use all six HF bands.
        foreach (var (m, cab) in New("NAQP", "CW", "SSB", "RTTY"))
            // Mults = US states + VE provinces (State) + other NA countries, counted per band.
            // Non-NA (DX) contacts score QSO points only, never a mult — NaCountryExceptHome, not Dxcc.
            yield return D($"naqp-{m.L}", $"NAQP {m.N}", cab, m.N == "RTTY" ? HfNo160 : HfBands, m.Modes,
                new[] { Name(), StateF() }, new[] { Name(), StateF() },
                Pts(1), new[] { M(MultSource.State, true), M(MultSource.NaCountryExceptHome, true) },
                dupe: DupeRule.PerBand);

        // Sprints run on 80/40/20 only; multipliers counted once (all-band).
        foreach (var (m, cab) in New("NA-SPRINT", "CW", "SSB", "RTTY"))
            // Mults (US states + VE provinces + other NA countries) counted ONCE, all-band; a
            // non-NA DX contact is QSO points only — same NaCountryExceptHome fix as NAQP.
            yield return D($"na-sprint-{m.L}", $"NA Sprint {m.N}", cab, new() { "80M", "40M", "20M" }, m.Modes,
                new[] { Serial(), Name(), StateF() }, new[] { Serial(), Name(), StateF() },
                Pts(1), new[] { M(MultSource.State), M(MultSource.NaCountryExceptHome) },
                dupe: DupeRule.PerBand, serial: SerialMode.AllBand);

        // 10m only, worked once per event; no location multiplier (score = QSO points).
        // Member (non-zero 10-10 number) = 2 points, non-member = 1.
        yield return D("ten-ten", "10-10 QSO Party", "TEN-TEN", new() { "10M" }, new[] { "CW", "SSB" },
            new[] { Name(), Txt("nr", "10-10#", 6, false), StateF("S/P/C") }, new[] { Name(), Txt("nr", "10-10#", 6, false), StateF("S/P/C") },
            new PointsRule { Default = 1, MemberField = "nr", MemberPoints = 2 }, Array.Empty<MultRule>(), dupe: DupeRule.PerContest);
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
