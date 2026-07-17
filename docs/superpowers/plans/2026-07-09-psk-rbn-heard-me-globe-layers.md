# PSK & RBN "Heard Me" Globe Layers — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Two independent, toggleable globe layers drawing animated great-circle arcs from the operator's station to every station that recently heard them — PSK Reporter (digital) and RBN (CW/RTTY skimmers) — with a clickable receiver → RX report.

**Architecture:** Approach A — the globe polls two backend HTTP endpoints (existing PSK proxy + a new RBN heard-me endpoint), normalizes both into one arc model, and feeds the globe's existing `arcsData`/`pointsData`. Backend logic that is testable (filtering, band mapping, clamping, location-resolution decision) lives in pure functions; the WebGL globe is confirmed live by the operator.

**Tech stack:** .NET 10 (`RbnService`, `RbnController`, `PskReporterController`, `CtyService`, `IQrzService`), React + globe.gl (`GlobePlugin.tsx` — co-owned), zustand `settingsStore.ts`, `Settings.cs` Contracts, vitest + xUnit.

## Global Constraints

- **`GlobePlugin.tsx` is co-owned with Rick** — keep edits minimal; reuse the existing `arcsData` (animated, `arcDashAnimateTime(1500)`) and `pointsData` layers and their `onPointClick`/`pointLabel` handlers rather than adding parallel machinery.
- **Every new setting must be backend-backed**: a `Settings.cs` field with `[BsonElement]` + a TS store field/default + Settings UI. Do NOT repeat the `showPskOverlay`/`pskCallsign`/`showAuroraOverlay` mistake (frontend-only, silently dropped on save).
- **The WebGL globe cannot render headless** — all testable logic must be pure functions with unit tests; the visual is confirmed live by the operator.
- **Windows clamped on both ends**: PSK `[5,60]`, RBN `[5,120]` — frontend clamps before sending, backend clamps again.
- Branch off `v2-alpha` (branch `feat/globe-heard-me-layers` already created). Never push `local/testing`. One commit per task; tests green before each commit.

## Interfaces (verified against current code)

- `RbnService` DI: ctor takes `ILogger, IHubContext<LogHub,ILogHubClient>, IServiceProvider serviceProvider, IHotListService`. It is a hosted singleton — resolve scoped services (`IQrzService`) via `_serviceProvider.CreateScope()`.
- `IRbnService.GetRecentSpots(int minutes=5) : IReadOnlyList<RbnSpot>` and `LookupSkimmerLocationAsync(string) : Task<(string? Grid, double? Lat, double? Lon, string? Country)>` (currently a stub returning all-null).
- `Contracts.Models.RbnSpot`: `Callsign` (skimmer), `Dx` (spotted), `Frequency` (kHz, double), `Band`, `Mode`, `Snr` (int?), `Speed` (int?), `Timestamp` (DateTime UTC), `IsHot`, `Grid`, `SkimmerLat`, `SkimmerLon`, `SkimmerCountry`.
- `CtyService.GetCentroidFromCallsign(string callsign) : (double Lat, double Lon)?` — static, offline.
- `IQrzService.LookupCallsignAsync(string) : Task<QrzCallsignInfo?>`; `QrzCallsignInfo` (record, `Contracts/Api/QrzDto.cs`) has `double? Latitude, double? Longitude`.
- `PskReporterController.GetReports([FromQuery] string callsign)` hardcodes `flowStartSeconds=-3600`, caches per callsign 5 min. `PskReceptionReport` has `SenderCallsign, SenderLocator, ReceiverCallsign, ReceiverLocator, FrequencyHz (long), Mode, Snr (int), FlowStartSeconds (long)`.
- Frontend `api` (class `ApiClient`, `src/.../api/client.ts`): `getPskReports(callsign)`, `getRbnSpots(minutes)`, `getRbnSkimmerLocation(callsign)`, private `this.fetch<T>(path)`. Exported singleton `api` (line 1221).
- `MapSettings` (store) ends at `showLongPath`; Contracts `MapSettings` ends at `ShowDxNewsTicker` (line 548). Insert new fields after those.
- Globe (`GlobePlugin.tsx`): `stationLat/stationLon` computed (lines 364–386); `.arcsData([])` init (767), `.arcDashAnimateTime(1500)` (778); `arcData`/`markerData` fed at lines 1152–1153; `.onPointClick` (797); `.pointLabel` (713); `GLOBE_BAND_COLORS`/`globeBandFromFrequency` (167–183); overlay control buttons in JSX (~line 1520). `settings.station.callsign`, `settings.station.latitude/longitude/gridSquare` available; `radioStates`/`selectedRadioId` give the live rig band.

## File Structure

**Backend**
- Create `src/SDRLoggerPlus.Server/Services/Rbn/RbnHeardMeLogic.cs` — pure filter + skimmer-location decision + clamp helpers.
- Modify `src/SDRLoggerPlus.Server/Services/RbnService.cs` — implement `LookupSkimmerLocationAsync` (QRZ→cty via scope + cache).
- Modify `src/SDRLoggerPlus.Server/Controllers/RbnController.cs` — add `GET heardme`.
- Modify `src/SDRLoggerPlus.Server/Controllers/PskReporterController.cs` — add clamped `minutes` param + cache key.
- Create `src/SDRLoggerPlus.Contracts/Models/RbnHeardMeReport.cs` — DTO.
- Modify `src/SDRLoggerPlus.Contracts/Models/Settings.cs` — 5 `MapSettings` fields.
- Tests: `src/SDRLoggerPlus.Server.Tests/Tests/Services/RbnHeardMeLogicTests.cs`, `src/SDRLoggerPlus.Server.Tests/Tests/Controllers/RbnHeardMeControllerTests.cs`, `.../Controllers/PskReporterControllerTests.cs` (clamp).

**Frontend**
- Modify `src/SDRLoggerPlus.Web/src/store/settingsStore.ts` — 5 `MapSettings` fields + defaults.
- Modify `src/SDRLoggerPlus.Web/src/api/client.ts` — `getPskReports(callsign, minutes)`, `getRbnHeardMe(...)`, `RbnHeardMeReport` type.
- Create `src/SDRLoggerPlus.Web/src/utils/heardMe.ts` + `heardMe.test.ts` — pure normalizer/arc-builder.
- Create `src/SDRLoggerPlus.Web/src/hooks/useHeardMeReports.ts` — poll hook.
- Modify `src/SDRLoggerPlus.Web/src/plugins/GlobePlugin.tsx` — merge arcs/points, RX-report click card, band picker, gating.
- Modify `src/SDRLoggerPlus.Web/src/components/SettingsPanel.tsx` — toggles + band + window pickers.

---

## Task 1: Settings plumbing (Contracts + store)

**Files:**
- Modify: `src/SDRLoggerPlus.Contracts/Models/Settings.cs` (after `ShowDxNewsTicker`, ~line 548)
- Modify: `src/SDRLoggerPlus.Web/src/store/settingsStore.ts` (interface ~line 199; defaults ~line 563)

**Interfaces — Produces:** the 5 settings other tasks read: `showGlobeHeardMePsk`, `showGlobeHeardMeRbn`, `heardMeBand`, `heardMePskWindowMinutes`, `heardMeRbnWindowMinutes`.

- [ ] **Step 1: Add Contracts fields.** In `Settings.cs`, inside `class MapSettings`, after the `ShowDxNewsTicker` property, add:

```csharp
    [BsonElement("showGlobeHeardMePsk")]
    public bool ShowGlobeHeardMePsk { get; set; }

    [BsonElement("showGlobeHeardMeRbn")]
    public bool ShowGlobeHeardMeRbn { get; set; }

    [BsonElement("heardMeBand")]
    public string HeardMeBand { get; set; } = "20m";

    [BsonElement("heardMePskWindowMinutes")]
    public int HeardMePskWindowMinutes { get; set; } = 60;

    [BsonElement("heardMeRbnWindowMinutes")]
    public int HeardMeRbnWindowMinutes { get; set; } = 30;
```

- [ ] **Step 2: Add store fields.** In `settingsStore.ts` `interface MapSettings`, after `showLongPath: boolean;` add:

```ts
  // "Heard Me" globe layers — arcs from your station to stations that heard you.
  showGlobeHeardMePsk: boolean;   // PSK Reporter (digital)
  showGlobeHeardMeRbn: boolean;   // RBN (CW/RTTY skimmers)
  heardMeBand: string;            // manual band fallback when no rig connected
  heardMePskWindowMinutes: number; // PSK look-back, clamped [5,60]
  heardMeRbnWindowMinutes: number; // RBN look-back, clamped [5,120]
```

- [ ] **Step 3: Add store defaults.** In the default `map` object (near `showLongPath: true,`), add:

```ts
    showGlobeHeardMePsk: false,
    showGlobeHeardMeRbn: false,
    heardMeBand: '20m',
    heardMePskWindowMinutes: 60,
    heardMeRbnWindowMinutes: 30,
```

- [ ] **Step 4: Typecheck + build Contracts.**

Run: `cd src/SDRLoggerPlus.Web && npx tsc --noEmit` → Expected: exit 0.
Run: `dotnet build src/SDRLoggerPlus.Contracts` → Expected: Build succeeded.

- [ ] **Step 5: Commit.**

```bash
git add src/SDRLoggerPlus.Contracts/Models/Settings.cs src/SDRLoggerPlus.Web/src/store/settingsStore.ts
git commit -m "feat(settings): Heard-Me globe layer settings (toggles, band, windows)"
```

---

## Task 2: PSK endpoint — clamped window param

**Files:**
- Modify: `src/SDRLoggerPlus.Server/Controllers/PskReporterController.cs`
- Test: `src/SDRLoggerPlus.Server.Tests/Tests/Controllers/PskReporterControllerTests.cs`

**Interfaces — Produces:** `GET /api/pskreporter/reports?callsign=&minutes=` (minutes clamped [5,60]); `PskReporterController.ClampWindowMinutes(int) : int`.

- [ ] **Step 1: Write the failing clamp test.** Create/extend the test file:

```csharp
using SDRLoggerPlus.Server.Controllers;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Controllers;

[Trait("Category", "Unit")]
public class PskReporterControllerTests
{
    [Theory]
    [InlineData(0, 5)]     // below min → 5
    [InlineData(3, 5)]
    [InlineData(5, 5)]
    [InlineData(30, 30)]   // in range → unchanged
    [InlineData(60, 60)]
    [InlineData(120, 60)]  // above max → 60
    public void ClampWindowMinutes_clamps_to_5_to_60(int input, int expected)
    {
        Assert.Equal(expected, PskReporterController.ClampWindowMinutes(input));
    }
}
```

- [ ] **Step 2: Run it — expect FAIL** (method doesn't exist).

Run: `dotnet test src/SDRLoggerPlus.Server.Tests --filter "FullyQualifiedName~PskReporterControllerTests"` → Expected: compile error / fail.

- [ ] **Step 3: Implement.** In `PskReporterController`:
  - Add the helper:

```csharp
    /// <summary>Clamp the PSK look-back window to PSK Reporter's acceptable range.</summary>
    public static int ClampWindowMinutes(int minutes) => Math.Clamp(minutes, 5, 60);
```

  - Change the action signature to `GetReports([FromQuery] string callsign, [FromQuery] int minutes = 60)`.
  - At the top of the body (after the null check), add `minutes = ClampWindowMinutes(minutes);`.
  - Change the cache key to include the window: `var cacheKey = $"pskreporter_{callsign}_{minutes}";`.
  - Change the URL's `flowStartSeconds=-3600` to `flowStartSeconds=-{minutes * 60}`.

- [ ] **Step 4: Run test — expect PASS.**

Run: `dotnet test src/SDRLoggerPlus.Server.Tests --filter "FullyQualifiedName~PskReporterControllerTests"` → Expected: passed.

- [ ] **Step 5: Commit.**

```bash
git add src/SDRLoggerPlus.Server/Controllers/PskReporterController.cs src/SDRLoggerPlus.Server.Tests/Tests/Controllers/PskReporterControllerTests.cs
git commit -m "feat(pskreporter): clamped minutes window param + per-window cache key"
```

---

## Task 3: RBN heard-me pure logic (filter + band + clamp + location decision)

**Files:**
- Create: `src/SDRLoggerPlus.Server/Services/Rbn/RbnHeardMeLogic.cs`
- Test: `src/SDRLoggerPlus.Server.Tests/Tests/Services/RbnHeardMeLogicTests.cs`

**Interfaces — Produces:**
- `RbnHeardMeLogic.ClampWindowMinutes(int) : int` (5..120)
- `RbnHeardMeLogic.HeardBy(IEnumerable<RbnSpot> spots, string myCall, string? band) : List<RbnSpot>` — spots where `Dx==myCall` (case-insensitive) and (band null/empty OR `spot.Band==band`).
- `RbnHeardMeLogic.PickLocation(double? qrzLat, double? qrzLon, (double Lat,double Lon)? ctyCentroid) : (double Lat,double Lon)?` — QRZ coords when both present, else cty centroid, else null.

- [ ] **Step 1: Write the failing tests.**

```csharp
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Services.Rbn;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

[Trait("Category", "Unit")]
public class RbnHeardMeLogicTests
{
    private static RbnSpot Spot(string skimmer, string dx, double khz, string band) =>
        new() { Callsign = skimmer, Dx = dx, Frequency = khz, Band = band, Mode = "CW", Snr = 20, Timestamp = DateTime.UtcNow };

    [Fact]
    public void HeardBy_matches_dx_call_case_insensitively()
    {
        var spots = new[] { Spot("W3LPL", "K1ABC", 14025, "20m"), Spot("N4ZR", "W9XYZ", 7025, "40m") };
        var result = RbnHeardMeLogic.HeardBy(spots, "k1abc", null);
        Assert.Single(result);
        Assert.Equal("W3LPL", result[0].Callsign);
    }

    [Fact]
    public void HeardBy_filters_by_band_when_supplied()
    {
        var spots = new[] { Spot("W3LPL", "K1ABC", 14025, "20m"), Spot("N4ZR", "K1ABC", 7025, "40m") };
        Assert.Single(RbnHeardMeLogic.HeardBy(spots, "K1ABC", "20m"));
        Assert.Equal(2, RbnHeardMeLogic.HeardBy(spots, "K1ABC", null).Count);
    }

    [Theory]
    [InlineData(0, 5)]
    [InlineData(30, 30)]
    [InlineData(999, 120)]
    public void ClampWindowMinutes_clamps_to_5_to_120(int input, int expected)
        => Assert.Equal(expected, RbnHeardMeLogic.ClampWindowMinutes(input));

    [Fact]
    public void PickLocation_prefers_qrz_then_cty_then_null()
    {
        Assert.Equal((40.0, -75.0), RbnHeardMeLogic.PickLocation(40.0, -75.0, (1.0, 2.0)));
        Assert.Equal((1.0, 2.0), RbnHeardMeLogic.PickLocation(null, null, (1.0, 2.0)));
        Assert.Equal((1.0, 2.0), RbnHeardMeLogic.PickLocation(40.0, null, (1.0, 2.0))); // partial QRZ → cty
        Assert.Null(RbnHeardMeLogic.PickLocation(null, null, null));
    }
}
```

- [ ] **Step 2: Run — expect FAIL** (class missing).

Run: `dotnet test src/SDRLoggerPlus.Server.Tests --filter "FullyQualifiedName~RbnHeardMeLogicTests"` → Expected: compile error.

- [ ] **Step 3: Implement the pure logic.**

```csharp
using SDRLoggerPlus.Contracts.Models;

namespace SDRLoggerPlus.Server.Services.Rbn;

/// <summary>
/// Pure helpers for the RBN "who heard me" endpoint — no telnet, no I/O, unit-testable.
/// </summary>
public static class RbnHeardMeLogic
{
    public static int ClampWindowMinutes(int minutes) => Math.Clamp(minutes, 5, 120);

    /// <summary>Spots where <paramref name="myCall"/> is the spotted DX, optionally on one band.</summary>
    public static List<RbnSpot> HeardBy(IEnumerable<RbnSpot> spots, string myCall, string? band)
    {
        var call = (myCall ?? string.Empty).Trim();
        return spots.Where(s =>
                string.Equals(s.Dx, call, StringComparison.OrdinalIgnoreCase) &&
                (string.IsNullOrEmpty(band) || string.Equals(s.Band, band, StringComparison.OrdinalIgnoreCase)))
            .ToList();
    }

    /// <summary>QRZ coords when BOTH present, else the cty centroid, else null.</summary>
    public static (double Lat, double Lon)? PickLocation(double? qrzLat, double? qrzLon, (double Lat, double Lon)? ctyCentroid)
    {
        if (qrzLat is { } la && qrzLon is { } lo) return (la, lo);
        return ctyCentroid;
    }
}
```

- [ ] **Step 4: Run — expect PASS.**

Run: `dotnet test src/SDRLoggerPlus.Server.Tests --filter "FullyQualifiedName~RbnHeardMeLogicTests"` → Expected: passed.

- [ ] **Step 5: Commit.**

```bash
git add src/SDRLoggerPlus.Server/Services/Rbn/RbnHeardMeLogic.cs src/SDRLoggerPlus.Server.Tests/Tests/Services/RbnHeardMeLogicTests.cs
git commit -m "feat(rbn): pure heard-me filter + window clamp + location-pick logic"
```

---

## Task 4: Implement skimmer-location resolution in RbnService

**Files:**
- Modify: `src/SDRLoggerPlus.Server/Services/RbnService.cs` (`LookupSkimmerLocationAsync`, ~lines 310–324)

**Interfaces — Consumes:** `RbnHeardMeLogic.PickLocation`, `CtyService.GetCentroidFromCallsign`, `IQrzService.LookupCallsignAsync`. **Produces:** `LookupSkimmerLocationAsync` returning real coords (was all-null stub), cached per skimmer in `_skimmerLocations`.

- [ ] **Step 1: Replace the stub body.** In `LookupSkimmerLocationAsync`, keep the existing cache-hit check at the top, then replace the "return null" stub (lines ~317–323) with:

```csharp
        double? qrzLat = null, qrzLon = null;
        try
        {
            using var scope = _serviceProvider.CreateScope();
            var qrz = scope.ServiceProvider.GetService<IQrzService>();
            if (qrz is not null)
            {
                var info = await qrz.LookupCallsignAsync(callsign);
                qrzLat = info?.Latitude;
                qrzLon = info?.Longitude;
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "QRZ lookup failed for skimmer {Skimmer}; falling back to cty.dat", callsign);
        }

        var centroid = CtyService.GetCentroidFromCallsign(callsign);
        var picked = RbnHeardMeLogic.PickLocation(qrzLat, qrzLon, centroid);

        // Grid/Country stay null — arcs only need lat/lon; the tuple shape is unchanged.
        var result = picked is { } p
            ? (Grid: (string?)null, Lat: (double?)p.Lat, Lon: (double?)p.Lon, Country: (string?)null)
            : (Grid: (string?)null, Lat: (double?)null, Lon: (double?)null, Country: (string?)null);
        _skimmerLocations.TryAdd(callsign, result);
        return result;
```

Add `using SDRLoggerPlus.Server.Services.Rbn;` and `using Microsoft.Extensions.DependencyInjection;` at the top if not present. (Grid/Country stay null — arcs only need lat/lon; the tuple shape is unchanged so existing callers still compile.)

- [ ] **Step 2: Build.**

Run: `dotnet build src/SDRLoggerPlus.Server` → Expected: Build succeeded (0 errors).

- [ ] **Step 3: Commit.**

```bash
git add src/SDRLoggerPlus.Server/Services/RbnService.cs
git commit -m "feat(rbn): resolve skimmer location via QRZ with cty.dat fallback (cached)"
```

> **Note (for reviewer):** this method is I/O-bound (QRZ + scope) so it isn't unit-tested directly; its decision core is covered by `RbnHeardMeLogic.PickLocation` (Task 3). The endpoint test (Task 5) exercises it via a fake `IRbnService`.

---

## Task 5: RBN heard-me endpoint + DTO

**Files:**
- Create: `src/SDRLoggerPlus.Contracts/Models/RbnHeardMeReport.cs`
- Modify: `src/SDRLoggerPlus.Server/Controllers/RbnController.cs`
- Test: `src/SDRLoggerPlus.Server.Tests/Tests/Controllers/RbnHeardMeControllerTests.cs`

**Interfaces — Produces:** `GET /api/rbn/heardme?callsign=&band=&minutes=` → `List<RbnHeardMeReport>`.

- [ ] **Step 1: Create the DTO.**

```csharp
namespace SDRLoggerPlus.Contracts.Models;

/// <summary>One RBN reception ("skimmer heard my callsign") for the globe Heard-Me layer.</summary>
public class RbnHeardMeReport
{
    public string Skimmer { get; set; } = string.Empty;
    public double Lat { get; set; }
    public double Lon { get; set; }
    public double FreqKhz { get; set; }
    public string Band { get; set; } = string.Empty;
    public string Mode { get; set; } = string.Empty;
    public int Snr { get; set; }
    public long AgeSeconds { get; set; }
}
```

- [ ] **Step 2: Write the failing controller test.** Uses a fake `IRbnService` (one locatable skimmer, one not).

```csharp
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging.Abstractions;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Controllers;
using SDRLoggerPlus.Server.Services;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Controllers;

[Trait("Category", "Unit")]
public class RbnHeardMeControllerTests
{
    private sealed class FakeRbn : IRbnService
    {
        public IReadOnlyList<RbnSpot> Spots = Array.Empty<RbnSpot>();
        public IReadOnlyList<RbnSpot> GetRecentSpots(int minutes = 5) => Spots;
        public Task<(string? Grid, double? Lat, double? Lon, string? Country)> LookupSkimmerLocationAsync(string callsign)
            => Task.FromResult(callsign == "W3LPL"
                ? ((string?)null, (double?)39.0, (double?)-77.0, (string?)null)
                : ((string?)null, (double?)null, (double?)null, (string?)null));
    }

    [Fact]
    public async Task HeardMe_returns_only_located_spots_for_my_call()
    {
        var fake = new FakeRbn
        {
            Spots = new[]
            {
                new RbnSpot { Callsign = "W3LPL", Dx = "K1ABC", Frequency = 14025, Band = "20m", Mode = "CW", Snr = 25, Timestamp = DateTime.UtcNow },
                new RbnSpot { Callsign = "NOLOC", Dx = "K1ABC", Frequency = 14025, Band = "20m", Mode = "CW", Snr = 10, Timestamp = DateTime.UtcNow },
                new RbnSpot { Callsign = "N4ZR", Dx = "OTHER", Frequency = 14025, Band = "20m", Mode = "CW", Snr = 30, Timestamp = DateTime.UtcNow },
            }
        };
        var controller = new RbnController(NullLogger<RbnController>.Instance, fake);

        var result = await controller.GetHeardMe("k1abc", null, 30);

        var ok = Assert.IsType<OkObjectResult>(result);
        var reports = Assert.IsAssignableFrom<IEnumerable<RbnHeardMeReport>>(ok.Value);
        var list = reports.ToList();
        Assert.Single(list);                       // OTHER filtered out; NOLOC dropped (no coords)
        Assert.Equal("W3LPL", list[0].Skimmer);
        Assert.Equal(39.0, list[0].Lat);
    }
}
```

- [ ] **Step 3: Run — expect FAIL** (`GetHeardMe` missing).

Run: `dotnet test src/SDRLoggerPlus.Server.Tests --filter "FullyQualifiedName~RbnHeardMeControllerTests"` → Expected: compile error.

- [ ] **Step 4: Implement the endpoint.** Add to `RbnController` (add `using SDRLoggerPlus.Contracts.Models;` and `using SDRLoggerPlus.Server.Services.Rbn;`):

```csharp
    [HttpGet("heardme")]
    public async Task<IActionResult> GetHeardMe(
        [FromQuery] string callsign,
        [FromQuery] string? band = null,
        [FromQuery] int minutes = 30)
    {
        if (string.IsNullOrWhiteSpace(callsign))
            return BadRequest(new { error = "callsign is required" });

        minutes = RbnHeardMeLogic.ClampWindowMinutes(minutes);
        var matches = RbnHeardMeLogic.HeardBy(_rbnService.GetRecentSpots(minutes), callsign, band);

        var now = DateTime.UtcNow;
        var reports = new List<RbnHeardMeReport>();
        foreach (var s in matches)
        {
            var (_, lat, lon, _) = await _rbnService.LookupSkimmerLocationAsync(s.Callsign);
            if (lat is not { } la || lon is not { } lo) continue; // no location → can't draw an arc
            reports.Add(new RbnHeardMeReport
            {
                Skimmer = s.Callsign,
                Lat = la,
                Lon = lo,
                FreqKhz = s.Frequency,
                Band = s.Band,
                Mode = s.Mode,
                Snr = s.Snr ?? 0,
                AgeSeconds = (long)Math.Max(0, (now - s.Timestamp).TotalSeconds),
            });
        }
        return Ok(reports);
    }
```

- [ ] **Step 5: Run test — expect PASS.**

Run: `dotnet test src/SDRLoggerPlus.Server.Tests --filter "FullyQualifiedName~RbnHeardMeControllerTests"` → Expected: passed.

- [ ] **Step 6: Commit.**

```bash
git add src/SDRLoggerPlus.Contracts/Models/RbnHeardMeReport.cs src/SDRLoggerPlus.Server/Controllers/RbnController.cs src/SDRLoggerPlus.Server.Tests/Tests/Controllers/RbnHeardMeControllerTests.cs
git commit -m "feat(rbn): GET /api/rbn/heardme endpoint + RbnHeardMeReport DTO"
```

---

## Task 6: Frontend heard-me normalizer/arc-builder (pure util)

**Files:**
- Create: `src/SDRLoggerPlus.Web/src/utils/heardMe.ts`
- Test: `src/SDRLoggerPlus.Web/src/utils/heardMe.test.ts`

**Interfaces — Consumes:** `gridToLatLon` (`../utils/maidenhead`). Uses local structural input types (no api-module dependency). **Produces:** `HeardMeArc` type + `buildHeardMeArcs(station, psk, rbn, opts) : HeardMeArc[]`, `heardMeBandColor(band) : string`.

- [ ] **Step 1: Write the failing tests.**

```ts
import { describe, it, expect } from 'vitest';
import { buildHeardMeArcs, type HeardMeArc } from './heardMe';

const station = { lat: 43.0, lon: -89.4 }; // Madison WI-ish

describe('buildHeardMeArcs', () => {
  it('maps PSK reports to arcs from the station to each receiver', () => {
    const arcs = buildHeardMeArcs(station,
      [{ senderCallsign: 'K1ABC', senderLocator: 'EN53', receiverCallsign: 'W3LPL',
         receiverLocator: 'FM19', frequencyHz: 14074000, mode: 'FT8', snr: -5, flowStartSeconds: 0 }],
      [], { band: '20m' });
    expect(arcs).toHaveLength(1);
    expect(arcs[0].source).toBe('psk');
    expect(arcs[0].startLat).toBeCloseTo(station.lat);
    expect(arcs[0].receiverCall).toBe('W3LPL');
    expect(arcs[0].band).toBe('20m');
  });

  it('filters PSK to the active band client-side', () => {
    const arcs = buildHeardMeArcs(station,
      [{ senderCallsign: 'K1ABC', senderLocator: 'EN53', receiverCallsign: 'W3LPL',
         receiverLocator: 'FM19', frequencyHz: 7074000, mode: 'FT8', snr: -5, flowStartSeconds: 0 }],
      [], { band: '20m' });
    expect(arcs).toHaveLength(0); // 40m report excluded when band=20m
  });

  it('maps RBN reports (already located) to arcs and tags source rbn', () => {
    const arcs = buildHeardMeArcs(station, [],
      [{ skimmer: 'N4ZR', lat: 39.0, lon: -77.0, freqKhz: 14025, band: '20m', mode: 'CW', snr: 25, ageSeconds: 60 }],
      { band: '20m' });
    expect(arcs).toHaveLength(1);
    expect(arcs[0].source).toBe('rbn');
    expect(arcs[0].endLat).toBeCloseTo(39.0);
  });

  it('caps and sorts by SNR/recency', () => {
    const rbn = Array.from({ length: 300 }, (_, i) => ({
      skimmer: `S${i}`, lat: i % 80, lon: i % 80, freqKhz: 14025, band: '20m', mode: 'CW', snr: i, ageSeconds: 0,
    }));
    const arcs = buildHeardMeArcs(station, [], rbn, { band: '20m', cap: 150 });
    expect(arcs.length).toBe(150);
    expect(arcs[0].snr).toBeGreaterThanOrEqual(arcs[149].snr); // strongest first
  });
});
```

- [ ] **Step 2: Run — expect FAIL** (module missing).

Run: `cd src/SDRLoggerPlus.Web && npx vitest run src/utils/heardMe.test.ts` → Expected: fail (cannot import).

- [ ] **Step 3: Implement.**

```ts
import { gridToLatLon } from './maidenhead';

// Structural inputs — match the api/client PskReceptionReport / RbnHeardMeReport
// shapes, defined locally so this util has no dependency on the api module (and
// so it can be built/tested before Task 7 adds those types). Real
// PskReceptionReport[] / RbnHeardMeReport[] are assignable to these.
interface PskInput {
  receiverCallsign: string; receiverLocator: string;
  frequencyHz: number; mode: string; snr: number; flowStartSeconds: number;
  senderCallsign?: string; senderLocator?: string;
}
interface RbnInput {
  skimmer: string; lat: number; lon: number;
  freqKhz: number; band: string; mode: string; snr: number; ageSeconds: number;
}

export interface HeardMeArc {
  source: 'psk' | 'rbn';
  receiverCall: string;
  startLat: number; startLon: number; // station
  endLat: number; endLon: number;     // receiver
  freqKhz: number; band: string; mode: string; snr: number; ageMinutes: number;
}

const BAND_COLORS: Record<string, string> = {
  '160m': '#8B0000', '80m': '#DC143C', '60m': '#FF6347', '40m': '#FF8C00',
  '30m': '#FFD700', '20m': '#32CD32', '17m': '#00CED1', '15m': '#00BFFF',
  '12m': '#4169E1', '10m': '#8A2BE2', '6m': '#FF00FF',
};
const BAND_RANGES: Record<string, [number, number]> = {
  '160m': [1800, 2000], '80m': [3500, 4000], '60m': [5330, 5410],
  '40m': [7000, 7300], '30m': [10100, 10150], '20m': [14000, 14350],
  '17m': [18068, 18168], '15m': [21000, 21450], '12m': [24890, 24990],
  '10m': [28000, 29700], '6m': [50000, 54000],
};
export function heardMeBandColor(band: string): string { return BAND_COLORS[band] ?? '#888888'; }
function bandFromKhz(khz: number): string {
  for (const [b, [lo, hi]] of Object.entries(BAND_RANGES)) if (khz >= lo && khz <= hi) return b;
  return '?';
}

interface Station { lat: number; lon: number; }
interface Opts { band: string | null; cap?: number; }

/** Normalize PSK + RBN reception into station→receiver arcs, band-filtered, capped by SNR then recency. */
export function buildHeardMeArcs(
  station: Station, psk: PskInput[], rbn: RbnInput[], opts: Opts,
): HeardMeArc[] {
  const cap = opts.cap ?? 150;
  const band = opts.band ?? null;
  const arcs: HeardMeArc[] = [];

  for (const r of psk) {
    const khz = r.frequencyHz / 1000;
    const b = bandFromKhz(khz);
    if (band && b !== band) continue;
    const to = gridToLatLon(r.receiverLocator);
    if (!to) continue;
    arcs.push({
      source: 'psk', receiverCall: r.receiverCallsign,
      startLat: station.lat, startLon: station.lon, endLat: to.lat, endLon: to.lon,
      freqKhz: khz, band: b, mode: r.mode, snr: r.snr,
      ageMinutes: Math.max(0, Math.round((Date.now() / 1000 - r.flowStartSeconds) / 60)),
    });
  }
  for (const r of rbn) {
    if (band && r.band !== band) continue;
    arcs.push({
      source: 'rbn', receiverCall: r.skimmer,
      startLat: station.lat, startLon: station.lon, endLat: r.lat, endLon: r.lon,
      freqKhz: r.freqKhz, band: r.band, mode: r.mode, snr: r.snr,
      ageMinutes: Math.round(r.ageSeconds / 60),
    });
  }

  arcs.sort((a, b) => (b.snr - a.snr) || (a.ageMinutes - b.ageMinutes));
  return arcs.slice(0, cap);
}
```

- [ ] **Step 4: Run — expect PASS.**

Run: `cd src/SDRLoggerPlus.Web && npx vitest run src/utils/heardMe.test.ts` → Expected: passed. Then `npx tsc --noEmit` → Expected: exit 0 (the util has no api-module dependency, so it builds standalone).

- [ ] **Step 5: Commit.**

```bash
git add src/SDRLoggerPlus.Web/src/utils/heardMe.ts src/SDRLoggerPlus.Web/src/utils/heardMe.test.ts
git commit -m "feat(globe): pure heard-me arc normalizer (PSK+RBN → capped arcs)"
```

---

## Task 7: Frontend API client methods + type

**Files:**
- Modify: `src/SDRLoggerPlus.Web/src/api/client.ts`

**Interfaces — Produces:** `RbnHeardMeReport` interface; `api.getPskReports(callsign, minutes?)`; `api.getRbnHeardMe(callsign, band, minutes)`.

- [ ] **Step 1: Add the type** near `PskReceptionReport` (~line 231):

```ts
export interface RbnHeardMeReport {
  skimmer: string;
  lat: number;
  lon: number;
  freqKhz: number;
  band: string;
  mode: string;
  snr: number;
  ageSeconds: number;
}
```

- [ ] **Step 2: Update `getPskReports`** (~line 598) to accept a window:

```ts
  async getPskReports(callsign: string, minutes = 60): Promise<PskReceptionReport[]> {
    return this.fetch<PskReceptionReport[]>(
      `/pskreporter/reports?callsign=${encodeURIComponent(callsign)}&minutes=${minutes}`);
  }
```

- [ ] **Step 3: Add `getRbnHeardMe`** next to `getRbnSpots` (~line 399):

```ts
  async getRbnHeardMe(callsign: string, band: string | null, minutes = 30): Promise<RbnHeardMeReport[]> {
    const bandParam = band ? `&band=${encodeURIComponent(band)}` : '';
    return this.fetch<RbnHeardMeReport[]>(
      `/rbn/heardme?callsign=${encodeURIComponent(callsign)}${bandParam}&minutes=${minutes}`);
  }
```

- [ ] **Step 4: Typecheck.**

Run: `cd src/SDRLoggerPlus.Web && npx tsc --noEmit` → Expected: exit 0.

- [ ] **Step 5: Commit.**

```bash
git add src/SDRLoggerPlus.Web/src/api/client.ts
git commit -m "feat(api): getRbnHeardMe + windowed getPskReports + RbnHeardMeReport type"
```

---

## Task 8: `useHeardMeReports` polling hook

**Files:**
- Create: `src/SDRLoggerPlus.Web/src/hooks/useHeardMeReports.ts`

**Interfaces — Consumes:** `api.getPskReports`, `api.getRbnHeardMe`, `useSettingsStore`, `useAppStore` (`radioStates`, `selectedRadioId`). **Produces:** `useHeardMeReports() : { psk: PskReceptionReport[]; rbn: RbnHeardMeReport[]; activeBand: string | null }`.

- [ ] **Step 1: Implement the hook.** (No standalone unit test — it's thin glue over tested pieces; verified live + via `tsc`.)

```ts
import { useEffect, useRef, useState } from 'react';
import { api, type PskReceptionReport, type RbnHeardMeReport } from '../api/client';
import { useSettingsStore } from '../store/settingsStore';
import { useAppStore } from '../store/appStore';

const clamp = (n: number, lo: number, hi: number) => Math.min(hi, Math.max(lo, n));

/** Polls PSK + RBN "who heard me" per the enabled Heard-Me layers. */
export function useHeardMeReports() {
  const { settings } = useSettingsStore();
  const { radioStates, selectedRadioId } = useAppStore();
  const map = settings.map;
  const call = (settings.station.callsign || '').trim();

  // Active band: live rig band when connected, else the manual fallback.
  const rigBand = selectedRadioId ? radioStates.get(selectedRadioId)?.band ?? null : null;
  const activeBand = rigBand || map.heardMeBand || null;

  const [psk, setPsk] = useState<PskReceptionReport[]>([]);
  const [rbn, setRbn] = useState<RbnHeardMeReport[]>([]);

  const pskWindow = clamp(map.heardMePskWindowMinutes ?? 60, 5, 60);
  const rbnWindow = clamp(map.heardMeRbnWindowMinutes ?? 30, 5, 120);

  // PSK: 5-min cadence (etiquette + backend cache).
  useEffect(() => {
    if (!map.showGlobeHeardMePsk || !call) { setPsk([]); return; }
    let cancelled = false;
    const run = async () => {
      try { const r = await api.getPskReports(call, pskWindow); if (!cancelled) setPsk(r); } catch { /* keep last */ }
    };
    run();
    const id = setInterval(run, 5 * 60 * 1000);
    return () => { cancelled = true; clearInterval(id); };
  }, [map.showGlobeHeardMePsk, call, pskWindow]);

  // RBN: ~45s cadence.
  useEffect(() => {
    if (!map.showGlobeHeardMeRbn || !call) { setRbn([]); return; }
    let cancelled = false;
    const run = async () => {
      try { const r = await api.getRbnHeardMe(call, activeBand, rbnWindow); if (!cancelled) setRbn(r); } catch { /* keep last */ }
    };
    run();
    const id = setInterval(run, 45 * 1000);
    return () => { cancelled = true; clearInterval(id); };
  }, [map.showGlobeHeardMeRbn, call, activeBand, rbnWindow]);

  return { psk, rbn, activeBand };
}
```

- [ ] **Step 2: Typecheck.**

Run: `cd src/SDRLoggerPlus.Web && npx tsc --noEmit` → Expected: exit 0. (Confirm `radioStates.get(id)?.band` exists on the radio-state type; it's used by RigPlugin/LogEntry today.)

- [ ] **Step 3: Commit.**

```bash
git add src/SDRLoggerPlus.Web/src/hooks/useHeardMeReports.ts
git commit -m "feat(globe): useHeardMeReports polling hook (PSK 5m / RBN 45s)"
```

---

## Task 9: GlobePlugin — render arcs + receiver points + RX-report click

**Files:**
- Modify: `src/SDRLoggerPlus.Web/src/plugins/GlobePlugin.tsx`

**Interfaces — Consumes:** `useHeardMeReports`, `buildHeardMeArcs`, `heardMeBandColor`, `stationLat/stationLon`. Merges into the existing `arcData` (fed at line 1153) and `markerData` (line 1152); extends `onPointClick` (797) + `pointLabel` (713).

- [ ] **Step 1: Wire the hook + build arcs.** Near the other hooks (after `const { settings, ... } = useSettingsStore()`), add:

```tsx
  const { psk: heardMePsk, rbn: heardMeRbn, activeBand: heardMeActiveBand } = useHeardMeReports();
```

Import at top: `import { useHeardMeReports } from '../hooks/useHeardMeReports';` and `import { buildHeardMeArcs, heardMeBandColor, type HeardMeArc } from '../utils/heardMe';`.

- [ ] **Step 2: Compute heard-me arcs** in the effect that builds `arcData`/`markerData` (around lines 1100–1153). Before `globeRef.current.arcsData(arcData)`:

```tsx
    const heardArcs: HeardMeArc[] = (settings.map.showGlobeHeardMePsk || settings.map.showGlobeHeardMeRbn)
      ? buildHeardMeArcs(
          { lat: stationLat, lon: stationLon },
          settings.map.showGlobeHeardMePsk ? heardMePsk : [],
          settings.map.showGlobeHeardMeRbn ? heardMeRbn : [],
          { band: heardMeActiveBand, cap: 150 },
        )
      : [];
```

- [ ] **Step 3: Map heard-me arcs into the globe arc shape** and append to `arcData`. Match the existing arc object shape used at line 1153 (inspect it first). Heard-me arcs use `arcColor` = band color, `arcStroke` thin, and the dash distinguishes PSK vs RBN (RBN gets a shorter dash). If the existing arc layer's accessors are shared, add fields consistent with them, e.g.:

```tsx
    for (const a of heardArcs) {
      arcData.push({
        startLat: a.startLat, startLng: a.startLon, endLat: a.endLat, endLng: a.endLon,
        color: heardMeBandColor(a.band),
        // marker for click/label handlers to recognize a heard-me endpoint:
        heardMe: { source: a.source, receiverCall: a.receiverCall, band: a.band,
                   freqKhz: a.freqKhz, mode: a.mode, snr: a.snr, ageMinutes: a.ageMinutes },
      });
    }
```

If the current `arcColor`/`arcStroke`/dash accessors are fixed constants (lines ~767–790), extend them to read per-arc: `arcColor((d)=> (d as any).color ?? <existing>)`, and `arcDashLength`/`arcDashGap` to give `heardMe.source==='rbn'` a distinct dash. Keep changes additive so existing DX-spot arcs are unaffected.

- [ ] **Step 4: Add receiver points** to `markerData` (fed at line 1152) so each receiver is clickable, tagged `type:'heardme'`:

```tsx
    for (const a of heardArcs) {
      markerData.push({
        lat: a.endLat, lng: a.endLon, text: a.receiverCall, color: heardMeBandColor(a.band),
        type: 'heardme', heardMe: { source: a.source, receiverCall: a.receiverCall, band: a.band,
                                    freqKhz: a.freqKhz, mode: a.mode, snr: a.snr, ageMinutes: a.ageMinutes },
      });
    }
```

Extend the `MarkerData`/point type union (line ~158) to include `'heardme'` and an optional `heardMe` payload.

- [ ] **Step 5: RX-report on click.** In `onPointClick` (~797), branch on `type==='heardme'`: set a new state `heardMeSelected` to the point's `heardMe` payload (instead of the callsign lookup used for DX). Render a small floating card (reuse the existing overlay-card styling) showing: receiver call, band, `${freqKhz.toFixed(1)} kHz`, mode, `SNR ${snr} dB`, `${ageMinutes}m ago`, and a "PSK"/"RBN" source chip. Add a close (×). Also extend `pointLabel` (~713) so hovering a `heardme` point shows a compact tooltip with the same fields.

- [ ] **Step 6: Typecheck + live build.**

Run: `cd src/SDRLoggerPlus.Web && npx tsc --noEmit` → Expected: exit 0.
Run the app (backend + vite + electron per CLAUDE.md); enable a layer in Settings; **operator confirms live**: arcs animate from the station to receivers, band-colored; clicking a receiver shows its RX report; PSK vs RBN dashes differ. (Globe can't be verified headless.)

- [ ] **Step 7: Commit.**

```bash
git add src/SDRLoggerPlus.Web/src/plugins/GlobePlugin.tsx
git commit -m "feat(globe): render PSK/RBN heard-me arcs + clickable RX report"
```

---

## Task 10: Settings UI + on-globe band picker

**Files:**
- Modify: `src/SDRLoggerPlus.Web/src/components/SettingsPanel.tsx`
- Modify: `src/SDRLoggerPlus.Web/src/plugins/GlobePlugin.tsx` (overlay band picker)

**Interfaces — Consumes:** `updateMapSettings`, the 5 settings from Task 1.

- [ ] **Step 1: Settings controls.** In the Map section of `SettingsPanel.tsx` (follow the existing PSK/aurora checkbox pattern), add:
  - Checkbox "Heard Me — PSK (globe)" → `updateMapSettings({ showGlobeHeardMePsk: e.target.checked })`.
  - Checkbox "Heard Me — RBN (globe)" → `updateMapSettings({ showGlobeHeardMeRbn: e.target.checked })`.
  - Band `<select>` (options from the band list used elsewhere, e.g. `BANDS`) bound to `heardMeBand`.
  - Two window `<select>`s: PSK options `[15,30,60]`, RBN options `[15,30,60,120]`, writing `heardMePskWindowMinutes` / `heardMeRbnWindowMinutes` (values already within clamp range).

- [ ] **Step 2: On-globe band picker.** In the globe overlay JSX (near the Pause/Play + SunMoon buttons, ~line 1520), when `(showGlobeHeardMePsk || showGlobeHeardMeRbn)` AND no live rig band, render a compact `<select>` bound to `heardMeBand` (via `updateMapSettings` + `saveSettings`, mirroring how the existing globe toggles persist). When a rig band is live, show it as a small read-only chip instead (the layer follows the rig).

- [ ] **Step 3: Typecheck.**

Run: `cd src/SDRLoggerPlus.Web && npx tsc --noEmit` → Expected: exit 0.

- [ ] **Step 4: Backend settings round-trip.** With the backend running, PUT settings with `showGlobeHeardMeRbn:true` + `heardMeRbnWindowMinutes:120`, GET them back, assert both persist (confirms the Contracts fields from Task 1 are wired). 

- [ ] **Step 5: Commit.**

```bash
git add src/SDRLoggerPlus.Web/src/components/SettingsPanel.tsx src/SDRLoggerPlus.Web/src/plugins/GlobePlugin.tsx
git commit -m "feat(globe): Heard-Me settings toggles + on-globe band picker"
```

---

## Final verification (whole feature)

- `dotnet test src/SDRLoggerPlus.Server.Tests --filter "Category=Unit"` → all green (incl. new RBN/PSK tests).
- `cd src/SDRLoggerPlus.Web && npx vitest run` → heardMe tests green; **no NEW failures** vs the 8 pre-existing (themes/MeterPlugin/AboutDialog) already failing on `v2-alpha`.
- `npx tsc --noEmit` exit 0; `dotnet build` clean.
- Live: both layers toggle, follow rig band (or manual picker), arcs animate + are band-colored, PSK vs RBN distinguishable, clicking a receiver shows the RX report, windows change the data.

## Self-review notes
- **Spec coverage:** two toggles (T1/T10), PSK window (T2), RBN endpoint+filter+location (T3/T4/T5), normalizer+cap (T6), api (T7), poll+active-band (T8), arcs+click (T9), band picker (T10) — all spec sections covered.
- **Type consistency:** `RbnHeardMeReport` fields identical in Contracts (T5) and TS (T7); `HeardMeArc` produced by T6 consumed by T9; settings names identical across T1/T8/T9/T10.
- **Co-owned file:** GlobePlugin changes are additive (append to existing arc/marker arrays, branch existing handlers) — no rewrite.
