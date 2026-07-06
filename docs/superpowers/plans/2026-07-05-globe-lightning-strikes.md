# Globe Lightning Strikes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Plot live Blitzortung lightning strikes on the existing 3D globe as animated expanding rings, fed by the backend, two-tier (local fast / global slow), toggleable and off by default.

**Architecture:** Backend `BlitzortungClient` gains a coordinate-returning fetch; a new `LightningStrikeService` (hosted) polls local regions fast + all regions slow, dedupes/prunes/caps via a pure `StrikeBuffer`, and pushes new strikes over SignalR while exposing the current set via REST. The frontend keeps a pure rolling buffer and feeds globe.gl's unused `ringsData` layer, gated by `settings.map.showLightning`.

**Tech Stack:** .NET 10 (ASP.NET Core, SignalR, `RichardSzalay.MockHttp` for tests), React 18 + TypeScript, Zustand, globe.gl, Vitest.

## Global Constraints

- Base branch: `feat/globe-lightning-strikes` off `v2-alpha` @ `f20ce6c`. Do not merge without re-syncing (`GlobePlugin.tsx` is co-owned with Rick).
- Strike ring color: **electric cyan/white for local, dimmer for global — never yellow** (Rick's DX-tower rings are yellow).
- Toggle `settings.map.showLightning` defaults **false**; backend only polls Blitzortung while it is enabled.
- Poll cadence: local regions every **60 s**, all regions every **300 s** (tunable constants). Retention: local **5 min**, global **10 min**. Safety cap **2000** strikes, trimming **farthest-from-station first, always keeping local**.
- Local tier = Blitzortung regions **07, 12, 13** (Americas); global tier = the remaining regions (**1–14 excluding 07/12/13**), so the two tiers never overlap.
- No aggregation, no radar, no 2D map, no day/night (later phases). No strike persistence across restarts.
- Backend tests run: `dotnet test src/SDRLoggerPlus.Server.Tests --filter "Category=Unit"`. Frontend tests run from `src/SDRLoggerPlus.Web`: `npx vitest run <file>`.

---

### Task 1: Contracts — strike records + hub event

**Files:**
- Modify: `src/SDRLoggerPlus.Contracts/Events/LogEvents.cs`
- Modify: `src/SDRLoggerPlus.Server/Hubs/LogHub.cs`

**Interfaces:**
- Produces: `LightningStrike(double Lat, double Lon, DateTime TimestampUtc, bool Local)`, `LightningStrikesEvent(IReadOnlyList<LightningStrike> Strikes)`; `ILogHubClient.OnLightningStrikes`; `LogHubExtensions.BroadcastLightningStrikes`.

- [ ] **Step 1: Add the records**

Append to `src/SDRLoggerPlus.Contracts/Events/LogEvents.cs`:

```csharp
/// <summary>A single lightning strike for the globe display. Local = in the operator's
/// fast-refresh region tier (vs. the slower global tier).</summary>
public record LightningStrike(double Lat, double Lon, DateTime TimestampUtc, bool Local = false);

/// <summary>A batch of newly-observed lightning strikes pushed to clients.</summary>
public record LightningStrikesEvent(IReadOnlyList<LightningStrike> Strikes);
```

- [ ] **Step 2: Add the hub client method + broadcast extension**

In `src/SDRLoggerPlus.Server/Hubs/LogHub.cs`, add to the `ILogHubClient` interface (next to `OnLightningStatus`):

```csharp
    Task OnLightningStrikes(LightningStrikesEvent evt);
```

And add to the `LogHubExtensions` static class:

```csharp
    public static async Task BroadcastLightningStrikes(this IHubContext<LogHub, ILogHubClient> hub, LightningStrikesEvent evt)
    {
        await hub.Clients.All.OnLightningStrikes(evt);
    }
```

- [ ] **Step 3: Verify it compiles**

Run: `dotnet build src/SDRLoggerPlus.Server`
Expected: build succeeds.

- [ ] **Step 4: Commit**

```bash
git add src/SDRLoggerPlus.Contracts/Events/LogEvents.cs src/SDRLoggerPlus.Server/Hubs/LogHub.cs
git commit -m "feat(lightning): strike contract records + hub event"
```

---

### Task 2: BlitzortungClient.GetStrikesRawAsync

**Files:**
- Modify: `src/SDRLoggerPlus.Server/Services/Weather/WeatherClients.cs`
- Test: `src/SDRLoggerPlus.Server.Tests/Tests/Services/BlitzortungStrikesTests.cs`

**Interfaces:**
- Consumes: `LightningStrike` (Task 1).
- Produces: `IBlitzortungClient.GetStrikesRawAsync(IEnumerable<int> regions, CancellationToken)` → `List<LightningStrike>` (Local=false; the service tags Local later).

- [ ] **Step 1: Write the failing test**

Create `src/SDRLoggerPlus.Server.Tests/Tests/Services/BlitzortungStrikesTests.cs`:

```csharp
using System.Net;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using RichardSzalay.MockHttp;
using SDRLoggerPlus.Server.Services.Weather;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

[Trait("Category", "Unit")]
public class BlitzortungStrikesTests
{
    private static IHttpClientFactory Factory(MockHttpMessageHandler handler)
    {
        var mock = new Moq.Mock<IHttpClientFactory>();
        mock.Setup(f => f.CreateClient(It.IsAny<string>())).Returns(() => handler.ToHttpClient());
        return mock.Object;
    }

    [Fact]
    public async Task GetStrikesRawAsync_parses_coords_and_timestamp_and_skips_malformed()
    {
        var handler = new MockHttpMessageHandler();
        // Flat arrays: [lon, lat, timestamp(ns), ...]; last row malformed (too short).
        handler.When("*getjson.php*")
            .Respond("application/json", "[[-91.6,44.8,1751690000000000000,0],[2.3,48.9,1751690001000000000,0],[999]]");

        var client = new BlitzortungClient(Factory(handler), NullLogger<BlitzortungClient>.Instance);
        var strikes = await client.GetStrikesRawAsync(new[] { 7 }, CancellationToken.None);

        strikes.Should().HaveCount(2);
        strikes[0].Lat.Should().BeApproximately(44.8, 1e-6);
        strikes[0].Lon.Should().BeApproximately(-91.6, 1e-6);
        strikes[0].Local.Should().BeFalse();
        strikes[0].TimestampUtc.Year.Should().Be(2025); // 1.7518e18 ns → 2025
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `dotnet test src/SDRLoggerPlus.Server.Tests --filter "FullyQualifiedName~BlitzortungStrikesTests"`
Expected: FAIL — `GetStrikesRawAsync` not defined.

- [ ] **Step 3: Implement**

In `src/SDRLoggerPlus.Server/Services/Weather/WeatherClients.cs`, add to the `IBlitzortungClient` interface:

```csharp
    Task<List<LightningStrike>> GetStrikesRawAsync(IEnumerable<int> regions, CancellationToken ct = default);
```

And to the `BlitzortungClient` class (reuse the existing per-region fetch style; note the existing code builds a client with the Referer/User-Agent headers):

```csharp
    public async Task<List<LightningStrike>> GetStrikesRawAsync(IEnumerable<int> regions, CancellationToken ct = default)
    {
        var strikes = new List<LightningStrike>();
        foreach (var region in regions)
        {
            try
            {
                var client = _httpClientFactory.CreateClient();
                client.Timeout = TimeSpan.FromSeconds(8);
                client.DefaultRequestHeaders.Add("Referer", "https://map.blitzortung.org/");
                client.DefaultRequestHeaders.Add("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) SDRLoggerPlus");
                var json = await client.GetStringAsync(
                    $"https://map.blitzortung.org/GEOjson/getjson.php?f=s&n={region:D2}", ct);
                using var doc = JsonDocument.Parse(json);
                if (doc.RootElement.ValueKind != JsonValueKind.Array) continue;
                foreach (var item in doc.RootElement.EnumerateArray())
                {
                    // Flat arrays: [lon, lat, timestamp(ns since epoch), ...]
                    if (item.ValueKind != JsonValueKind.Array || item.GetArrayLength() < 3) continue;
                    if (item[0].ValueKind != JsonValueKind.Number || item[1].ValueKind != JsonValueKind.Number
                        || item[2].ValueKind != JsonValueKind.Number) continue;
                    var lon = item[0].GetDouble();
                    var lat = item[1].GetDouble();
                    var ns = item[2].GetInt64();
                    var ts = DateTimeOffset.FromUnixTimeMilliseconds(ns / 1_000_000).UtcDateTime;
                    strikes.Add(new LightningStrike(lat, lon, ts, Local: false));
                }
            }
            catch (Exception ex)
            {
                _logger.LogDebug("Blitzortung raw region {Region} error: {Error}", region, ex.Message);
            }
        }
        return strikes;
    }
```

Add `using SDRLoggerPlus.Contracts.Events;` at the top of the file if not present.

- [ ] **Step 4: Run test to verify it passes**

Run: `dotnet test src/SDRLoggerPlus.Server.Tests --filter "FullyQualifiedName~BlitzortungStrikesTests"`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/SDRLoggerPlus.Server/Services/Weather/WeatherClients.cs src/SDRLoggerPlus.Server.Tests/Tests/Services/BlitzortungStrikesTests.cs
git commit -m "feat(lightning): BlitzortungClient.GetStrikesRawAsync returns coordinates"
```

---

### Task 3: StrikeBuffer (pure) + LightningStrikeService (hosted)

**Files:**
- Create: `src/SDRLoggerPlus.Server/Services/Weather/StrikeBuffer.cs`
- Create: `src/SDRLoggerPlus.Server/Services/Weather/LightningStrikeService.cs`
- Test: `src/SDRLoggerPlus.Server.Tests/Tests/Services/StrikeBufferTests.cs`

**Interfaces:**
- Consumes: `LightningStrike` (Task 1), `IBlitzortungClient.GetStrikesRawAsync` (Task 2), `ISettingsService`, `IHubContext<LogHub, ILogHubClient>`.
- Produces: `StrikeBuffer` (pure); `LightningStrikeService` with `IReadOnlyList<LightningStrike> GetCurrent()`.

- [ ] **Step 1: Write the failing test (StrikeBuffer)**

Create `src/SDRLoggerPlus.Server.Tests/Tests/Services/StrikeBufferTests.cs`:

```csharp
using FluentAssertions;
using SDRLoggerPlus.Contracts.Events;
using SDRLoggerPlus.Server.Services.Weather;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

[Trait("Category", "Unit")]
public class StrikeBufferTests
{
    private static readonly DateTime T0 = new(2026, 7, 5, 12, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void Add_returns_only_new_strikes_and_dedupes()
    {
        var buf = new StrikeBuffer(localWindow: TimeSpan.FromMinutes(5), globalWindow: TimeSpan.FromMinutes(10), cap: 2000);
        var a = new LightningStrike(44.8, -91.6, T0, Local: true);
        var added1 = buf.Add(new[] { a }, T0, stationLat: 44.8, stationLon: -91.6);
        var added2 = buf.Add(new[] { a }, T0, 44.8, -91.6); // same strike again

        added1.Should().HaveCount(1);
        added2.Should().BeEmpty();
        buf.Current(T0).Should().HaveCount(1);
    }

    [Fact]
    public void Current_prunes_by_window_local_longer_than_global()
    {
        var buf = new StrikeBuffer(TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(10), 2000);
        var local = new LightningStrike(44.8, -91.6, T0, Local: true);
        var global = new LightningStrike(0, 100, T0, Local: false);
        buf.Add(new[] { local, global }, T0, 44.8, -91.6);

        // 7 min later: global window (10m) keeps it, local window (5m) drops the local one.
        var later = buf.Current(T0.AddMinutes(7));
        later.Should().ContainSingle(s => !s.Local);
        later.Should().NotContain(s => s.Local);
    }

    [Fact]
    public void Cap_trims_farthest_first_and_always_keeps_local()
    {
        var buf = new StrikeBuffer(TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(10), cap: 2);
        var near = new LightningStrike(44.9, -91.6, T0, Local: true);   // ~11 km
        var mid = new LightningStrike(50.0, -91.6, T0, Local: false);   // ~580 km
        var far = new LightningStrike(0.0, 100.0, T0, Local: false);    // ~half a world
        buf.Add(new[] { near, mid, far }, T0, 44.8, -91.6);

        var current = buf.Current(T0);
        current.Should().HaveCount(2);
        current.Should().Contain(near);       // local always kept
        current.Should().Contain(mid);        // nearer of the two globals
        current.Should().NotContain(far);     // farthest trimmed
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `dotnet test src/SDRLoggerPlus.Server.Tests --filter "FullyQualifiedName~StrikeBufferTests"`
Expected: FAIL — `StrikeBuffer` not defined.

- [ ] **Step 3: Implement StrikeBuffer**

Create `src/SDRLoggerPlus.Server/Services/Weather/StrikeBuffer.cs`:

```csharp
using SDRLoggerPlus.Contracts.Events;

namespace SDRLoggerPlus.Server.Services.Weather;

/// <summary>
/// Pure rolling buffer of lightning strikes. Dedupes, prunes by age (local window
/// vs. global window), and caps total size by trimming the farthest-from-station
/// strikes first while always keeping local ones. No IO, no timers — unit-tested.
/// </summary>
public sealed class StrikeBuffer
{
    private readonly TimeSpan _localWindow;
    private readonly TimeSpan _globalWindow;
    private readonly int _cap;
    private readonly Dictionary<string, LightningStrike> _byKey = new();

    public StrikeBuffer(TimeSpan localWindow, TimeSpan globalWindow, int cap)
    {
        _localWindow = localWindow;
        _globalWindow = globalWindow;
        _cap = cap;
    }

    private static string Key(LightningStrike s) =>
        $"{s.Lat:F4}|{s.Lon:F4}|{s.TimestampUtc.Ticks}";

    /// <summary>Add strikes; returns only the ones not already present (for pushing).</summary>
    public IReadOnlyList<LightningStrike> Add(IEnumerable<LightningStrike> incoming, DateTime nowUtc, double? stationLat, double? stationLon)
    {
        var added = new List<LightningStrike>();
        foreach (var s in incoming)
        {
            var key = Key(s);
            if (_byKey.ContainsKey(key)) continue;
            _byKey[key] = s;
            added.Add(s);
        }
        Prune(nowUtc, stationLat, stationLon);
        return added;
    }

    /// <summary>Current pruned snapshot (does not mutate).</summary>
    public IReadOnlyList<LightningStrike> Current(DateTime nowUtc)
    {
        return _byKey.Values.Where(s => !IsExpired(s, nowUtc)).ToList();
    }

    private bool IsExpired(LightningStrike s, DateTime nowUtc)
    {
        var window = s.Local ? _localWindow : _globalWindow;
        return nowUtc - s.TimestampUtc > window;
    }

    private void Prune(DateTime nowUtc, double? stationLat, double? stationLon)
    {
        // Drop expired.
        foreach (var kv in _byKey.Where(kv => IsExpired(kv.Value, nowUtc)).ToList())
            _byKey.Remove(kv.Key);

        if (_byKey.Count <= _cap) return;

        // Over cap: keep all local; among globals keep the nearest to the station.
        var locals = _byKey.Values.Where(s => s.Local).ToList();
        var globals = _byKey.Values.Where(s => !s.Local).ToList();
        if (stationLat.HasValue && stationLon.HasValue)
            globals = globals.OrderBy(s => Haversine(stationLat.Value, stationLon.Value, s.Lat, s.Lon)).ToList();
        else
            globals = globals.OrderByDescending(s => s.TimestampUtc).ToList();

        var keepGlobals = Math.Max(0, _cap - locals.Count);
        var kept = locals.Concat(globals.Take(keepGlobals));
        _byKey.Clear();
        foreach (var s in kept) _byKey[Key(s)] = s;
    }

    private static double Haversine(double lat1, double lon1, double lat2, double lon2)
    {
        const double R = 6371;
        double ToRad(double d) => d * Math.PI / 180;
        var dLat = ToRad(lat2 - lat1);
        var dLon = ToRad(lon2 - lon1);
        var a = Math.Sin(dLat / 2) * Math.Sin(dLat / 2)
              + Math.Cos(ToRad(lat1)) * Math.Cos(ToRad(lat2)) * Math.Sin(dLon / 2) * Math.Sin(dLon / 2);
        return R * 2 * Math.Atan2(Math.Sqrt(a), Math.Sqrt(1 - a));
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `dotnet test src/SDRLoggerPlus.Server.Tests --filter "FullyQualifiedName~StrikeBufferTests"`
Expected: PASS — 3 passing.

- [ ] **Step 5: Implement LightningStrikeService**

Create `src/SDRLoggerPlus.Server/Services/Weather/LightningStrikeService.cs`:

```csharp
using Microsoft.AspNetCore.SignalR;
using SDRLoggerPlus.Contracts.Events;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Hubs;

namespace SDRLoggerPlus.Server.Services.Weather;

/// <summary>
/// Polls Blitzortung on two cadences — local regions (Americas) fast, all regions
/// slow — only while the globe lightning toggle is enabled. Feeds a StrikeBuffer
/// and pushes new strikes over SignalR. Ephemeral; nothing is persisted.
/// </summary>
public class LightningStrikeService : BackgroundService
{
    private static readonly int[] LocalRegions = { 7, 12, 13 };
    private static readonly int[] GlobalRegions = Enumerable.Range(1, 14).Except(LocalRegions).ToArray();
    private static readonly TimeSpan LocalInterval = TimeSpan.FromSeconds(60);
    private static readonly TimeSpan GlobalInterval = TimeSpan.FromSeconds(300);

    private readonly IServiceProvider _serviceProvider;
    private readonly IBlitzortungClient _blitzortung;
    private readonly IHubContext<LogHub, ILogHubClient>? _hubContext;
    private readonly ILogger<LightningStrikeService> _logger;
    private readonly StrikeBuffer _buffer = new(TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(10), cap: 2000);
    private readonly object _lock = new();

    public LightningStrikeService(
        IServiceProvider serviceProvider,
        IBlitzortungClient blitzortung,
        ILogger<LightningStrikeService> logger,
        IHubContext<LogHub, ILogHubClient>? hubContext = null)
    {
        _serviceProvider = serviceProvider;
        _blitzortung = blitzortung;
        _hubContext = hubContext;
        _logger = logger;
    }

    public IReadOnlyList<LightningStrike> GetCurrent()
    {
        lock (_lock) return _buffer.Current(DateTime.UtcNow);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var lastLocal = DateTime.MinValue;
        var lastGlobal = DateTime.MinValue;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var settings = await ReadSettingsAsync();
                if (settings.Map.ShowLightning)
                {
                    var now = DateTime.UtcNow;
                    var (stationLat, stationLon) = ResolveStation(settings);

                    if (now - lastLocal >= LocalInterval)
                    {
                        lastLocal = now;
                        await PollAsync(LocalRegions, local: true, stationLat, stationLon, stoppingToken);
                    }
                    if (now - lastGlobal >= GlobalInterval)
                    {
                        lastGlobal = now;
                        await PollAsync(GlobalRegions, local: false, stationLat, stationLon, stoppingToken);
                    }
                }
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex) { _logger.LogWarning(ex, "Lightning strike poll error"); }

            try { await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken); }
            catch (OperationCanceledException) { break; }
        }
    }

    private async Task PollAsync(int[] regions, bool local, double? stationLat, double? stationLon, CancellationToken ct)
    {
        var raw = await _blitzortung.GetStrikesRawAsync(regions, ct);
        // Local and global region sets are disjoint, so the whole batch shares this tier's tag.
        var tagged = raw.Select(s => s with { Local = local }).ToList();
        IReadOnlyList<LightningStrike> added;
        lock (_lock) added = _buffer.Add(tagged, DateTime.UtcNow, stationLat, stationLon);
        if (added.Count > 0 && _hubContext != null)
            await _hubContext.BroadcastLightningStrikes(new LightningStrikesEvent(added));
    }

    private static (double?, double?) ResolveStation(UserSettings settings)
    {
        var st = settings.Station;
        if (st?.Latitude is double lat && st?.Longitude is double lon) return (lat, lon);
        return (null, null);
    }

    private async Task<UserSettings> ReadSettingsAsync()
    {
        using var scope = _serviceProvider.CreateScope();
        var settingsService = scope.ServiceProvider.GetRequiredService<ISettingsService>();
        return await settingsService.GetSettingsAsync();
    }
}
```

> If `UserSettings.Station.Latitude/Longitude` differ in name/type from `double?`, adapt `ResolveStation` to the real shape (check `Contracts/Models/Settings.cs`) — the rest is unaffected.

- [ ] **Step 6: Run the full unit suite**

Run: `dotnet test src/SDRLoggerPlus.Server.Tests --filter "Category=Unit"`
Expected: PASS (all existing + StrikeBuffer + Blitzortung tests).

- [ ] **Step 7: Commit**

```bash
git add src/SDRLoggerPlus.Server/Services/Weather/StrikeBuffer.cs src/SDRLoggerPlus.Server/Services/Weather/LightningStrikeService.cs src/SDRLoggerPlus.Server.Tests/Tests/Services/StrikeBufferTests.cs
git commit -m "feat(lightning): StrikeBuffer + two-tier LightningStrikeService"
```

---

### Task 4: Backend wiring — setting, endpoint, registration

**Files:**
- Modify: `src/SDRLoggerPlus.Contracts/Models/Settings.cs`
- Modify: `src/SDRLoggerPlus.Server/Controllers/WeatherController.cs`
- Modify: `src/SDRLoggerPlus.Server/Program.cs`

**Interfaces:**
- Consumes: `LightningStrikeService.GetCurrent()` (Task 3).
- Produces: `GET /api/weather/lightning/strikes`; `MapSettings.ShowLightning`.

- [ ] **Step 1: Add the setting**

In `src/SDRLoggerPlus.Contracts/Models/Settings.cs`, in `class MapSettings` (next to `ShowPotaOverlay`):

```csharp
    [BsonElement("showLightning")]
    public bool ShowLightning { get; set; }
```

- [ ] **Step 2: Register the service**

In `src/SDRLoggerPlus.Server/Program.cs`, next to the `WeatherAlertService` registration:

```csharp
// Globe lightning strikes (own poller; only polls while the map toggle is enabled)
builder.Services.AddSingleton<LightningStrikeService>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<LightningStrikeService>());
```

- [ ] **Step 3: Add the endpoint**

In `src/SDRLoggerPlus.Server/Controllers/WeatherController.cs`, inject the service and add the route:

```csharp
    private readonly WeatherAlertService _weatherService;
    private readonly LightningStrikeService _strikeService;

    public WeatherController(WeatherAlertService weatherService, LightningStrikeService strikeService)
    {
        _weatherService = weatherService;
        _strikeService = strikeService;
    }

    [HttpGet("lightning/strikes")]
    public ActionResult<IReadOnlyList<SDRLoggerPlus.Contracts.Events.LightningStrike>> GetLightningStrikes()
        => Ok(_strikeService.GetCurrent());
```

(Keep the existing `GetLightning`/`GetWind` actions.)

- [ ] **Step 4: Verify build + full unit suite**

Run: `dotnet build src/SDRLoggerPlus.Server && dotnet test src/SDRLoggerPlus.Server.Tests --filter "Category=Unit"`
Expected: build succeeds; tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/SDRLoggerPlus.Contracts/Models/Settings.cs src/SDRLoggerPlus.Server/Controllers/WeatherController.cs src/SDRLoggerPlus.Server/Program.cs
git commit -m "feat(lightning): showLightning setting, strikes endpoint, service registration"
```

---

### Task 5: Frontend pure module `lightningStrikes.ts`

**Files:**
- Create: `src/SDRLoggerPlus.Web/src/utils/lightningStrikes.ts`
- Test: `src/SDRLoggerPlus.Web/src/utils/lightningStrikes.test.ts`

**Interfaces:**
- Produces:
  - `interface Strike { lat: number; lon: number; timestampUtc: string; local: boolean }`
  - `class StrikeStore { merge(strikes: Strike[]): void; active(nowMs: number): Strike[] }`
  - Ring lifetime: local strikes shown 5 min, global 10 min (mirrors the backend window; client prunes for rendering).

- [ ] **Step 1: Write the failing test**

Create `src/SDRLoggerPlus.Web/src/utils/lightningStrikes.test.ts`:

```ts
import { describe, it, expect } from 'vitest';
import { StrikeStore, type Strike } from './lightningStrikes';

const iso = (ms: number) => new Date(ms).toISOString();

describe('StrikeStore', () => {
  it('merges and dedupes by lat/lon/time', () => {
    const store = new StrikeStore();
    const s: Strike = { lat: 44.8, lon: -91.6, timestampUtc: iso(1_000_000), local: true };
    store.merge([s]);
    store.merge([s]);
    expect(store.active(1_000_000)).toHaveLength(1);
  });

  it('prunes local after 5 min, global after 10 min', () => {
    const store = new StrikeStore();
    const t = 1_000_000;
    store.merge([
      { lat: 44.8, lon: -91.6, timestampUtc: iso(t), local: true },
      { lat: 0, lon: 100, timestampUtc: iso(t), local: false },
    ]);
    const at7min = store.active(t + 7 * 60_000);
    expect(at7min.filter(s => s.local)).toHaveLength(0);   // local pruned
    expect(at7min.filter(s => !s.local)).toHaveLength(1);  // global still shown
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd src/SDRLoggerPlus.Web && npx vitest run src/utils/lightningStrikes.test.ts`
Expected: FAIL — module not found.

- [ ] **Step 3: Implement**

Create `src/SDRLoggerPlus.Web/src/utils/lightningStrikes.ts`:

```ts
export interface Strike {
  lat: number;
  lon: number;
  timestampUtc: string;
  local: boolean;
}

const LOCAL_WINDOW_MS = 5 * 60_000;
const GLOBAL_WINDOW_MS = 10 * 60_000;

const key = (s: Strike) => `${s.lat.toFixed(4)}|${s.lon.toFixed(4)}|${s.timestampUtc}`;

/** Client-side rolling set of strikes for the globe. Dedupes and prunes by age. */
export class StrikeStore {
  private byKey = new Map<string, Strike>();

  merge(strikes: Strike[]): void {
    for (const s of strikes) {
      const k = key(s);
      if (!this.byKey.has(k)) this.byKey.set(k, s);
    }
  }

  /** Non-expired strikes as of nowMs (also drops expired from the store). */
  active(nowMs: number): Strike[] {
    const out: Strike[] = [];
    for (const [k, s] of this.byKey) {
      const age = nowMs - Date.parse(s.timestampUtc);
      const window = s.local ? LOCAL_WINDOW_MS : GLOBAL_WINDOW_MS;
      if (age > window) this.byKey.delete(k);
      else out.push(s);
    }
    return out;
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd src/SDRLoggerPlus.Web && npx vitest run src/utils/lightningStrikes.test.ts`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/SDRLoggerPlus.Web/src/utils/lightningStrikes.ts src/SDRLoggerPlus.Web/src/utils/lightningStrikes.test.ts
git commit -m "feat(lightning): frontend rolling strike store"
```

---

### Task 6: Frontend wiring — SignalR event, REST, setting

**Files:**
- Modify: `src/SDRLoggerPlus.Web/src/api/signalr.ts`
- Modify: `src/SDRLoggerPlus.Web/src/api/client.ts`
- Modify: `src/SDRLoggerPlus.Web/src/store/settingsStore.ts`

**Interfaces:**
- Consumes: `Strike` (Task 5).
- Produces: `setLightningStrikesCallback`/`clearLightningStrikesCallback`; `api.getLightningStrikes()`; `settings.map.showLightning`.

- [ ] **Step 1: SignalR event wiring**

In `src/SDRLoggerPlus.Web/src/api/signalr.ts`:

(a) Add a module-scoped callback + setters, mirroring `setTciMetersCallback` (near line 612):

```ts
export interface LightningStrikeMsg { lat: number; lon: number; timestampUtc: string; local: boolean }
export interface LightningStrikesEvent { strikes: LightningStrikeMsg[] }

let lightningStrikesCallback: ((evt: LightningStrikesEvent) => void) | null = null;
export function setLightningStrikesCallback(cb: ((evt: LightningStrikesEvent) => void) | null): void {
  lightningStrikesCallback = cb;
}
export function clearLightningStrikesCallback(cb: (evt: LightningStrikesEvent) => void): void {
  if (lightningStrikesCallback === cb) lightningStrikesCallback = null;
}
```

(b) Register the hub handler alongside the other `this.connection.on(...)` calls (near line 902):

```ts
    this.connection.on('OnLightningStrikes', (evt: LightningStrikesEvent) => {
      lightningStrikesCallback?.(evt);
    });
```

- [ ] **Step 2: REST method**

In `src/SDRLoggerPlus.Web/src/api/client.ts`, next to `getLightningStatus`:

```ts
  async getLightningStrikes(): Promise<{ lat: number; lon: number; timestampUtc: string; local: boolean }[]> {
    return this.fetch('/weather/lightning/strikes');
  }
```

- [ ] **Step 3: Setting**

In `src/SDRLoggerPlus.Web/src/store/settingsStore.ts`: add `showLightning: boolean;` to the `MapSettings` interface (next to `showPotaOverlay`), and `showLightning: false,` to the map defaults block (next to `showPotaOverlay: false,`).

- [ ] **Step 4: Typecheck**

Run: `cd src/SDRLoggerPlus.Web && npx tsc --noEmit`
Expected: no new errors (pre-existing `GlobePlugin.tsx` errors from `v2-alpha` may remain — do not fix here; if any OTHER file errors, fix it).

- [ ] **Step 5: Commit**

```bash
git add src/SDRLoggerPlus.Web/src/api/signalr.ts src/SDRLoggerPlus.Web/src/api/client.ts src/SDRLoggerPlus.Web/src/store/settingsStore.ts
git commit -m "feat(lightning): frontend SignalR event, REST, showLightning setting"
```

---

### Task 7: Globe integration (manual-verified)

**Files:**
- Modify: `src/SDRLoggerPlus.Web/src/plugins/GlobePlugin.tsx`

**Interfaces:**
- Consumes: `StrikeStore` (Task 5); `setLightningStrikesCallback`/`clearLightningStrikesCallback` (Task 6); `api.getLightningStrikes` (Task 6); `settings.map.showLightning` (Task 6).

- [ ] **Step 1: Extend the GlobeInstance type**

In `src/SDRLoggerPlus.Web/src/plugins/GlobePlugin.tsx`, add to the `interface GlobeInstance` (next to `ringsData`):

```ts
  ringColor(accessor: (d: unknown) => (t: number) => string): GlobeInstance;
  ringMaxRadius(accessor: number | ((d: unknown) => number)): GlobeInstance;
  ringPropagationSpeed(accessor: number | ((d: unknown) => number)): GlobeInstance;
  ringRepeatPeriod(accessor: number | ((d: unknown) => number)): GlobeInstance;
  ringAltitude(accessor: number | ((d: unknown) => number)): GlobeInstance;
  ringResolution(res: number): GlobeInstance;
```

- [ ] **Step 2: Configure the ring layer on globe init**

In the globe-init chain (after the `arcsData` config block, before `globeRef.current = globe`), add:

```ts
      // Lightning strike rings — the strike layer owns ringsData (see the strike
      // effect + the removed per-frame clear in renderBeam). Cyan/white local,
      // dimmer for global; deliberately not yellow (that's the DX-tower pulse).
      globe
        .ringColor((d: unknown) => {
          const s = d as { local: boolean };
          const base = s.local ? '120, 230, 255' : '120, 180, 220'; // cyan-white / dim
          return (t: number) => `rgba(${base}, ${Math.max(0, 1 - t).toFixed(3)})`;
        })
        .ringMaxRadius((d: unknown) => ((d as { local: boolean }).local ? 3.5 : 2.5))
        .ringPropagationSpeed(2)
        .ringRepeatPeriod(0)   // single ripple per strike, no repeat
        .ringAltitude(0.006)
        .ringResolution(64)
        .ringsData([]);
```

- [ ] **Step 3: Remove the per-frame ringsData clear in renderBeam**

In `renderBeam`, the final chained call currently ends `.ringsData([])`. Delete **only** that `.ringsData([])` line (the strike effect now owns the layer). Leave the rest of the `renderBeam` chain intact.

- [ ] **Step 4: Add the strike effect**

Add these imports at the top:

```ts
import { StrikeStore, type Strike } from '../utils/lightningStrikes';
import { setLightningStrikesCallback, clearLightningStrikesCallback } from '../api/signalr';
```

Add a strike-store ref near the other refs in `GlobeCore`:

```ts
  const strikeStoreRef = useRef(new StrikeStore());
```

Add this effect (after the marker effect), gated on the setting and `globeReady`:

```ts
  // Lightning strikes → globe.gl ringsData. Backfill via REST, then live via SignalR.
  useEffect(() => {
    if (!globeReady || !globeRef.current) return;
    if (!settings.map.showLightning) {
      globeRef.current.ringsData([]);
      return;
    }
    const store = strikeStoreRef.current;
    let cancelled = false;

    const render = () => {
      if (cancelled || !globeRef.current) return;
      const active = store.active(Date.now());
      globeRef.current.ringsData(active.map(s => ({ lat: s.lat, lng: s.lon, local: s.local })));
    };

    // Initial backfill.
    api.getLightningStrikes().then(list => {
      if (cancelled) return;
      store.merge(list as Strike[]);
      render();
    }).catch(() => { /* best-effort */ });

    // Live updates.
    const cb = (evt: { strikes: Strike[] }) => { store.merge(evt.strikes); render(); };
    setLightningStrikesCallback(cb);

    // Age-out sweep so rings fade even without new strikes arriving.
    const interval = setInterval(render, 2000);

    return () => {
      cancelled = true;
      clearInterval(interval);
      clearLightningStrikesCallback(cb);
      if (globeRef.current) globeRef.current.ringsData([]);
    };
  }, [globeReady, settings.map.showLightning]);
```

- [ ] **Step 5: Add the overlay toggle button**

Find the globe overlay controls (where the Pause/Play and other buttons render). Add a lightning toggle button mirroring the existing overlay-button style, wired to `updateMapSettings({ showLightning: !settings.map.showLightning })` (import `Zap` from `lucide-react`, and `useSettingsStore`'s `updateMapSettings` if not already in scope). Active state highlights when `settings.map.showLightning` is true.

- [ ] **Step 6: Typecheck**

Run: `cd src/SDRLoggerPlus.Web && npx tsc --noEmit`
Expected: no NEW errors beyond the pre-existing `GlobePlugin.tsx` ones from `v2-alpha`.

- [ ] **Step 7: Manual verification (with dev servers running)**

1. Open `http://localhost:5173`, add/focus the **3D Globe** panel.
2. In Settings → Map (or the globe's lightning overlay button), enable **Show Lightning**.
3. Within ~60 s, cyan/white rings ripple at strike locations (backfill appears immediately if the backend has data). Confirm rings are cyan/white, **not** yellow, and distinct from the DX-tower pulse.
4. Toggle off → rings clear.
5. Backend log shows the lightning poller running only while enabled.

- [ ] **Step 8: Commit**

```bash
git add src/SDRLoggerPlus.Web/src/plugins/GlobePlugin.tsx
git commit -m "feat(lightning): render strikes on the globe ring layer with toggle"
```

---

## Self-Review Notes

- **Spec coverage:** two-tier fetch (Tasks 2–3), dedupe/window/cap keeping local (Task 3 `StrikeBuffer`), SignalR push + REST backfill (Tasks 1/3/4/6), toggle gating backend polling (Tasks 3–4) and frontend render (Task 7), cyan/not-yellow rings + `ringsData` ownership + removed per-frame clear + interface additions (Task 7), off-by-default setting (Tasks 4/6). Local-tier = Americas regions (Task 3) per the spec's stated simplification.
- **Type consistency:** `LightningStrike`(Lat/Lon/TimestampUtc/Local) backend Tasks 1–4; `Strike`(lat/lon/timestampUtc/local) frontend Tasks 5–7; `StrikeBuffer` (backend) vs `StrikeStore` (frontend) named distinctly on purpose. `GetStrikesRawAsync` signature identical in Tasks 2 and 3.
- **No placeholders:** every code/command step is concrete. Task 7 step 5 (toggle button) describes placement against existing overlay controls rather than pasting unseen surrounding JSX — the one spot requiring the implementer to match local markup.
