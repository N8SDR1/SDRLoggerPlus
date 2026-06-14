# Weather Alerts (Lightning + High Wind) — Design

**Date:** 2026-06-10 · Part of the [SDRLogger+ port roadmap](sdrloggerplus-port-roadmap.md)

## Purpose

Station-protection alerts: lightning proximity and high-wind warnings shown as a banner, replicating SDRLogger+ v1.08.2–v1.12 behavior (`main.py` lines 6010–6380). Sources: NWS (alerts + METAR), Ambient Weather PWS, Ecowitt PWS — user has all three.

## Reference behavior (SDRLogger+)

### Lightning (poll every 90 s; 30 s idle when disabled)
- Station location from grid square → lat/lon.
- Sources, each individually toggleable:
  1. **Blitzortung** — public strike feed; strikes within configured range (mi/km) → closest distance + bearing.
  2. **NOAA/NWS** — active severe-thunderstorm-type warnings for the point → warning headline.
  3. **Ambient Weather** — `rt.ambientweather.net/v1/devices?apiKey&applicationKey`, first device `lastData`: `lightning_distance` (miles → km), `lightning_hour` (count). Counts only when hour-count > 0.
  4. **Ecowitt** — same contract; `lightning_distance_km`, `lightning_hour` from the shared last-data fetch.
- Status: `active` when closest ≤ range or NOAA warning present; closest km/mi, compass direction (when bearing known — PWS sources have none), strikes_1hr total, contributing sources, last_update.

### High wind (poll every 120 s)
- Sources, individually toggleable:
  1. **NWS active alerts** for the point, filtered to: High Wind Warning / High Wind Watch / Wind Advisory / Extreme Wind Warning. `is_extreme` when High Wind Warning or Extreme Wind Warning.
  2. **NWS METAR** latest observation for a user-chosen station ICAO (e.g. KLUK); wind speed/gust normalized to mph from km/h / m/s unit codes.
  3. **Ambient** / 4. **Ecowitt** — `windspeedmph`, `windgustmph`, `winddir`.
- Take the **max** sustained and max gust across sources; direction from the source providing the max sustained.
- Severity (`_wind_severity`, thresholds in mph — internal math stays mph):
  - `extreme`: NWS is_extreme, or sustained ≥ thresh_sust+15, or gust ≥ thresh_gust+15
  - `high`: sustained ≥ thresh_sust, or gust ≥ thresh_gust, or any non-extreme NWS wind product active
  - `elevated`: sustained ≥ max(10, thresh_sust−10) or gust ≥ max(15, thresh_gust−10)
- Clear-cooldown: after conditions clear, suppress new `elevated`-only alerts for a configurable cooldown (minutes).
- v1.12 unit handling: status publishes **both** mph and kph values plus the user's display unit and thresholds, so the UI renders either unit without a second fetch.

### Shared
- Ecowitt last-data fetch shared between lightning and wind with a **30 s cache** (one API hit serves both pollers). Credentials: Application Key + API Key + Gateway MAC.
- Ambient credentials (API key + Application key) shared between lightning and wind.
- NWS requests carry a descriptive User-Agent (NWS API requirement).

## SDRLoggerPlus design

### Backend

`WeatherAlertService` (`IHostedService` singleton) running both pollers on `PeriodicTimer`s (90 s / 120 s). Internals split per source for testability:

```
Services/Weather/
  WeatherAlertService.cs      (orchestration, status state, SignalR push)
  NwsClient.cs                (alerts-for-point, METAR latest; typed results)
  AmbientWeatherClient.cs     (lastData fetch)
  EcowittClient.cs            (lastData fetch + 30s cache)
  BlitzortungClient.cs        (strike feed within radius)
  WindSeverity.cs             (pure: the tier function above)
```

All clients take `HttpClient` via `IHttpClientFactory` and are interface-backed for mocking. Station lat/lon from `StationSettings` (lat/lon if set, else grid→lat/lon — a grid-to-coords helper already exists for maps; reuse it).

Status pushed over SignalR (`WeatherStatusChanged`) on every poll **and** queryable:

- `GET /api/weather/lightning` and `GET /api/weather/wind` — field-for-field equivalents of SDRLogger+'s `/api/lightning_status` and `/api/wind_status` (camelCased), wind including both unit families + thresholds + display unit.

### Settings — `WeatherSettings` on `UserSettings`

```
Lightning: Enabled, UseBlitzortung, UseNws, UseAmbient, UseEcowitt, Range (default 50), RangeUnit ("mi"|"km")
Wind:      Enabled, UseNwsAlerts, UseNwsMetar, MetarStation (ICAO), UseAmbient, UseEcowitt,
           ThreshSustainedMph (default 30), ThreshGustMph (default 45), DisplayUnit ("mph"|"kph"),
           CooldownMinutes (default 20)
Credentials: AmbientApiKey, AmbientAppKey, EcowittAppKey, EcowittApiKey, EcowittMac
```

Thresholds stored in mph regardless of display unit (ported decision — keeps saved configs stable).

### Frontend

- **Banner**: a slim alert strip rendered by the existing `StatusBar`/header area — ⚡ lightning (distance, direction, strikes/hr, NOAA headline) and 💨 wind (`G72kph 48kph WSW`-style text in the chosen unit), colored by severity (yellow/orange/red). Dismiss hides until status changes.
- **Settings**: "Weather Alerts" section with Lightning and High Wind subsections mirroring SDRLogger+'s tabs, including live-rescaling threshold labels when the display unit changes (convert displayed numbers; stored mph values unchanged).

## Error handling

- Any source failing → excluded from that cycle, others still aggregate; errors logged at debug, never crash the poller.
- Missing credentials for an enabled PWS source → source skipped, status notes nothing (matches SDRLogger+); settings UI shows a hint when a source is enabled without credentials.
- Rate limiting (Ambient 429) → treated as a failed cycle for that source.

## Testing

- Unit: `WindSeverity` table-driven across all tiers + NWS flag combinations; clear-cooldown suppression; max-across-sources aggregation; METAR unit normalization (km/h, m/s, fallback); Ecowitt 30 s cache (two callers, one HTTP hit); mph↔kph mirroring.
- Integration: pollers against mocked HTTP handlers returning canned NWS/Ambient/Ecowitt JSON; assert status DTO and SignalR push.
- Live validation deferred to user (real API keys).
