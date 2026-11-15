/**
 * DX Coach — Phase 2 propagation gate.
 *
 * A *soft*, honest annotation on a spot opportunity. Per the design
 * (docs/design/ai-dx-coach.md §3.2) it only ever makes two kinds of claim:
 *
 *   1. **Gray-line timing** — timed, geometric, and computable exactly from the
 *      terminator math already in the app ("DX sunset in ~22 min → low-band
 *      gray-line window").
 *   2. **Coarse band-open score** — open / marginal / closed from a great-circle
 *      MUF/LUF/reliability model driven by SFI + K-index.
 *
 * It never does VOACAP-grade forecasting, and it never *invents* a fact — the
 * facts (SFI, K, the DX centroid, the operator QTH) come from the engine; this
 * file only does deterministic geometry/arithmetic on them.
 *
 * The band-open math is a faithful port of the server's
 * SDRLoggerPlus.Server/Services/PropagationService.cs (SsnFromSfi / CalculatePathMuf
 * / CalculateLuf / CalculateReliability) and of IonosondeCalibration (live
 * ionosonde correction), so the Coach and the Propagation panel
 * agree. Solar geometry reuses utils/solarCalculations + utils/sunTimes.
 */
import { calculateDistance } from './maidenhead';
import { getSolarElevation } from './solarCalculations';
import { calculateSunTimes } from './sunTimes';

export type BandOpenState = 'open' | 'marginal' | 'closed' | 'unknown';

export interface GrayLineInfo {
  /** An endpoint of the path is in the gray line (solar elevation −6°…0°) right now. */
  active: boolean;
  /** 'you' if the operator end is in/approaching gray line, 'dx' for the DX end. */
  end: 'you' | 'dx';
  /** Minutes until the next sunrise/sunset at that end (0 when already active). */
  minutesToEvent: number;
  /** Short human label, e.g. "Gray line now (DX)" or "DX sunset ~22m". */
  label: string;
}

/** One live ionosonde measurement (server: IonosondeReading). */
export interface IonosondeReading {
  name: string;
  lat: number;
  lon: number;
  /** Measured F2-layer critical frequency, MHz. */
  foF2: number;
  mufD?: number | null;
  timeUtc: string;
}

/** Where a forecast's level came from (server: ForecastSource). */
export type ForecastSource = 'measured' | 'calibrated' | 'model';

export interface PropAssessment {
  open: BandOpenState;
  /** 0–99 path reliability, or null when we lack coordinates to compute it. */
  reliability: number | null;
  /** Path MUF (MHz) after any live calibration, or null when not computed. */
  mufMHz: number | null;
  /** Whether live ionosondes set the forecast's level. */
  source: ForecastSource;
  grayLine: GrayLineInfo | null;
  /** One-line summary suitable for a tooltip. */
  summary: string;
}

const toRad = (d: number) => (d * Math.PI) / 180;
const toDeg = (r: number) => (r * 180) / Math.PI;

/** Great-circle midpoint — ported from PropagationService.MidpointLatLon. */
export function midpoint(
  lat1: number,
  lon1: number,
  lat2: number,
  lon2: number,
): { lat: number; lon: number } {
  const lat1r = toRad(lat1);
  const lon1r = toRad(lon1);
  const lat2r = toRad(lat2);
  const dLon = toRad(lon2 - lon1);
  const bx = Math.cos(lat2r) * Math.cos(dLon);
  const by = Math.cos(lat2r) * Math.sin(dLon);
  const midLat = Math.atan2(
    Math.sin(lat1r) + Math.sin(lat2r),
    Math.sqrt((Math.cos(lat1r) + bx) * (Math.cos(lat1r) + bx) + by * by),
  );
  const midLon = lon1r + Math.atan2(by, Math.cos(lat1r) + bx);
  return { lat: toDeg(midLat), lon: toDeg(midLon) };
}

/** SSN from SFI — PropagationService.SsnFromSfi. */
export function ssnFromSfi(sfi: number): number {
  return Math.max(0, Math.round((sfi - 67) / 0.97));
}

/**
 * Maximum Usable Frequency (MHz) of a whole path — port of
 * PropagationService.CalculatePathMuf (ITU-R P.533 style): each control point's
 * foF2 (model × its live correction) times the hop M-factor; the weakest control
 * point limits the path. `factors` aligns with controlPoints(); omitted = model only.
 */
export function pathMuf(
  ssn: number,
  deLat: number,
  deLon: number,
  dxLat: number,
  dxLon: number,
  now: Date,
  factors?: number[],
): number {
  const distanceKm = calculateDistance(deLat, deLon, dxLat, dxLon);
  const hops = Math.max(1, Math.ceil(distanceKm / 3500));
  const m = hopMFactor(distanceKm / hops);
  const foF2 = Math.min(
    ...controlPoints(deLat, deLon, dxLat, dxLon).map((p, i) => estimateFoF2(ssn, p.lat, p.lon, now) * (factors?.[i] ?? 1)),
  );
  return Math.max(2.0, foF2 * m);
}

/** MUF ÷ foF2 for one hop of this length — PropagationService.HopMFactor. */
export function hopMFactor(hopKm: number): number {
  return 1.0 + 2.5 * Math.sin(toRad(Math.min(90, (hopKm / 3500) * 90)));
}

/** Model foF2 (MHz) from SSN, latitude and solar zenith — PropagationService.EstimateFoF2. */
export function foF2FromZenith(ssn: number, lat: number, zenithDeg: number): number {
  const ssnFactor = 4.0 + ssn * 0.035;
  const dayNightFactor =
    zenithDeg < 90
      ? Math.pow(Math.cos(toRad(zenithDeg)), 0.3)
      : 0.2 + 0.1 * Math.cos(toRad(Math.min(zenithDeg, 120) - 90));
  const latFactor = 1.0 + 0.3 * Math.cos(toRad(lat * 2));
  return ssnFactor * dayNightFactor * latFactor;
}

/** Model foF2 (MHz) at a point and moment. */
export function estimateFoF2(ssn: number, lat: number, lon: number, now: Date): number {
  return foF2FromZenith(ssn, lat, 90 - getSolarElevation(lat, lon, now));
}

/** Point a fraction f (0–1) of the way along the great circle — PropagationService.IntermediatePoint. */
export function intermediatePoint(lat1: number, lon1: number, lat2: number, lon2: number, f: number): { lat: number; lon: number } {
  const p1 = toRad(lat1), l1 = toRad(lon1), p2 = toRad(lat2), l2 = toRad(lon2);
  const delta = calculateDistance(lat1, lon1, lat2, lon2) / 6371;
  if (delta < 1e-9) return { lat: lat1, lon: lon1 };
  const a = Math.sin((1 - f) * delta) / Math.sin(delta);
  const b = Math.sin(f * delta) / Math.sin(delta);
  const x = a * Math.cos(p1) * Math.cos(l1) + b * Math.cos(p2) * Math.cos(l2);
  const y = a * Math.cos(p1) * Math.sin(l1) + b * Math.cos(p2) * Math.sin(l2);
  const z = a * Math.sin(p1) + b * Math.sin(p2);
  return { lat: toDeg(Math.atan2(z, Math.sqrt(x * x + y * y))), lon: toDeg(Math.atan2(y, x)) };
}

/** Midpoint for paths up to 4,000 km; else the points 2,000 km in from each end. */
export function controlPoints(lat1: number, lon1: number, lat2: number, lon2: number): { lat: number; lon: number }[] {
  const d = calculateDistance(lat1, lon1, lat2, lon2);
  if (d <= 4000) return [midpoint(lat1, lon1, lat2, lon2)];
  return [intermediatePoint(lat1, lon1, lat2, lon2, 2000 / d), intermediatePoint(lat1, lon1, lat2, lon2, 1 - 2000 / d)];
}

// Calibration constants — keep identical to IonosondeCalibration (C#).
const ION_NEAR_KM = 2000;
const ION_WEIGHT_SCALE_KM = 1500;
const ION_MAX_INFLUENCE_KM = 4000;
const ION_GLOBAL_WEIGHT = 0.2;

/**
 * Live correction per path control point — port of IonosondeCalibration.ForPath.
 * At each station factor = measured foF2 / model foF2; interpolated to each
 * control point (nearby stations weighted heavily, blended with the worldwide
 * median). All 1 / 'model' with no data.
 */
export function calibrationForPath(
  readings: IonosondeReading[],
  ssn: number,
  deLat: number,
  deLon: number,
  dxLat: number,
  dxLon: number,
  now: Date,
): { factors: number[]; source: ForecastSource } {
  const points = controlPoints(deLat, deLon, dxLat, dxLon);
  if (readings.length === 0) return { factors: points.map(() => 1), source: 'model' };

  const stations = readings.map((r) => ({
    r,
    k: Math.min(3, Math.max(0.5, r.foF2 / estimateFoF2(ssn, r.lat, r.lon, now))),
  }));
  const sorted = stations.map((s) => s.k).sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  const global = sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;

  let allNear = true;
  const factors = points.map((p) => {
    let wSum = ION_GLOBAL_WEIGHT;
    let kSum = ION_GLOBAL_WEIGHT * global;
    let nearest = Infinity;
    for (const { r, k } of stations) {
      const d = calculateDistance(p.lat, p.lon, r.lat, r.lon);
      nearest = Math.min(nearest, d);
      if (d > ION_MAX_INFLUENCE_KM) continue;
      const w = Math.exp(-Math.pow(d / ION_WEIGHT_SCALE_KM, 2));
      wSum += w;
      kSum += w * k;
    }
    if (nearest > ION_NEAR_KM) allNear = false;
    return kSum / wSum;
  });
  return { factors, source: allNear ? 'measured' : 'calibrated' };
}

/** Lowest Usable Frequency (MHz) — port of PropagationService.CalculateLuf. */
export function calculateLuf(
  ssn: number,
  distanceKm: number,
  zenithDeg: number,
  kIndex: number,
): number {
  const hops = Math.max(1, Math.ceil(distanceKm / 3500));
  // D-layer absorption rises with the sun and vanishes after dark; one continuous
  // formula, night floor below 160m so top band can open.
  const sun = zenithDeg < 90 ? Math.sqrt(Math.cos(toRad(zenithDeg))) : 0;
  let luf = NIGHT_LUF_MHZ + (ssn * 0.02 + 2.5) * sun;
  luf *= 1.0 + 0.2 * (hops - 1);
  luf += kIndex * 0.4;
  return Math.max(NIGHT_LUF_MHZ, Math.min(luf, 15.0));
}

/** LUF with the D-layer gone (night), quiet single hop — PropagationService.NightLufMHz. */
export const NIGHT_LUF_MHZ = 1.0;

/** Reliability 0–99 — port of PropagationService.CalculateReliability. */
export function calculateReliability(
  freqMHz: number,
  mufMHz: number,
  lufMHz: number,
  kIndex: number,
  hops: number,
): number {
  if (freqMHz <= 0 || mufMHz <= 0) return 0;

  // A forecast MUF is a median: ~50% at the MUF, ~13% at 1.15·MUF (ITU-R P.533
  // day-to-day spread) — not a cliff.
  const reachesUp = normalCdf((mufMHz - freqMHz) / (MUF_SPREAD * mufMHz));
  // D-layer absorption grows as 1/f²: nothing at the LUF, full well above it.
  const clearsAbsorption = freqMHz <= lufMHz ? 0 : 1 - Math.pow(lufMHz / freqMHz, 2);

  let reliability = 99 * reachesUp * clearsAbsorption;

  let kDegradation = 1.0;
  if (kIndex >= 4) kDegradation = Math.max(0.1, 1.0 - (kIndex - 3) * 0.2);
  else if (kIndex >= 2) kDegradation = 1.0 - (kIndex - 1) * 0.05;
  reliability *= kDegradation;

  reliability *= Math.pow(0.92, Math.max(0, hops - 1));
  return Math.round(Math.max(0, Math.min(99, reliability)));
}

/** Day-to-day spread of the real MUF around the forecast median (1σ) — PropagationService.MufSpread. */
export const MUF_SPREAD = 0.13;

/** Standard normal CDF (Abramowitz & Stegun 7.1.26) — PropagationService.NormalCdf. */
export function normalCdf(z: number): number {
  const x = Math.abs(z) / Math.SQRT2;
  const t = 1 / (1 + 0.3275911 * x);
  const erf = 1 - ((((1.061405429 * t - 1.453152027) * t + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t * Math.exp(-x * x);
  return z >= 0 ? 0.5 * (1 + erf) : 0.5 * (1 - erf);
}

/** Coarse 3-bucket rating from a 0–99 reliability. */
export function bandOpenState(reliability: number): BandOpenState {
  if (reliability >= 50) return 'open';
  if (reliability >= 20) return 'marginal';
  return 'closed';
}

/**
 * Minutes until the next sunrise or sunset at (lat, lon), and whether that end
 * is in the gray line right now. Reuses calculateSunTimes; handles the
 * next-day wrap and polar day/night (null).
 */
export function nextSunEvent(
  lat: number,
  lon: number,
  now: Date,
): { event: 'sunrise' | 'sunset'; minutes: number } | null {
  const dayMs = 24 * 60 * 60 * 1000;
  for (let d = 0; d <= 1; d++) {
    const times = calculateSunTimes(lat, lon, new Date(now.getTime() + d * dayMs));
    if (!times) continue;
    const candidates: Array<{ event: 'sunrise' | 'sunset'; at: Date }> = [
      { event: 'sunrise' as const, at: times.sunrise },
      { event: 'sunset' as const, at: times.sunset },
    ]
      .filter((c) => c.at.getTime() > now.getTime())
      .sort((a, b) => a.at.getTime() - b.at.getTime());
    if (candidates.length > 0) {
      const next = candidates[0];
      return { event: next.event, minutes: Math.round((next.at.getTime() - now.getTime()) / 60000) };
    }
  }
  return null;
}

/** Is this point in the gray line right now (solar elevation between −6° and 0°)? */
function inGrayLine(lat: number, lon: number, now: Date): boolean {
  const el = getSolarElevation(lat, lon, now);
  return el <= 0 && el >= -6;
}

/**
 * How close is an approaching gray line worth flagging (minutes). Gray-line
 * enhancement is a real, near-term operating cue; we only surface it when it's
 * imminent so the Coach stays "right and rare".
 */
const GRAYLINE_LOOKAHEAD_MIN = 45;

/**
 * Assess a single spot opportunity. Pass the operator QTH and the DX centroid
 * (both may be null); the spot frequency in MHz; SFI + K; and `now`.
 *
 * Returns null-ish fields gracefully when coordinates or indices are missing so
 * the caller can simply not render what it doesn't have.
 */
export function assessPropagation(params: {
  freqMHz: number;
  deLat: number | null;
  deLon: number | null;
  dxLat: number | null;
  dxLon: number | null;
  sfi: number | null;
  kIndex: number | null;
  /** Live ionosonde readings; when present they calibrate the model's level. */
  ionosondes?: IonosondeReading[];
  now?: Date;
}): PropAssessment {
  const { freqMHz, deLat, deLon, dxLat, dxLon, sfi, kIndex } = params;
  const now = params.now ?? new Date();

  const haveDx = dxLat != null && dxLon != null;
  const haveDe = deLat != null && deLon != null;

  // --- Gray-line timing (needs at least one endpoint) --------------------
  let grayLine: GrayLineInfo | null = null;
  const ends: Array<{ end: 'you' | 'dx'; lat: number; lon: number }> = [];
  if (haveDe) ends.push({ end: 'you', lat: deLat!, lon: deLon! });
  if (haveDx) ends.push({ end: 'dx', lat: dxLat!, lon: dxLon! });

  for (const e of ends) {
    if (inGrayLine(e.lat, e.lon, now)) {
      grayLine = {
        active: true,
        end: e.end,
        minutesToEvent: 0,
        label: e.end === 'dx' ? 'Gray line now (DX)' : 'Gray line now (you)',
      };
      break; // an active gray line beats an approaching one
    }
  }
  if (!grayLine) {
    // Nearest imminent sun event across the endpoints.
    let best: GrayLineInfo | null = null;
    for (const e of ends) {
      const nxt = nextSunEvent(e.lat, e.lon, now);
      if (!nxt || nxt.minutes > GRAYLINE_LOOKAHEAD_MIN) continue;
      if (!best || nxt.minutes < best.minutesToEvent) {
        const who = e.end === 'dx' ? 'DX' : 'You';
        best = {
          active: false,
          end: e.end,
          minutesToEvent: nxt.minutes,
          label: `${who} ${nxt.event} ~${nxt.minutes}m`,
        };
      }
    }
    grayLine = best;
  }

  // --- Coarse band-open score (needs full path + indices) ----------------
  let reliability: number | null = null;
  let open: BandOpenState = 'unknown';
  let mufMHz: number | null = null;
  let source: ForecastSource = 'model';
  if (haveDe && haveDx && sfi != null && kIndex != null) {
    const distanceKm = calculateDistance(deLat!, deLon!, dxLat!, dxLon!);
    const mid = midpoint(deLat!, deLon!, dxLat!, dxLon!);
    const zenith = 90 - getSolarElevation(mid.lat, mid.lon, now);
    const ssn = ssnFromSfi(sfi);
    const hops = Math.max(1, Math.ceil(distanceKm / 3500));
    const cal = calibrationForPath(params.ionosondes ?? [], ssn, deLat!, deLon!, dxLat!, dxLon!, now);
    source = cal.source;
    const muf = pathMuf(ssn, deLat!, deLon!, dxLat!, dxLon!, now, cal.factors);
    const luf = calculateLuf(ssn, distanceKm, zenith, kIndex);
    mufMHz = muf;
    reliability = calculateReliability(freqMHz, muf, luf, kIndex, hops);
    open = bandOpenState(reliability);
  }

  // --- Summary -----------------------------------------------------------
  const parts: string[] = [];
  if (open !== 'unknown') {
    const basis =
      source === 'measured'
        ? 'live ionosondes near this path'
        : source === 'calibrated'
          ? 'model corrected by live ionosondes'
          : 'solar-flux model';
    parts.push(
      `Path ${open} (${reliability}% reliability; MUF ~${Math.round(mufMHz!)} MHz from ${basis})`,
    );
  }
  if (grayLine) parts.push(grayLine.label);
  const summary = parts.length > 0 ? parts.join(' · ') : 'No propagation data';

  return { open, reliability, mufMHz, source, grayLine, summary };
}
