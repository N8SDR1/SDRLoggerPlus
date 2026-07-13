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
 * SDRLoggerPlus.Server/Services/PropagationService.cs (SsnFromSfi / CalculateMuf
 * / CalculateLuf / CalculateReliability) so the Coach and the Propagation panel
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

export interface PropAssessment {
  open: BandOpenState;
  /** 0–99 path reliability, or null when we lack coordinates to compute it. */
  reliability: number | null;
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
 * Maximum Usable Frequency (MHz) for a path — port of
 * PropagationService.CalculateMuf. `zenithDeg` is the solar zenith angle at the
 * path midpoint (90 − solar elevation).
 */
export function calculateMuf(ssn: number, distanceKm: number, midLat: number, zenithDeg: number): number {
  const hops = Math.max(1, Math.ceil(distanceKm / 3500));
  const ssnFactor = 4.0 + ssn * 0.035;

  let dayNightFactor: number;
  if (zenithDeg < 90) {
    dayNightFactor = Math.pow(Math.cos(toRad(zenithDeg)), 0.3);
  } else {
    dayNightFactor = 0.2 + 0.1 * Math.cos(toRad(Math.min(zenithDeg, 120) - 90));
  }

  const latFactor = 1.0 + 0.3 * Math.cos(toRad(midLat * 2));
  const foF2 = ssnFactor * dayNightFactor * latFactor;

  const hopDistance = distanceKm / hops;
  const mFactor = 1.0 + 2.5 * Math.sin(toRad(Math.min(90, (hopDistance / 3500) * 90)));

  let muf = foF2 * mFactor;
  if (hops > 1) muf *= Math.pow(0.9, hops - 1);
  return Math.max(2.0, muf);
}

/** Lowest Usable Frequency (MHz) — port of PropagationService.CalculateLuf. */
export function calculateLuf(
  ssn: number,
  distanceKm: number,
  zenithDeg: number,
  kIndex: number,
): number {
  const hops = Math.max(1, Math.ceil(distanceKm / 3500));
  if (zenithDeg >= 90) return 2.0 + kIndex * 0.3;

  const cosZenith = Math.cos(toRad(zenithDeg));
  let baseLuf = 2.0 + (ssn * 0.02 + 1.5) * Math.pow(cosZenith, 0.5);
  baseLuf *= 1.0 + 0.2 * (hops - 1);
  baseLuf += kIndex * 0.5;
  return Math.max(1.8, Math.min(baseLuf, 15.0));
}

/** Reliability 0–99 — port of PropagationService.CalculateReliability. */
export function calculateReliability(
  freqMHz: number,
  mufMHz: number,
  lufMHz: number,
  kIndex: number,
  hops: number,
): number {
  if (freqMHz > mufMHz) return 0;
  if (freqMHz < lufMHz) return 0;

  const usableRange = mufMHz - lufMHz;
  if (usableRange <= 0) return 0;

  const fot = mufMHz * 0.85;
  const fotPosition = (fot - lufMHz) / usableRange;
  const positionInRange = (freqMHz - lufMHz) / usableRange;

  const deviation = Math.abs(positionInRange - fotPosition);
  let reliability = 90.0 * Math.exp(-2.0 * deviation * deviation);

  let kDegradation = 1.0;
  if (kIndex >= 4) kDegradation = Math.max(0.1, 1.0 - (kIndex - 3) * 0.2);
  else if (kIndex >= 2) kDegradation = 1.0 - (kIndex - 1) * 0.05;
  reliability *= kDegradation;

  reliability *= Math.pow(0.92, Math.max(0, hops - 1));
  return Math.round(Math.max(0, Math.min(99, reliability)));
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
  if (haveDe && haveDx && sfi != null && kIndex != null) {
    const distanceKm = calculateDistance(deLat!, deLon!, dxLat!, dxLon!);
    const mid = midpoint(deLat!, deLon!, dxLat!, dxLon!);
    const zenith = 90 - getSolarElevation(mid.lat, mid.lon, now);
    const ssn = ssnFromSfi(sfi);
    const hops = Math.max(1, Math.ceil(distanceKm / 3500));
    const muf = calculateMuf(ssn, distanceKm, mid.lat, zenith);
    const luf = calculateLuf(ssn, distanceKm, zenith, kIndex);
    reliability = calculateReliability(freqMHz, muf, luf, kIndex, hops);
    open = bandOpenState(reliability);
  }

  // --- Summary -----------------------------------------------------------
  const parts: string[] = [];
  if (open !== 'unknown') {
    parts.push(
      reliability != null ? `Path ${open} (${reliability}% reliability)` : `Path ${open}`,
    );
  }
  if (grayLine) parts.push(grayLine.label);
  const summary = parts.length > 0 ? parts.join(' · ') : 'No propagation data';

  return { open, reliability, grayLine, summary };
}
