import type { Spot } from '../store/appStore';
import { getBandFromFrequency, bandClassForFrequency } from './spotBands';
import { assessPropagation, type IonosondeReading, type PropAssessment } from './propagationGate';
import { calculateDistance } from './maidenhead';

/**
 * DX Coach engine — turns live cluster spots into ranked award opportunities.
 *
 * Award side: the per-spot needs the backend already computes —
 *   • DXCC — newDxcc (whole new entity) / newBand (worked entity, new band)
 *   • WAZ  — newZone (whole new CQ zone) / newZoneBand (worked zone, new band)
 *
 * Workability side — evidence first, forecast last. Every opportunity gets ONE
 * confidence level from the strongest evidence available:
 *
 *   heardMe     your own signal was heard near the DX on this band (PSK/RBN)
 *   heardNear   a spotter with a known position close to you heard the DX
 *   heardRegion a spotter on your continent heard it (position only approximate)
 *   likely / possible / unlikely   forecast only (measured ionosphere or model)
 *   unknown     nothing to judge by (no QTH / no DX position)
 *
 * The Coach RANKS by this, it doesn't hide by it: a spot is proof somebody is
 * working that station, and a wrong forecast must cost a ranking position, not a
 * missed DXCC. Only "unlikely" can be hidden, and only if the operator opts in.
 */
export type ReasonKind = 'newDxcc' | 'newZone' | 'newBand' | 'newZoneBand';

export type Confidence = 'heardMe' | 'heardNear' | 'heardRegion' | 'likely' | 'possible' | 'unlikely' | 'unknown';

export interface AwardReason {
  kind: ReasonKind;
  chip: string;
  detail: string;
}

/** The single best piece of evidence behind an opportunity's confidence, for the UI. */
export interface Evidence {
  confidence: Confidence;
  /** Short label for the chip, e.g. "Heard near you". */
  label: string;
  /** One sentence saying exactly why, for the tooltip. */
  detail: string;
}

export interface Opportunity {
  spot: Spot;
  band: string;
  reasons: AwardReason[];
  onRigBand: boolean;
  prop: PropAssessment;
  evidence: Evidence;
  score: number;
  /** How many live spots collapsed into this one opportunity (call+band). */
  count: number;
}

/** One report of the operator's own signal (PSKReporter / RBN), normalised. */
export interface ReceptionReport {
  receiver: string;
  lat: number;
  lon: number;
  band: string;
  ageMinutes: number;
  source: 'PSKReporter' | 'RBN';
}

// Ranking weight per reason — DXCC entity is the crown jewel, a new CQ zone is
// next (rare + hard), then band-fills. An opportunity scores by its best reason.
export const REASON_SCORE: Record<ReasonKind, number> = {
  newDxcc: 100,
  newZone: 85,
  newBand: 50,
  newZoneBand: 45,
};

// Confidence nudges. Smaller than the gaps between award tiers on purpose: a new
// DXCC that looks unlikely still outranks a band-fill nobody has confirmed.
export const CONFIDENCE_SCORE: Record<Confidence, number> = {
  heardMe: 40,
  heardNear: 30,
  heardRegion: 20,
  likely: 12,
  possible: 0,
  unknown: 0,
  unlikely: -25,
};

/** "Near" — a spotter (or a receiver of your signal) this close counts as local evidence. */
export const NEAR_KM = 1500;

/** Forecast reliability bands, matching propagationGate's open/marginal/closed. */
const LIKELY_MIN = 50;
const POSSIBLE_MIN = 20;

/** Build the ordered reason list for a spot from its DXCC + WAZ status. */
export function reasonsFor(spot: Spot, band: string): AwardReason[] {
  const out: AwardReason[] = [];
  if (spot.status === 'newDxcc') {
    out.push({ kind: 'newDxcc', chip: '🌍 New DXCC', detail: 'New country for DXCC' });
  } else if (spot.status === 'newBand') {
    out.push({ kind: 'newBand', chip: '📻 New band', detail: `New band-slot on ${band}` });
  }
  const zone = spot.cqZone;
  if (spot.zoneStatus === 'newZone') {
    out.push({ kind: 'newZone', chip: `🧭 New zone ${zone ?? ''}`.trim(), detail: `New CQ zone ${zone ?? ''}`.trim() });
  } else if (spot.zoneStatus === 'newZoneBand') {
    out.push({ kind: 'newZoneBand', chip: `📶 Zone ${zone ?? ''} · band`.trim(), detail: `Zone ${zone ?? ''} — new on ${band}`.trim() });
  }
  out.sort((a, b) => REASON_SCORE[b.kind] - REASON_SCORE[a.kind]);
  return out;
}

export interface CoachOptions {
  hideUnlikely: boolean;
  showDxcc: boolean;
  showWaz: boolean;
  showLowBand: boolean;
  showHf: boolean;
  show6m: boolean;
  showVhf: boolean;
  showUhf: boolean;
}

export interface CoachContext {
  qth: { lat: number; lon: number } | null;
  sfi: number | null;
  kIndex: number | null;
  rigBand: string | null;
  /** Recent reports of the operator's own signal, all bands. */
  heardMe?: ReceptionReport[];
  /** Live ionosonde readings; they calibrate the forecast's level. */
  ionosondes?: IonosondeReading[];
  now?: Date;
}

const ageMin = (iso: string, now: Date) => Math.max(0, Math.round((now.getTime() - Date.parse(iso)) / 60000));
const km = (n: number) => `${Math.round(n).toLocaleString()} km`;

/** Pick the strongest evidence for one station+band group. */
export function judge(
  spots: Spot[],
  band: string,
  dx: { lat: number; lon: number } | null,
  prop: PropAssessment,
  ctx: CoachContext,
): Evidence {
  const now = ctx.now ?? new Date();

  // 1. Your own signal reached the DX's area on this band.
  if (dx) {
    let best: { r: ReceptionReport; d: number } | null = null;
    for (const r of ctx.heardMe ?? []) {
      if (r.band !== band) continue;
      const d = calculateDistance(r.lat, r.lon, dx.lat, dx.lon);
      if (d <= NEAR_KM && (!best || r.ageMinutes < best.r.ageMinutes)) best = { r, d };
    }
    if (best) {
      return {
        confidence: 'heardMe',
        label: 'Your signal heard there',
        detail: `${best.r.receiver} heard YOU on ${band} ${best.r.ageMinutes} min ago (${best.r.source}), ${km(best.d)} from this station.`,
      };
    }
  }

  // 2. A spotter with a known position near you heard the DX.
  if (ctx.qth) {
    let best: { s: Spot; d: number } | null = null;
    for (const s of spots) {
      const loc = s.spotterLocation;
      if (!loc?.precise) continue;
      const d = calculateDistance(ctx.qth.lat, ctx.qth.lon, loc.lat, loc.lon);
      if (d <= NEAR_KM && (!best || d < best.d)) best = { s, d };
    }
    if (best) {
      return {
        confidence: 'heardNear',
        label: 'Heard near you',
        detail: `${best.s.spotter} heard this station ${ageMin(best.s.timestamp, now)} min ago, ${km(best.d)} from you.`,
      };
    }
  }

  // 3. A spotter on your continent heard it (position too coarse to call "near").
  const regional = spots.find((s) => s.heardFromMyContinent);
  if (regional) {
    const where = regional.spotterStation?.continent ?? 'your continent';
    return {
      confidence: 'heardRegion',
      label: `Heard in ${where}`,
      detail: `${regional.spotter} (${where}) heard this station ${ageMin(regional.timestamp, now)} min ago.`,
    };
  }

  // 4. Forecast only. Say what it rests on: measured ionosphere beats the model.
  if (prop.reliability == null) {
    return { confidence: 'unknown', label: 'No forecast', detail: 'Not enough location data to judge this path.' };
  }
  const basis = prop.source === 'measured' ? ' (measured)' : '';
  const detail = `No reports from near you yet. ${prop.summary}.`;
  if (prop.reliability >= LIKELY_MIN) return { confidence: 'likely', label: `Forecast: likely${basis}`, detail };
  if (prop.reliability >= POSSIBLE_MIN) return { confidence: 'possible', label: `Forecast: possible${basis}`, detail };
  return { confidence: 'unlikely', label: `Forecast: unlikely${basis}`, detail };
}

export function buildOpportunities(spots: Spot[], opts: CoachOptions, ctx: CoachContext): Opportunity[] {
  // 1. Group live spots of the same station on the same band (several spotters,
  //    repeats). Evidence is pooled across the whole group.
  type Group = { spots: Spot[]; band: string; reasons: AwardReason[] };
  const groups = new Map<string, Group>();

  for (const s of spots) {
    const band = getBandFromFrequency(s.frequency);

    // What award gaps does this spot fill, limited to the award types chosen?
    const reasons = reasonsFor(s, band).filter((r) =>
      r.kind === 'newDxcc' || r.kind === 'newBand' ? opts.showDxcc : opts.showWaz,
    );
    if (reasons.length === 0) continue;

    // Band-class filter — an HF-only op has no use for 2m/70cm opportunities.
    const cls = bandClassForFrequency(s.frequency);
    if (cls === 'MF' && !opts.showLowBand) continue;
    if (cls === 'HF' && !opts.showHf) continue;
    if (cls === '6M' && !opts.show6m) continue;
    if (cls === 'VHF' && !opts.showVhf) continue;
    if (cls === 'UHF' && !opts.showUhf) continue;

    const key = `${s.dxCall.toUpperCase()}|${band}`;
    const g = groups.get(key) ?? { spots: [], band, reasons };
    g.spots.push(s);
    groups.set(key, g);
  }

  // 2. Judge each station+band once; the newest spot represents the group.
  const out: Opportunity[] = [];
  for (const g of groups.values()) {
    const spot = g.spots.reduce((a, b) => (b.timestamp > a.timestamp ? b : a));
    const dx =
      spot.dxStation?.lat != null && spot.dxStation?.lon != null
        ? { lat: spot.dxStation.lat, lon: spot.dxStation.lon }
        : null;

    const prop = assessPropagation({
      freqMHz: spot.frequency / 1000,
      deLat: ctx.qth?.lat ?? null,
      deLon: ctx.qth?.lon ?? null,
      dxLat: dx?.lat ?? null,
      dxLon: dx?.lon ?? null,
      sfi: ctx.sfi,
      kIndex: ctx.kIndex,
      ionosondes: ctx.ionosondes,
      now: ctx.now,
    });

    const evidence = judge(g.spots, g.band, dx, prop, ctx);
    if (opts.hideUnlikely && evidence.confidence === 'unlikely') continue;

    const onRigBand = ctx.rigBand != null && ctx.rigBand !== '?' && g.band === ctx.rigBand;

    let score = REASON_SCORE[g.reasons[0].kind] + CONFIDENCE_SCORE[evidence.confidence];
    if (onRigBand) score += 15;
    if (prop.grayLine) score += prop.grayLine.active ? 10 : 6;

    out.push({ spot, band: g.band, reasons: g.reasons, onRigBand, prop, evidence, score, count: g.spots.length });
  }

  out.sort((a, b) => b.score - a.score || (b.spot.timestamp > a.spot.timestamp ? 1 : -1));
  return out;
}

/** Normalise PSKReporter + RBN reports of the operator's own signal. */
export function toReceptionReports(
  psk: { receiverCallsign: string; receiverLocator: string; frequencyHz: number; flowStartSeconds: number }[],
  rbn: { skimmer: string; lat: number; lon: number; freqKhz: number; band: string; ageSeconds: number }[],
  gridToLatLon: (grid: string) => { lat: number; lon: number } | null,
  now: Date = new Date(),
): ReceptionReport[] {
  const out: ReceptionReport[] = [];
  for (const r of psk) {
    const at = gridToLatLon(r.receiverLocator);
    if (!at) continue;
    out.push({
      receiver: r.receiverCallsign,
      lat: at.lat,
      lon: at.lon,
      band: getBandFromFrequency(r.frequencyHz / 1000),
      ageMinutes: Math.max(0, Math.round((now.getTime() / 1000 - r.flowStartSeconds) / 60)),
      source: 'PSKReporter',
    });
  }
  for (const r of rbn) {
    out.push({
      receiver: r.skimmer,
      lat: r.lat,
      lon: r.lon,
      band: r.band,
      ageMinutes: Math.round(r.ageSeconds / 60),
      source: 'RBN',
    });
  }
  return out;
}
