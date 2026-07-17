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

/** Normalize PSK + RBN reception into station→receiver arcs, band-filtered, capped per-source by SNR then recency.
 * PSK (FT8) and RBN (CW) SNR live on different scales, so each source is sorted and capped
 * independently — otherwise a combined cap lets RBN's typically-higher SNR evict all PSK arcs. */
export function buildHeardMeArcs(
  station: Station, psk: PskInput[], rbn: RbnInput[], opts: Opts,
): HeardMeArc[] {
  const cap = opts.cap ?? 150;
  const band = opts.band ?? null;
  const pskArcs: HeardMeArc[] = [];
  const rbnArcs: HeardMeArc[] = [];

  for (const r of psk) {
    const khz = r.frequencyHz / 1000;
    const b = bandFromKhz(khz);
    if (band && b !== band) continue;
    const to = gridToLatLon(r.receiverLocator);
    if (!to) continue;
    pskArcs.push({
      source: 'psk', receiverCall: r.receiverCallsign,
      startLat: station.lat, startLon: station.lon, endLat: to.lat, endLon: to.lon,
      freqKhz: khz, band: b, mode: r.mode, snr: r.snr,
      ageMinutes: Math.max(0, Math.round((Date.now() / 1000 - r.flowStartSeconds) / 60)),
    });
  }
  for (const r of rbn) {
    if (band && r.band !== band) continue;
    rbnArcs.push({
      source: 'rbn', receiverCall: r.skimmer,
      startLat: station.lat, startLon: station.lon, endLat: r.lat, endLon: r.lon,
      freqKhz: r.freqKhz, band: r.band, mode: r.mode, snr: r.snr,
      ageMinutes: Math.round(r.ageSeconds / 60),
    });
  }

  const bySnrThenAge = (a: HeardMeArc, b: HeardMeArc) => (b.snr - a.snr) || (a.ageMinutes - b.ageMinutes);
  pskArcs.sort(bySnrThenAge);
  rbnArcs.sort(bySnrThenAge);
  return [...pskArcs.slice(0, cap), ...rbnArcs.slice(0, cap)];
}
