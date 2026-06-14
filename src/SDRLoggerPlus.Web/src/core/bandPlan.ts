/**
 * ITU band plan (SDRLogger+ port): strict per-region band edges used for
 * out-of-band VFO warnings, plus loose ranges for band detection so a VFO
 * slightly outside an allocation still maps to its band (and can be warned).
 */

export type ItuRegion = 1 | 2 | 3;

/** Strict ITU band edges per region: { band: [lo_mhz, hi_mhz] }. */
export const ITU_BANDS: Record<ItuRegion, Record<string, [number, number]>> = {
  1: { // Region 1 — Europe, Africa, Middle East, Russia
    '160m': [1.810, 2.000], '80m': [3.500, 3.800], '60m': [5.3515, 5.3665],
    '40m': [7.000, 7.200], '30m': [10.100, 10.150], '20m': [14.000, 14.350],
    '17m': [18.068, 18.168], '15m': [21.000, 21.450], '12m': [24.890, 24.990],
    '10m': [28.000, 29.700], '6m': [50.000, 52.000], '2m': [144.000, 146.000], '70cm': [430.000, 440.000],
  },
  2: { // Region 2 — Americas
    '160m': [1.800, 2.000], '80m': [3.500, 4.000], '60m': [5.3305, 5.4065],
    '40m': [7.000, 7.300], '30m': [10.100, 10.150], '20m': [14.000, 14.350],
    '17m': [18.068, 18.168], '15m': [21.000, 21.450], '12m': [24.890, 24.990],
    '10m': [28.000, 29.700], '6m': [50.000, 54.000], '2m': [144.000, 148.000], '70cm': [420.000, 450.000],
  },
  3: { // Region 3 — Asia-Pacific
    '160m': [1.800, 2.000], '80m': [3.500, 3.900], '60m': [5.3515, 5.3665],
    '40m': [7.000, 7.200], '30m': [10.100, 10.150], '20m': [14.000, 14.350],
    '17m': [18.068, 18.168], '15m': [21.000, 21.450], '12m': [24.890, 24.990],
    '10m': [28.000, 29.700], '6m': [50.000, 54.000], '2m': [144.000, 148.000], '70cm': [420.000, 450.000],
  },
};

/** Loose detection ranges — wider than any region's strict edges. */
const LOOSE_BANDS: [number, number, string][] = [
  [1.7, 2.1, '160m'],
  [3.4, 4.1, '80m'],
  [5.2, 5.5, '60m'],
  [6.9, 7.4, '40m'],
  [9.9, 10.5, '30m'],
  [13.9, 14.4, '20m'],
  [18.0, 18.3, '17m'],
  [20.9, 21.5, '15m'],
  [24.8, 25.1, '12m'],
  [27.9, 29.8, '10m'],
  [49.5, 54.5, '6m'],
  [143.5, 148.5, '2m'],
  [419, 451, '70cm'],
];

/** Maps a frequency (MHz) to a band name via the loose ranges, or null. */
export function bandForFrequency(mhz: number): string | null {
  for (const [lo, hi, name] of LOOSE_BANDS) {
    if (mhz >= lo && mhz <= hi) return name;
  }
  return null;
}

export interface OutOfBandInfo {
  band: string;
  lo: number;
  hi: number;
}

/**
 * Returns out-of-band info when the frequency maps to a band but lies outside
 * that band's strict ITU edges for the given region; null when in-band or in
 * no band at all. Unknown regions fall back to region 2.
 */
export function checkOutOfBand(mhz: number, region: ItuRegion): OutOfBandInfo | null {
  const band = bandForFrequency(mhz);
  if (!band) return null;
  const plan = ITU_BANDS[region] ?? ITU_BANDS[2];
  const edges = plan[band];
  if (!edges) return null;
  const [lo, hi] = edges;
  return mhz < lo || mhz > hi ? { band, lo, hi } : null;
}
