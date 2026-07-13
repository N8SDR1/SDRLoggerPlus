import type { MultiSelectOption } from '../components/MultiSelectDropdown';

// Ham band edges in kHz. Shared by the DX Cluster and POTA panels so band
// classification and the Band dropdown never drift apart.
export const BAND_RANGES: Record<string, [number, number]> = {
  '2200m': [135.7, 137.8],
  '630m': [472, 479],
  '160m': [1800, 2000],
  '80m': [3500, 4000],
  '60m': [5330, 5410],
  '40m': [7000, 7300],
  '30m': [10100, 10150],
  '20m': [14000, 14350],
  '17m': [18068, 18168],
  '15m': [21000, 21450],
  '12m': [24890, 24990],
  '10m': [28000, 29700],
  '6m': [50000, 54000],
  '2m': [144000, 148000],
  '1.25m': [222000, 225000],
  '70cm': [420000, 450000],
  '33cm': [902000, 928000],
  '23cm': [1240000, 1300000],
};

export const BAND_OPTIONS: MultiSelectOption[] = [
  { value: '2200m', label: '2200m' },
  { value: '630m', label: '630m' },
  { value: '160m', label: '160m' },
  { value: '80m', label: '80m' },
  { value: '60m', label: '60m' },
  { value: '40m', label: '40m' },
  { value: '30m', label: '30m' },
  { value: '20m', label: '20m' },
  { value: '17m', label: '17m' },
  { value: '15m', label: '15m' },
  { value: '12m', label: '12m' },
  { value: '10m', label: '10m' },
  { value: '6m', label: '6m' },
  { value: '2m', label: '2m' },
  { value: '1.25m', label: '1.25m' },
  { value: '70cm', label: '70cm' },
  { value: '33cm', label: '33cm' },
  { value: '23cm', label: '23cm' },
];

export const MODE_OPTIONS: MultiSelectOption[] = [
  { value: 'CW', label: 'CW' },
  { value: 'SSB', label: 'SSB' },
  { value: 'FT8', label: 'FT8' },
  { value: 'FT4', label: 'FT4' },
  { value: 'RTTY', label: 'RTTY' },
  { value: 'DIGI', label: 'Digital' },
];

/** Map a frequency in kHz to its band label, or '?' if outside known ham bands. */
export function getBandFromFrequency(freq: number): string {
  for (const [band, [min, max]] of Object.entries(BAND_RANGES)) {
    if (freq >= min && freq <= max) return band;
  }
  return '?';
}

export type BandClass = 'MF' | 'HF' | '6M' | 'VHF' | 'UHF';

/**
 * Classify a frequency (kHz) into a band class:
 * MF/LF 135 kHz–1.8 MHz (2200m, 630m), HF < 30 MHz (160m–10m),
 * 6M (50–54 MHz — its own class because so many HF rigs cover 160m–6m and
 * their owners want HF + 6m without 2m/70cm), VHF the rest of 30–300 MHz
 * (2m, 1.25m), UHF ≥ 300 MHz (70cm, 33cm, 23cm and up — incl. microwave).
 * Returns null only below the lowest ham allocation. Keyed on frequency, not
 * band label, so it stays correct for any band added later.
 */
export function bandClassForFrequency(freqKhz: number): BandClass | null {
  if (freqKhz < 135) return null;
  if (freqKhz < 1800) return 'MF';
  if (freqKhz < 30000) return 'HF';
  if (freqKhz >= 50000 && freqKhz < 54000) return '6M';
  if (freqKhz < 300000) return 'VHF';
  return 'UHF';
}
