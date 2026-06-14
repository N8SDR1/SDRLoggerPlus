// S-meter conversion (HF convention): S9 = -73 dBm, 6 dB per S-unit below
// S9, and dB-linear "over S9" annotated in 10 dB steps up to +40.
// The meter sweep runs S0 (-127 dBm) .. S9+40 (-33 dBm).

export const S9_DBM = -73;
export const DB_PER_S_UNIT = 6;
export const S_METER_FLOOR_DBM = S9_DBM - 9 * DB_PER_S_UNIT; // -127 = S0
export const S_METER_CEIL_DBM = S9_DBM + 40; // -33 = S9+40

export interface SUnitReading {
  /** Display label: "S0".."S9", "S9+10".."S9+40" */
  label: string;
  /** Needle position across the full sweep, 0..1 */
  fraction: number;
}

export function dbmToSUnit(dbm: number, maxOverS9Db = 40): SUnitReading {
  const ceilingDbm = S9_DBM + Math.max(10, maxOverS9Db);
  const clamped = Math.min(ceilingDbm, Math.max(S_METER_FLOOR_DBM, dbm));

  let label: string;
  if (clamped <= S9_DBM) {
    // Round to nearest S-unit; ties (exactly halfway) round up.
    const sUnit = Math.round((clamped - S_METER_FLOOR_DBM) / DB_PER_S_UNIT);
    label = `S${sUnit}`;
  } else {
    const over = Math.round((clamped - S9_DBM) / 10) * 10;
    label = over === 0 ? 'S9' : `S9+${over}`;
  }

  // Geometric sweep: S0..S9 occupies the first 60% of the scale (like a
  // real meter face), the +40 dB over-nine region the remaining 40%.
  const S9_SWEEP = 0.6;
  let fraction: number;
  if (clamped <= S9_DBM) {
    fraction = ((clamped - S_METER_FLOOR_DBM) / (S9_DBM - S_METER_FLOOR_DBM)) * S9_SWEEP;
  } else {
    fraction = S9_SWEEP + ((clamped - S9_DBM) / (ceilingDbm - S9_DBM)) * (1 - S9_SWEEP);
  }

  return { label, fraction };
}
