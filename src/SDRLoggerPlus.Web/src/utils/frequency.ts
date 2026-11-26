// Spot frequency unit helpers.
//
// DX-spot frequencies travel in **kHz** everywhere in the app: SpotSelectedEvent,
// SpotReceivedEvent, RbnSpot, and the POTA/cluster/globe/TCI senders all use kHz
// (see LogHub.SelectSpot's kHz→Hz conversion and TciRadioService's Hz→kHz note).
// The rig readouts and the log form display MHz; band lookups want Hz. Keeping the
// conversions here — named by unit — stops the recurring kHz-treated-as-Hz mistake.

/** A spot frequency in kHz → the MHz string the log form shows (6 dp).
 *  e.g. 14250 → "14.250000". */
export function spotKhzToMhzString(khz: number): string {
  return (khz / 1000).toFixed(6);
}

/** A spot frequency in kHz → Hz, for band-lookup helpers that take Hz.
 *  e.g. 14250 → 14250000. */
export function spotKhzToHz(khz: number): number {
  return khz * 1000;
}

// -- storage conversions --------------------------------------------------
//
// Qso.Frequency is stored in **kHz**; every ADIF exporter divides by 1000 to
// reach the MHz that ADIF's FREQ field requires. The log forms display MHz,
// so the conversion happens at the submit/load boundary — here, not inline.
//
// Rounding matters: 14.074 * 1000 is 14074.000000000002 in binary floating
// point, and that noise would be persisted verbatim.

/** Round away binary-float noise, keeping sub-Hz precision in kHz. */
function roundKhz(khz: number): number {
  return Math.round(khz * 1e6) / 1e6;
}

/** MHz as typed in the log form → the kHz value stored in Qso.Frequency.
 *  e.g. 14.074 → 14074, 50.125 → 50125. */
export function formMhzToStoredKhz(mhz: number): number {
  return roundKhz(mhz * 1000);
}

/** Rig/Combo Hz → the kHz value stored in Qso.Frequency.
 *  e.g. 14074000 → 14074. */
export function rigHzToStoredKhz(hz: number): number {
  return roundKhz(hz / 1000);
}

/** Stored kHz → the MHz number the edit form shows.
 *  e.g. 14074 → 14.074, 14075.287 → 14.075287. */
export function storedKhzToFormMhz(khz: number): number {
  return Math.round((khz / 1000) * 1e6) / 1e6;
}

// -- Log Entry frequency --------------------------------------------------

/**
 * The frequency Log Entry shows and logs, as the form's MHz string.
 *
 * While Log Entry follows a connected rig, the rig's live frequency IS the
 * QSO frequency: it is read from the radio state at the moment it is shown or
 * logged, never kept as a copy in the form. A copy could be emptied (the form
 * resets after each QSO) or go stale (a spot or decoder pushed its own value),
 * and a rig only reports when its frequency changes, so nothing would refill
 * it — the next QSO was logged with no or the wrong frequency. SAT mode logs
 * the uplink the S.A.T. controller fills in instead, and without a rig the
 * operator's typed value is used.
 */
export function logEntryFrequencyMhz(opts: {
  following: boolean;
  rigFrequencyHz: number | null | undefined;
  satMode: boolean;
  formFrequency: string;
}): { mhz: string; fromRig: boolean } {
  const { following, rigFrequencyHz, satMode, formFrequency } = opts;
  if (following && !satMode && rigFrequencyHz != null && rigFrequencyHz > 0) {
    return { mhz: (rigFrequencyHz / 1_000_000).toFixed(6), fromRig: true };
  }
  return { mhz: formFrequency, fromRig: false };
}
