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
