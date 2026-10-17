/** Log History auto-filter. Default on. '0' is the only stored off value. */
export const AUTO_HISTORY_KEY = 'sdrl_auto_history';

/**
 * Whether Log History should follow the current callsign.
 * Shared by the Log Entry switch and the spot-selection path (DX cluster
 * click and WSJT-X / JTDX DX Call), so turning History off applies to both.
 */
export function isAutoHistoryEnabled(): boolean {
  try {
    return localStorage.getItem(AUTO_HISTORY_KEY) !== '0';
  } catch {
    return true;
  }
}
