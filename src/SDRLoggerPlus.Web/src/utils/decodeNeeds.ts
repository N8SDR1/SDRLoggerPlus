import type { WsjtxDecodeEvent } from '../api/signalr';

/**
 * Whether a decode fills an award need — the shared "Needed only" test used by
 * both the Digital Decodes panel and the Grid Tracker's Follow-Digital-Decodes
 * mode, so the two always agree on what "needed" means.
 */
export function isDecodeNeeded(d: WsjtxDecodeEvent): boolean {
  return d.spotStatus === 'newDxcc' || d.spotStatus === 'newBand'
    || d.zoneStatus === 'newZone' || d.zoneStatus === 'newZoneBand'
    || d.gridStatus === 'newGrid' || d.gridStatus === 'newGridBand';
}
