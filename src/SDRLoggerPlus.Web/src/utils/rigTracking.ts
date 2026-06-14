/**
 * Maps a rig's reported operating mode to the set of DX-cluster spot-filter
 * modes it should match. Returns null when the rig mode is unknown/empty,
 * meaning "do not constrain by mode" (band-only tracking).
 *
 * Spot-filter modes come from ClusterPlugin's MODE_OPTIONS:
 *   CW, SSB, FT8, FT4, RTTY, DIGI
 */
export function rigModeToSpotModes(rigMode: string | undefined | null): string[] | null {
  if (!rigMode) return null;
  const m = rigMode.trim().toUpperCase();
  if (m === '') return null;

  switch (m) {
    case 'USB':
    case 'LSB':
    case 'AM':
    case 'FM':
      return ['SSB'];
    case 'CW':
    case 'CW-R':
    case 'CWR':
      return ['CW'];
    case 'FT8':
      return ['FT8'];
    case 'FT4':
      return ['FT4'];
    case 'RTTY':
      return ['RTTY'];
    case 'DIGU':
    case 'DIGL':
    case 'DATA':
    case 'PKT':
    case 'PSK':
      return ['FT8', 'FT4', 'DIGI'];
    default:
      return null;
  }
}
