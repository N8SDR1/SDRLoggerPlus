/**
 * Which already-built globe tiles to drop.
 *
 * `three-slippy-map-globe` (used by the 3D globe and the map's inset globe)
 * only ever adds tile meshes. A zoomed globe that auto-rotates or is panned
 * therefore keeps one 256×256 GPU texture per tile it has displayed, for the
 * life of the session. The camera neighborhood is the cap: tiles outside it
 * are released and fetched again if the view comes back. One coarser zoom
 * that is still on the visible face stays, so zooming in does not flash the
 * bare sphere while the finer tiles load.
 */

export interface SlippyTileResidency {
  id: string;
  level: number;
  /** Active zoom, and inside the library's camera fetch radius. */
  inFetchRadius: boolean;
  /** Still on the visible face. Used for the one-level backdrop. */
  inView: boolean;
}

/** Coarser zoom left resident so a zoom-in does not flash the bare sphere. */
export const TILE_BACKDROP_LEVELS = 1;

export function tileIdsToRelease(
  tiles: readonly SlippyTileResidency[],
  activeLevel: number,
): string[] {
  const backdropLevel = activeLevel - TILE_BACKDROP_LEVELS;
  const released: string[] = [];
  for (const tile of tiles) {
    if (tile.level === activeLevel && tile.inFetchRadius) continue;
    if (tile.level === backdropLevel && tile.inView) continue;
    released.push(tile.id);
  }
  return released;
}
