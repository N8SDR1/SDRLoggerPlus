import { describe, expect, it } from 'vitest';
import { tileIdsToRelease, type SlippyTileResidency } from '../slippyTileBudget';

it('resolves the vendored tile engine in place of the published package', async () => {
  const mod = await import('three-slippy-map-globe');
  expect(typeof mod.default).toBe('function');
});

function tile(
  id: string,
  level: number,
  inFetchRadius: boolean,
  inView: boolean,
): SlippyTileResidency {
  return { id, level, inFetchRadius, inView };
}

describe('tileIdsToRelease', () => {
  const active = 7;

  it('keeps the tiles inside the camera neighborhood at the active zoom', () => {
    const tiles = [
      tile('7:1:1', 7, true, true),
      tile('7:2:2', 7, true, true),
    ];
    expect(tileIdsToRelease(tiles, active)).toEqual([]);
  });

  it('releases active-zoom tiles the camera has moved away from', () => {
    const tiles = [
      tile('7:1:1', 7, true, true),
      tile('7:9:9', 7, false, false),
    ];
    expect(tileIdsToRelease(tiles, active)).toEqual(['7:9:9']);
  });

  it('keeps the previous zoom where it is still on the visible face', () => {
    const tiles = [
      tile('6:1:1', 6, false, true),
      tile('6:8:8', 6, false, false),
    ];
    expect(tileIdsToRelease(tiles, active)).toEqual(['6:8:8']);
  });

  it('releases zoom levels the camera has left, including a long visit history', () => {
    const history: SlippyTileResidency[] = [];
    for (let i = 0; i < 4000; i++) {
      history.push(tile(`8:${i}:0`, 8, false, true));
    }
    for (let i = 0; i < 30; i++) {
      history.push(tile(`7:${i}:0`, 7, true, true));
    }
    history.push(tile('6:0:0', 6, false, true));

    const released = tileIdsToRelease(history, active);
    expect(released).toHaveLength(4000);
    expect(released.every((id) => id.startsWith('8:'))).toBe(true);
    expect(released).not.toContain('7:0:0');
    expect(released).not.toContain('6:0:0');
  });
});
