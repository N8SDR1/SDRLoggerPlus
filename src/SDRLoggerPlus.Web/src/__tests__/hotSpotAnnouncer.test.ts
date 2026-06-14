import { describe, it, expect, beforeEach } from 'vitest';
import { shouldAnnounce, spellCallsign, resetAnnouncerState } from '../utils/hotSpotAnnouncer';

describe('hotSpotAnnouncer', () => {
  beforeEach(() => resetAnnouncerState());

  it('spells callsigns character by character', () => {
    expect(spellCallsign('K5P')).toBe('K 5 P');
    expect(spellCallsign('vp8xyz')).toBe('V P 8 X Y Z');
  });

  it('announces the first time, then respects the cooldown', () => {
    const t0 = 1_000_000;
    expect(shouldAnnounce('K5P', 15, t0)).toBe(true);
    expect(shouldAnnounce('K5P', 15, t0 + 5 * 60_000)).toBe(false);   // 5 min later
    expect(shouldAnnounce('K5P', 15, t0 + 16 * 60_000)).toBe(true);   // past cooldown
  });

  it('tracks cooldowns per callsign, case-insensitively', () => {
    const t0 = 1_000_000;
    expect(shouldAnnounce('K5P', 15, t0)).toBe(true);
    expect(shouldAnnounce('VP8XYZ', 15, t0)).toBe(true);  // different call, no shared cooldown
    expect(shouldAnnounce('k5p', 15, t0 + 1000)).toBe(false);
  });
});
