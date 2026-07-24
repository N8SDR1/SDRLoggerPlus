import { describe, it, expect } from 'vitest';
import { POPULAR_RIGS, normalizeModelName, resolvePopularRigs } from './popularRigs';
import type { HamlibRigModelInfo } from '../api/signalr';

const rig = (modelId: number, manufacturer: string, model: string): HamlibRigModelInfo => ({
  modelId, manufacturer, model, version: '', displayName: `${manufacturer} ${model}`,
});

describe('normalizeModelName', () => {
  it('ignores case, spaces and punctuation', () => {
    expect(normalizeModelName('FT-DX 10')).toBe('FTDX10');
    expect(normalizeModelName('ftdx-10')).toBe('FTDX10');
    expect(normalizeModelName('IC-7300')).toBe('IC7300');
  });

  it('keeps genuinely different models apart', () => {
    expect(normalizeModelName('TS-590SG')).not.toBe(normalizeModelName('TS-590S'));
  });
});

describe('resolvePopularRigs', () => {
  it('resolves a model id from the live list rather than hardcoding one', () => {
    const resolved = resolvePopularRigs([rig(3073, 'Icom', 'IC-7300')]);
    expect(resolved).toHaveLength(1);
    expect(resolved[0].id).toBe('ic7300');
    expect(resolved[0].modelId).toBe(3073);
    expect(resolved[0].modelName).toBe('Icom IC-7300');
  });

  it('matches regardless of how the library spells the model', () => {
    // Hamlib's spelling of the Yaesu names varies; the shortcut must still find them.
    expect(resolvePopularRigs([rig(1042, 'Yaesu', 'FT-DX10')])[0]?.id).toBe('ftdx10');
    expect(resolvePopularRigs([rig(1042, 'Yaesu', 'FTDX-10')])[0]?.id).toBe('ftdx10');
  });

  it('drops entries the installed library does not offer', () => {
    // Nothing should be presented as one-click if this build cannot actually drive it.
    expect(resolvePopularRigs([rig(1, 'Dummy', 'Dummy')])).toHaveLength(0);
  });

  it('returns nothing when the rig list has not loaded yet', () => {
    expect(resolvePopularRigs([])).toHaveLength(0);
  });

  it('prefers the first match so a duplicate name cannot displace it', () => {
    const resolved = resolvePopularRigs([rig(3073, 'Icom', 'IC-7300'), rig(9999, 'Icom', 'IC-7300')]);
    expect(resolved).toHaveLength(1);
    expect(resolved[0].modelId).toBe(3073);
  });

  it('carries the serial defaults and the radio-side hint through', () => {
    const [r] = resolvePopularRigs([rig(3073, 'Icom', 'IC-7300')]);
    expect(r.defaults.connectionType).toBe('Serial');
    expect(r.defaults.baudRate).toBeGreaterThan(0);
    expect(r.setupHint).toMatch(/CI-V/);
  });
});

describe('POPULAR_RIGS catalogue', () => {
  it('has unique ids', () => {
    const ids = POPULAR_RIGS.map((r) => r.id);
    expect(new Set(ids).size).toBe(ids.length);
  });

  it('gives every entry aliases, defaults and a setup hint', () => {
    for (const r of POPULAR_RIGS) {
      expect(r.aliases.length, `${r.id} needs at least one alias`).toBeGreaterThan(0);
      expect(r.setupHint.length, `${r.id} needs a setup hint`).toBeGreaterThan(0);
      expect(r.defaults.baudRate, `${r.id} needs a baud rate`).toBeTruthy();
    }
  });

  it('treats an entry as unverified unless it says otherwise', () => {
    // The UI warns on anything not yet confirmed against real hardware, so the default
    // has to be "unverified" — a missing flag must never read as a confirmation.
    for (const r of POPULAR_RIGS) {
      expect(r.verified === undefined || r.verified === true || r.verified === false).toBe(true);
      if (r.verified !== true) expect(Boolean(r.verified)).toBe(false);
    }
  });

  it('has no alias claimed by two different radios', () => {
    const seen = new Map<string, string>();
    for (const r of POPULAR_RIGS) {
      for (const a of r.aliases) {
        const key = normalizeModelName(a);
        expect(seen.has(key), `${key} claimed by both ${seen.get(key)} and ${r.id}`).toBe(false);
        seen.set(key, r.id);
      }
    }
  });
});
