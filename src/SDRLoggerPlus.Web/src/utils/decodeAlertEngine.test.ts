import { describe, it, expect } from 'vitest';
import { matchesRule, matchesAnyRule, usCallArea, baseCall } from './decodeAlertEngine';
import type { WsjtxDecodeEvent } from '../api/signalr';
import type { DecodeAlertRule } from '../store/settingsStore';

function evt(p: Partial<WsjtxDecodeEvent> = {}): WsjtxDecodeEvent {
  return {
    source: 1, clientId: 'MSHV', callsign: 'W1ABC', isCq: true, snr: -10,
    deltaTimeSeconds: 0, audioOffsetHz: 1500, frequencyHz: 14_075_500, band: '20m',
    mode: 'FT8', decodedAtUtc: '2026-07-15T20:00:00Z', ...p,
  };
}

function rule(p: Partial<DecodeAlertRule> = {}): DecodeAlertRule {
  return {
    id: 'r1', enabled: true, name: 't',
    newDxcc: false, newBand: false, newZone: false, newGrid: false,
    continents: [], dxccEntities: [], callAreas: [], prefixes: [], gridFields: [],
    bands: [], modes: [], sound: true, voice: false, popup: true, cooldownMinutes: 10, ...p,
  };
}

describe('usCallArea', () => {
  it.each([
    ['W6ABC', 6], ['K1ABC', 1], ['N0XYZ', 0], ['AA1AA', 1],
    ['W1ABC/6', 6],   // portable override
    ['VE3ABC', null], // non-US
    ['DL1XYZ', null],
  ])('%s -> %s', (call, area) => {
    expect(usCallArea(call)).toBe(area);
  });
});

describe('baseCall', () => {
  it('strips portable suffix', () => expect(baseCall('W1ABC/P')).toBe('W1ABC'));
  it('picks the real call from a prefix form', () => expect(baseCall('VE3/W1ABC')).toBe('W1ABC'));
  it('leaves a plain call', () => expect(baseCall('W6XYZ')).toBe('W6XYZ'));
});

describe('matchesRule — needs', () => {
  it('fires on newGrid when enabled', () =>
    expect(matchesRule(evt({ gridStatus: 'newGrid' }), rule({ newGrid: true }))).toBe(true));
  it('no need enabled → no match', () =>
    expect(matchesRule(evt({ gridStatus: 'newGrid' }), rule())).toBe(false));
  it('need enabled but decode not that need → no match', () =>
    expect(matchesRule(evt({ spotStatus: 'worked' }), rule({ newGrid: true }))).toBe(false));
  it('newDxcc fires', () =>
    expect(matchesRule(evt({ spotStatus: 'newDxcc' }), rule({ newDxcc: true }))).toBe(true));
});

describe('matchesRule — scope', () => {
  const needGrid = { newGrid: true } as const;

  it('continent must match', () => {
    expect(matchesRule(evt({ gridStatus: 'newGrid', continent: 'NA' }), rule({ ...needGrid, continents: ['NA'] }))).toBe(true);
    expect(matchesRule(evt({ gridStatus: 'newGrid', continent: 'EU' }), rule({ ...needGrid, continents: ['NA'] }))).toBe(false);
  });

  it('US call area must match', () => {
    expect(matchesRule(evt({ callsign: 'W6ABC', gridStatus: 'newGrid' }), rule({ ...needGrid, callAreas: [6] }))).toBe(true);
    expect(matchesRule(evt({ callsign: 'W1ABC', gridStatus: 'newGrid' }), rule({ ...needGrid, callAreas: [6] }))).toBe(false);
  });

  it('prefix must match (base call)', () => {
    expect(matchesRule(evt({ callsign: 'W1ABC', gridStatus: 'newGrid' }), rule({ ...needGrid, prefixes: ['W'] }))).toBe(true);
    expect(matchesRule(evt({ callsign: 'K1ABC', gridStatus: 'newGrid' }), rule({ ...needGrid, prefixes: ['W'] }))).toBe(false);
  });

  it('grid field must match', () => {
    expect(matchesRule(evt({ grid: 'EM79', gridStatus: 'newGrid' }), rule({ ...needGrid, gridFields: ['EM'] }))).toBe(true);
    expect(matchesRule(evt({ grid: 'FN42', gridStatus: 'newGrid' }), rule({ ...needGrid, gridFields: ['EM'] }))).toBe(false);
  });

  it('band/mode filters apply', () => {
    expect(matchesRule(evt({ gridStatus: 'newGrid', band: '20m' }), rule({ ...needGrid, bands: ['20m'] }))).toBe(true);
    expect(matchesRule(evt({ gridStatus: 'newGrid', band: '40m' }), rule({ ...needGrid, bands: ['20m'] }))).toBe(false);
    expect(matchesRule(evt({ gridStatus: 'newGrid', mode: 'FT4' }), rule({ ...needGrid, modes: ['FT8'] }))).toBe(false);
  });

  it('combined: needed grid, NA, 20m — all must hold', () => {
    const r = rule({ newGrid: true, continents: ['NA'], bands: ['20m'] });
    expect(matchesRule(evt({ gridStatus: 'newGrid', continent: 'NA', band: '20m' }), r)).toBe(true);
    expect(matchesRule(evt({ gridStatus: 'newGrid', continent: 'NA', band: '40m' }), r)).toBe(false);
  });
});

describe('matchesAnyRule — powers the "Match Alerts" list filter', () => {
  const naGrids = rule({ id: 'na', newGrid: true, continents: ['NA'] });

  it('a NA new-grid decode passes; an EU one is filtered out', () => {
    expect(matchesAnyRule(evt({ gridStatus: 'newGrid', continent: 'NA' }), [naGrids])).toBe(true);
    // The reported bug: Saudi/Kuwait/Brazil (non-NA) must NOT show under a NA-grids rule.
    expect(matchesAnyRule(evt({ callsign: 'HZ1AB', gridStatus: 'newGrid', continent: 'AS' }), [naGrids])).toBe(false);
    expect(matchesAnyRule(evt({ callsign: 'PY2XY', gridStatus: 'newGrid', continent: 'SA' }), [naGrids])).toBe(false);
  });

  it('disabled rules are ignored', () => {
    expect(matchesAnyRule(evt({ gridStatus: 'newGrid', continent: 'NA' }), [{ ...naGrids, enabled: false }])).toBe(false);
  });

  it('passes if ANY enabled rule matches (OR across rules)', () => {
    const euDxcc = rule({ id: 'eu', newDxcc: true, continents: ['EU'] });
    expect(matchesAnyRule(evt({ spotStatus: 'newDxcc', continent: 'EU' }), [naGrids, euDxcc])).toBe(true);
  });

  it('no rules → nothing matches', () => {
    expect(matchesAnyRule(evt({ gridStatus: 'newGrid', continent: 'NA' }), [])).toBe(false);
  });
});
