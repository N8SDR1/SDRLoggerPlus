import { describe, it, expect } from 'vitest';
import {
  buildOpportunities,
  toReceptionReports,
  type CoachOptions,
  type CoachContext,
  type ReceptionReport,
} from '../../utils/dxCoach';
import { assessPropagation } from '../../utils/propagationGate';
import { gridToLatLon } from '../../utils/maidenhead';
import type { Spot } from '../../store/appStore';

const OPTS: CoachOptions = {
  hideUnlikely: false,
  showDxcc: true,
  showWaz: true,
  showLowBand: true,
  showHf: true,
  show6m: true,
  showVhf: true,
  showUhf: true,
};

// Ohio (EM79) on a quiet mid-November midday at a declining-cycle flux — the
// conditions under which the old reliability filter emptied the Coach.
const NOW = new Date('2026-11-15T15:00:00Z');
const CTX: CoachContext = { qth: { lat: 39.7, lon: -84.5 }, sfi: 110, kIndex: 0, rigBand: null, now: NOW };

const GERMANY = { country: 'Fed. Rep. of Germany', lat: 51, lon: 10 };

let n = 0;
function spot(over: Partial<Spot> = {}): Spot {
  n++;
  return {
    id: `s${n}`,
    dxCall: 'DL1ABC',
    spotter: 'G4XYZ',
    frequency: 28025, // kHz — 10m
    mode: 'CW',
    timestamp: '2026-11-15T14:55:00Z',
    country: GERMANY.country,
    status: 'newBand',
    dxStation: GERMANY,
    spotterStation: { country: 'England', continent: 'EU' },
    heardFromMyContinent: false,
    ...over,
  };
}

const confidenceOf = (spots: Spot[], ctx: CoachContext = CTX) => buildOpportunities(spots, OPTS, ctx)[0]?.evidence.confidence;

describe('buildOpportunities — evidence first', () => {
  it('premise: the model alone scores this real, open 10m path as unlikely', () => {
    const p = assessPropagation({ freqMHz: 28.025, deLat: 39.7, deLon: -84.5, dxLat: 51, dxLon: 10, sfi: 110, kIndex: 0, now: NOW });
    expect(p.reliability!).toBeLessThan(20);
  });

  it('never hides by default — an unlikely forecast still shows, labelled as such', () => {
    const out = buildOpportunities([spot()], OPTS, CTX);
    expect(out).toHaveLength(1);
    expect(out[0].evidence.confidence).toBe('unlikely');
    expect(out[0].evidence.label).toBe('Forecast: unlikely');
  });

  it('"Hide unlikely paths" hides only forecast-unlikely spots', () => {
    const opts = { ...OPTS, hideUnlikely: true };
    expect(buildOpportunities([spot()], opts, CTX)).toHaveLength(0);
    expect(buildOpportunities([spot({ heardFromMyContinent: true, spotterStation: { continent: 'NA' } })], opts, CTX)).toHaveLength(1);
  });

  it('a same-continent spotter with only an approximate position = heard in your region', () => {
    const s = spot({ spotter: 'K6ABC', heardFromMyContinent: true, spotterStation: { continent: 'NA' }, spotterLocation: { lat: 39.8, lon: -98.6, precise: false } });
    const out = buildOpportunities([s], OPTS, CTX);
    expect(out[0].evidence.confidence).toBe('heardRegion');
    expect(out[0].evidence.label).toBe('Heard in NA');
  });

  it('a spotter with a precise position within 1,500 km = heard near you, with the distance', () => {
    const s = spot({ spotter: 'W8XX', heardFromMyContinent: true, spotterStation: { continent: 'NA' }, spotterLocation: { lat: 40.0, lon: -83.0, precise: true } });
    const out = buildOpportunities([s], OPTS, CTX);
    expect(out[0].evidence.confidence).toBe('heardNear');
    expect(out[0].evidence.detail).toMatch(/W8XX heard this station 5 min ago, 13\d km from you/);
  });

  it('a precise but distant same-continent spotter is only regional, not near', () => {
    // California → Ohio is ~3,000 km: W6 hearing Germany doesn't prove Ohio can.
    const s = spot({ spotter: 'W6XX', heardFromMyContinent: true, spotterStation: { continent: 'NA' }, spotterLocation: { lat: 37.3, lon: -121.9, precise: true } });
    expect(confidenceOf([s])).toBe('heardRegion');
  });

  it('your own signal heard near the DX on that band beats everything', () => {
    const heardMe: ReceptionReport[] = [{ receiver: 'DK0XX', lat: 50.5, lon: 9.0, band: '10m', ageMinutes: 4, source: 'RBN' }];
    const s = spot({ heardFromMyContinent: true, spotterStation: { continent: 'NA' }, spotterLocation: { lat: 40.0, lon: -83.0, precise: true } });
    const out = buildOpportunities([s], OPTS, { ...CTX, heardMe });
    expect(out[0].evidence.confidence).toBe('heardMe');
    expect(out[0].evidence.detail).toContain('DK0XX heard YOU on 10m 4 min ago (RBN)');
  });

  it('a reception report on another band, or far from the DX, does not count', () => {
    const heardMe: ReceptionReport[] = [
      { receiver: 'DK0XX', lat: 50.5, lon: 9.0, band: '20m', ageMinutes: 4, source: 'RBN' },
      { receiver: 'JA1XX', lat: 35.7, lon: 139.7, band: '10m', ageMinutes: 4, source: 'PSKReporter' },
    ];
    expect(confidenceOf([spot()], { ...CTX, heardMe })).toBe('unlikely');
  });

  it('pools evidence across repeat spots of one station+band', () => {
    const out = buildOpportunities(
      [spot(), spot({ spotter: 'W8XX', spotterLocation: { lat: 40.0, lon: -83.0, precise: true } }), spot()],
      OPTS,
      CTX,
    );
    expect(out).toHaveLength(1);
    expect(out[0].count).toBe(3);
    expect(out[0].evidence.confidence).toBe('heardNear');
  });

  it('no QTH: still shown, judged unknown unless someone heard it', () => {
    expect(confidenceOf([spot()], { ...CTX, qth: null })).toBe('unknown');
    expect(confidenceOf([spot({ heardFromMyContinent: true, spotterStation: { continent: 'NA' } })], { ...CTX, qth: null })).toBe('heardRegion');
  });

  it('skips spots that fill no award gap, and award types switched off', () => {
    expect(buildOpportunities([spot({ status: 'worked' })], OPTS, CTX)).toHaveLength(0);
    expect(buildOpportunities([spot({ status: 'newBand' })], { ...OPTS, showDxcc: false }, CTX)).toHaveLength(0);
  });
});

describe('ranking', () => {
  const near = { heardFromMyContinent: true, spotterStation: { continent: 'NA' }, spotterLocation: { lat: 40.0, lon: -83.0, precise: true } };

  it('better evidence ranks higher within the same award tier', () => {
    const out = buildOpportunities([spot({ dxCall: 'F5AAA' }), spot({ dxCall: 'G3BBB', ...near })], OPTS, CTX);
    expect(out.map((o) => o.spot.dxCall)).toEqual(['G3BBB', 'F5AAA']);
  });

  it('a new DXCC on an unlikely path still outranks an unconfirmed band-fill', () => {
    const out = buildOpportunities(
      [spot({ dxCall: 'G3BBB', status: 'newBand' }), spot({ dxCall: 'ZS1AAA', status: 'newDxcc', dxStation: { lat: -30, lon: 25 } })],
      OPTS,
      CTX,
    );
    expect(out.map((o) => o.evidence.confidence)).toEqual(['unlikely', 'unlikely']);
    expect(out.map((o) => o.spot.dxCall)).toEqual(['ZS1AAA', 'G3BBB']);
  });

  it('but a band-fill confirmed near you outranks a DXCC only forecast as unlikely', () => {
    const out = buildOpportunities(
      [spot({ dxCall: 'G3BBB', status: 'newBand', ...near }), spot({ dxCall: 'ZS1AAA', status: 'newDxcc', dxStation: { lat: -30, lon: 25 } })],
      OPTS,
      CTX,
    );
    expect(out.map((o) => o.spot.dxCall)).toEqual(['G3BBB', 'ZS1AAA']);
  });
});

describe('toReceptionReports', () => {
  it('normalises PSKReporter (grid, Hz) and RBN (lat/lon, band) into one list', () => {
    const out = toReceptionReports(
      [{ receiverCallsign: 'DL2XX', receiverLocator: 'JO62', frequencyHz: 28074000, flowStartSeconds: NOW.getTime() / 1000 - 600 }],
      [{ skimmer: 'DK0XX', lat: 50.5, lon: 9.0, freqKhz: 28025, band: '10m', ageSeconds: 120 }],
      gridToLatLon,
      NOW,
    );
    expect(out).toEqual([
      expect.objectContaining({ receiver: 'DL2XX', band: '10m', ageMinutes: 10, source: 'PSKReporter' }),
      expect.objectContaining({ receiver: 'DK0XX', band: '10m', ageMinutes: 2, source: 'RBN' }),
    ]);
  });
});
