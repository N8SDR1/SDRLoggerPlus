import { describe, it, expect } from 'vitest';
import { buildHeardMeArcs, type HeardMeArc } from './heardMe';

const station = { lat: 43.0, lon: -89.4 }; // Madison WI-ish

describe('buildHeardMeArcs', () => {
  it('maps PSK reports to arcs from the station to each receiver', () => {
    const arcs = buildHeardMeArcs(station,
      [{ senderCallsign: 'K1ABC', senderLocator: 'EN53', receiverCallsign: 'W3LPL',
         receiverLocator: 'FM19', frequencyHz: 14074000, mode: 'FT8', snr: -5, flowStartSeconds: 0 }],
      [], { band: '20m' });
    expect(arcs).toHaveLength(1);
    expect(arcs[0].source).toBe('psk');
    expect(arcs[0].startLat).toBeCloseTo(station.lat);
    expect(arcs[0].receiverCall).toBe('W3LPL');
    expect(arcs[0].band).toBe('20m');
  });

  it('filters PSK to the active band client-side', () => {
    const arcs = buildHeardMeArcs(station,
      [{ senderCallsign: 'K1ABC', senderLocator: 'EN53', receiverCallsign: 'W3LPL',
         receiverLocator: 'FM19', frequencyHz: 7074000, mode: 'FT8', snr: -5, flowStartSeconds: 0 }],
      [], { band: '20m' });
    expect(arcs).toHaveLength(0); // 40m report excluded when band=20m
  });

  it('maps RBN reports (already located) to arcs and tags source rbn', () => {
    const arcs = buildHeardMeArcs(station, [],
      [{ skimmer: 'N4ZR', lat: 39.0, lon: -77.0, freqKhz: 14025, band: '20m', mode: 'CW', snr: 25, ageSeconds: 60 }],
      { band: '20m' });
    expect(arcs).toHaveLength(1);
    expect(arcs[0].source).toBe('rbn');
    expect(arcs[0].endLat).toBeCloseTo(39.0);
  });

  it('caps and sorts by SNR/recency', () => {
    const rbn = Array.from({ length: 300 }, (_, i) => ({
      skimmer: `S${i}`, lat: i % 80, lon: i % 80, freqKhz: 14025, band: '20m', mode: 'CW', snr: i, ageSeconds: 0,
    }));
    const arcs = buildHeardMeArcs(station, [], rbn, { band: '20m', cap: 150 });
    expect(arcs.length).toBe(150);
    expect(arcs[0].snr).toBeGreaterThanOrEqual(arcs[149].snr); // strongest first
  });

  it('caps each source independently so RBN cannot evict PSK arcs', () => {
    // RBN SNR (dB) typically runs much higher than PSK/FT8 SNR (dB, often negative),
    // so a combined sort+cap would let RBN evict every PSK arc. Verify both sources
    // survive at their own cap when each has more than `cap` reports.
    const psk = Array.from({ length: 200 }, (_, i) => ({
      senderCallsign: 'K1ABC', senderLocator: 'EN53', receiverCallsign: `PSK${i}`,
      receiverLocator: 'FM19', frequencyHz: 14074000, mode: 'FT8', snr: -20 + (i % 10), flowStartSeconds: 0,
    }));
    const rbn = Array.from({ length: 200 }, (_, i) => ({
      skimmer: `RBN${i}`, lat: i % 80, lon: i % 80, freqKhz: 14025, band: '20m', mode: 'CW', snr: i, ageSeconds: 0,
    }));
    const arcs = buildHeardMeArcs(station, psk, rbn, { band: '20m', cap: 150 });
    expect(arcs.length).toBe(300); // 150 psk + 150 rbn
    const pskCount = arcs.filter((a) => a.source === 'psk').length;
    const rbnCount = arcs.filter((a) => a.source === 'rbn').length;
    expect(pskCount).toBe(150);
    expect(rbnCount).toBe(150);
  });
});
