import { describe, it, expect } from 'vitest';
import { rigModeToSpotModes } from '../../utils/rigTracking';

describe('rigModeToSpotModes', () => {
  it('maps phone modes to SSB', () => {
    expect(rigModeToSpotModes('USB')).toEqual(['SSB']);
    expect(rigModeToSpotModes('LSB')).toEqual(['SSB']);
    expect(rigModeToSpotModes('AM')).toEqual(['SSB']);
    expect(rigModeToSpotModes('FM')).toEqual(['SSB']);
  });

  it('maps CW variants to CW', () => {
    expect(rigModeToSpotModes('CW')).toEqual(['CW']);
    expect(rigModeToSpotModes('CW-R')).toEqual(['CW']);
  });

  it('maps FT8 and FT4 to themselves', () => {
    expect(rigModeToSpotModes('FT8')).toEqual(['FT8']);
    expect(rigModeToSpotModes('FT4')).toEqual(['FT4']);
  });

  it('maps RTTY to RTTY', () => {
    expect(rigModeToSpotModes('RTTY')).toEqual(['RTTY']);
  });

  it('maps generic digital rig modes to the digital family', () => {
    expect(rigModeToSpotModes('DIGU')).toEqual(['FT8', 'FT4', 'DIGI']);
    expect(rigModeToSpotModes('DATA')).toEqual(['FT8', 'FT4', 'DIGI']);
    expect(rigModeToSpotModes('PKT')).toEqual(['FT8', 'FT4', 'DIGI']);
  });

  it('is case-insensitive and trims', () => {
    expect(rigModeToSpotModes(' usb ')).toEqual(['SSB']);
    expect(rigModeToSpotModes('cw')).toEqual(['CW']);
  });

  it('returns null for unknown or empty modes (no mode filter)', () => {
    expect(rigModeToSpotModes('')).toBeNull();
    expect(rigModeToSpotModes(undefined)).toBeNull();
    expect(rigModeToSpotModes('SSTV')).toBeNull();
  });
});
