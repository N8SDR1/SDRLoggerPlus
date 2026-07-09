import { describe, it, expect, afterEach } from 'vitest';
import {
  PRESETS,
  PRESET_IDS,
  buildVariables,
  applyThemeToDocument,
  getSeedColors,
  hexToTriplet,
  mixHex,
  type CustomColors,
} from './themes';

const REQUIRED_VARS = [
  '--surface-900', '--surface-850', '--surface-800', '--surface-700',
  '--surface-600', '--surface-500', '--surface-400', '--surface-300', '--surface-200',
  '--accent-primary', '--accent-secondary',
  '--gray-50', '--gray-100', '--gray-200', '--gray-300', '--gray-400',
  '--gray-500', '--gray-600', '--gray-700', '--gray-800', '--gray-900',
  '--text-foreground',
  '--scrollbar-track', '--scrollbar-thumb', '--scrollbar-thumb-hover',
];

describe('color helpers', () => {
  it('converts hex to an "r g b" triplet', () => {
    expect(hexToTriplet('#00e5ff')).toBe('0 229 255');
    expect(hexToTriplet('#0a0d12')).toBe('10 13 18');
    expect(hexToTriplet('FFFFFF')).toBe('255 255 255');
  });

  it('mixes two hex colors', () => {
    expect(mixHex('#000000', '#ffffff', 0)).toBe('#000000');
    expect(mixHex('#000000', '#ffffff', 1)).toBe('#ffffff');
    expect(mixHex('#000000', '#ffffff', 0.5)).toBe('#808080');
  });
});

describe('PRESETS', () => {
  it('defines the agreed lineup', () => {
    expect(PRESET_IDS).toEqual(['nightops', 'lyra', 'midnight', 'dracula', 'nord', 'blurple']);
  });

  it('Night Ops carries the SDRLogger-derived palette', () => {
    const p = PRESETS.nightops;
    expect(p.bg).toBe('#0a0d12');
    expect(p.accent).toBe('#00e5ff');
    expect(p.text).toBe('#cdd9e5');
  });

  it('every preset builds a complete variable map', () => {
    for (const id of PRESET_IDS) {
      const vars = buildVariables(PRESETS[id]);
      for (const key of REQUIRED_VARS) {
        expect(vars[key], `${id} missing ${key}`).toBeTruthy();
      }
    }
  });
});

describe('buildVariables', () => {
  const custom: CustomColors = {
    accent: '#00e5ff',
    background: '#0a0d12',
    panel: '#111620',
    text: '#cdd9e5',
  };

  it('maps the four custom colors onto the core variables', () => {
    const vars = buildVariables({
      bg: custom.background,
      panel: custom.panel,
      text: custom.text,
      accent: custom.accent,
    });
    expect(vars['--surface-900']).toBe('10 13 18');
    expect(vars['--surface-800']).toBe('17 22 32');
    expect(vars['--surface-200']).toBe('205 217 229');
    expect(vars['--accent-primary']).toBe('0 229 255');
    expect(vars['--text-foreground']).toBe('205 217 229');
  });

  it('derives missing palette entries (border, muted, secondary accent)', () => {
    const vars = buildVariables({ bg: '#101010', panel: '#202020', text: '#e0e0e0', accent: '#ff0000' });
    expect(vars['--surface-600']).toBeTruthy(); // border derived
    expect(vars['--gray-500']).toBeTruthy();    // muted derived
    expect(vars['--accent-secondary']).toBeTruthy();
  });

  it('scrollbar colors are hex (not triplets)', () => {
    const vars = buildVariables(PRESETS.nightops);
    expect(vars['--scrollbar-track']).toMatch(/^#[0-9a-f]{6}$/i);
  });
});

describe('applyThemeToDocument', () => {
  const html = () => document.documentElement;
  const customColors: CustomColors = {
    accent: '#ff0000', background: '#101010', panel: '#202020', text: '#e0e0e0',
  };

  afterEach(() => {
    applyThemeToDocument('dark', customColors); // reset
  });

  it('built-in dark: dark class on, no inline variables', () => {
    applyThemeToDocument('dark', customColors);
    expect(html().classList.contains('dark')).toBe(true);
    expect(html().style.getPropertyValue('--accent-primary')).toBe('');
  });

  it('light: dark class off, no inline variables', () => {
    applyThemeToDocument('light', customColors);
    expect(html().classList.contains('dark')).toBe(false);
    expect(html().style.getPropertyValue('--accent-primary')).toBe('');
  });

  it('preset: dark class on + inline variables set', () => {
    applyThemeToDocument('nightops', customColors);
    expect(html().classList.contains('dark')).toBe(true);
    expect(html().style.getPropertyValue('--accent-primary')).toBe('0 229 255');
    expect(html().style.getPropertyValue('--surface-900')).toBe('10 13 18');
  });

  it('custom: uses the user colors', () => {
    applyThemeToDocument('custom', customColors);
    expect(html().style.getPropertyValue('--accent-primary')).toBe('255 0 0');
  });

  it('switching back to dark clears preset variables', () => {
    applyThemeToDocument('nightops', customColors);
    applyThemeToDocument('dark', customColors);
    expect(html().style.getPropertyValue('--accent-primary')).toBe('');
    expect(html().style.getPropertyValue('--surface-900')).toBe('');
  });
});

describe('getSeedColors', () => {
  it('seeds from a preset palette', () => {
    expect(getSeedColors('nightops')).toEqual({
      accent: '#00e5ff', background: '#0a0d12', panel: '#111620', text: '#cdd9e5',
    });
  });

  it('seeds from built-in dark', () => {
    const seeds = getSeedColors('dark');
    expect(seeds.accent.toLowerCase()).toBe('#e08a3d');
    expect(seeds.background.toLowerCase()).toBe('#141417');
  });
});
