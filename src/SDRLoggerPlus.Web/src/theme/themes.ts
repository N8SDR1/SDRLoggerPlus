/**
 * Theme engine: preset palettes + custom colors on top of the CSS-variable system.
 *
 * 'light' and 'dark' are the hand-tuned CSS themes in index.css and are applied purely
 * via the `dark` class. Presets and Custom apply the `dark` class as a base, then
 * override the themable variables inline on <html>. Everything downstream (gradients,
 * shadows, glass tints) derives from these variables, so a small palette retunes the
 * whole app. Semantic accents (success/warning/danger/info) and ham-mode colors are
 * deliberately NOT overridden — they keep their meaning across themes.
 */

export type ThemeId =
  | 'light'
  | 'dark'
  | 'nightops'
  | 'midnight'
  | 'dracula'
  | 'nord'
  | 'blurple'
  | 'custom';

export interface CustomColors {
  accent: string;
  background: string;
  panel: string;
  text: string;
}

/** A preset palette. Only bg/panel/text/accent are required; the rest are derived. */
export interface ThemePalette {
  bg: string;
  panel: string;
  text: string;
  accent: string;
  surfaceAlt?: string;
  card?: string;
  border?: string;
  muted?: string;
  accent2?: string;
}

// ─── color helpers ───────────────────────────────────────────────────────────

function parseHex(hex: string): [number, number, number] {
  const h = hex.replace('#', '');
  return [
    parseInt(h.slice(0, 2), 16),
    parseInt(h.slice(2, 4), 16),
    parseInt(h.slice(4, 6), 16),
  ];
}

/** "#rrggbb" → "r g b" (the form the CSS variables use). */
export function hexToTriplet(hex: string): string {
  return parseHex(hex).join(' ');
}

/** Linear mix of two hex colors; t=0 → a, t=1 → b. */
export function mixHex(a: string, b: string, t: number): string {
  const ca = parseHex(a);
  const cb = parseHex(b);
  const out = ca.map((v, i) => Math.round(v + (cb[i] - v) * t));
  return '#' + out.map((v) => v.toString(16).padStart(2, '0')).join('');
}

// ─── presets ─────────────────────────────────────────────────────────────────

export const PRESETS: Record<string, ThemePalette> = {
  // SDRLogger-derived cyan-on-navy console look (renamed at the user's direction).
  nightops: {
    bg: '#0a0d12', surfaceAlt: '#0e1219', panel: '#111620', card: '#161c28',
    border: '#1e2a3a', text: '#cdd9e5', muted: '#5a7080',
    accent: '#00e5ff', accent2: '#39ff14',
  },
  // GitHub-Dark / VS Code Dark+ family.
  midnight: {
    bg: '#0d1117', panel: '#161b22', border: '#30363d',
    text: '#e6edf3', muted: '#8b949e', accent: '#58a6ff',
  },
  dracula: {
    bg: '#21222c', panel: '#282a36', border: '#44475a',
    text: '#f8f8f2', muted: '#6272a4', accent: '#bd93f9', accent2: '#ff79c6',
  },
  nord: {
    bg: '#2e3440', panel: '#3b4252', card: '#434c5e', border: '#4c566a',
    text: '#eceff4', muted: '#7b88a1', accent: '#88c0d0', accent2: '#81a1c1',
  },
  // Discord-family indigo on neutral dark gray.
  blurple: {
    bg: '#1e1f22', panel: '#2b2d31', card: '#313338', border: '#3f4147',
    text: '#f2f3f5', muted: '#949ba4', accent: '#5865f2',
  },
};

export const PRESET_IDS = ['nightops', 'midnight', 'dracula', 'nord', 'blurple'] as const;

/** Base colors of the built-in themes, used to seed the Custom pickers. */
const BUILTIN_SEEDS: Record<'light' | 'dark', CustomColors> = {
  dark: { accent: '#e08a3d', background: '#141417', panel: '#27272b', text: '#e4e4e7' },
  light: { accent: '#c05216', background: '#eceae6', panel: '#faf9f7', text: '#201e1a' },
};

// ─── variable derivation ─────────────────────────────────────────────────────

/** Builds the full inline CSS-variable map for a palette. */
export function buildVariables(p: ThemePalette): Record<string, string> {
  const surfaceAlt = p.surfaceAlt ?? mixHex(p.bg, p.panel, 0.45);
  const card = p.card ?? mixHex(p.panel, p.text, 0.04);
  const border = p.border ?? mixHex(p.panel, p.text, 0.16);
  const muted = p.muted ?? mixHex(p.bg, p.text, 0.55);
  const accent2 = p.accent2 ?? mixHex(p.accent, p.text, 0.3);

  const t = hexToTriplet;
  return {
    // Surface scale: deepest canvas → primary text weight
    '--surface-900': t(p.bg),
    '--surface-850': t(surfaceAlt),
    '--surface-800': t(p.panel),
    '--surface-700': t(card),
    '--surface-600': t(border),
    '--surface-500': t(mixHex(border, muted, 0.4)),
    '--surface-400': t(muted),
    '--surface-300': t(mixHex(muted, p.text, 0.45)),
    '--surface-200': t(p.text),

    '--accent-primary': t(p.accent),
    '--accent-secondary': t(accent2),

    // Gray/text scale
    '--gray-50': t(mixHex(p.text, '#ffffff', 0.5)),
    '--gray-100': t(p.text),
    '--gray-200': t(mixHex(p.text, p.bg, 0.1)),
    '--gray-300': t(mixHex(p.text, p.bg, 0.2)),
    '--gray-400': t(mixHex(muted, p.text, 0.45)),
    '--gray-500': t(muted),
    '--gray-600': t(mixHex(muted, p.bg, 0.45)),
    '--gray-700': t(mixHex(border, p.bg, 0.3)),
    '--gray-800': t(p.panel),
    '--gray-900': t(p.bg),

    '--text-foreground': t(p.text),

    // Scrollbar (hex form — used directly, not via rgb())
    '--scrollbar-track': p.bg,
    '--scrollbar-thumb': border,
    '--scrollbar-thumb-hover': mixHex(border, p.text, 0.25),
  };
}

function paletteFor(theme: ThemeId, customColors: CustomColors): ThemePalette | null {
  if (theme === 'custom') {
    return {
      bg: customColors.background,
      panel: customColors.panel,
      text: customColors.text,
      accent: customColors.accent,
    };
  }
  return PRESETS[theme] ?? null;
}

/** All variable names we may set inline, for cleanup when switching themes. */
const ALL_VAR_NAMES = Object.keys(buildVariables(PRESETS.nightops));

/** Applies a theme to <html>: class toggle + inline variable overrides. */
export function applyThemeToDocument(theme: ThemeId, customColors: CustomColors): void {
  const html = document.documentElement;

  // Light is the only non-dark base; presets and custom build on dark.
  if (theme === 'light') {
    html.classList.remove('dark');
  } else {
    html.classList.add('dark');
  }

  const palette = paletteFor(theme, customColors);
  if (palette) {
    const vars = buildVariables(palette);
    for (const [name, value] of Object.entries(vars)) {
      html.style.setProperty(name, value);
    }
  } else {
    for (const name of ALL_VAR_NAMES) {
      html.style.removeProperty(name);
    }
  }
}

/** Returns the four base colors of a theme, to seed the Custom pickers. */
export function getSeedColors(theme: ThemeId): CustomColors {
  if (theme === 'light' || theme === 'dark') {
    return { ...BUILTIN_SEEDS[theme] };
  }
  const p = PRESETS[theme];
  if (p) {
    return { accent: p.accent, background: p.bg, panel: p.panel, text: p.text };
  }
  return { ...BUILTIN_SEEDS.dark };
}
