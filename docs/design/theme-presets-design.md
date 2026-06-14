# Theme Presets & Custom Colors — Design

**Date:** 2026-06-11
**User decisions:** customizable accent/background/panels/text; preset palettes drawn
from looks people know in today's apps, including one matching SDRLogger+ (named
**Night Ops** at the user's direction — no other product's name in the UI); one unified
Theme picker (a preset IS a theme); Custom unlocks four pickers seeded from the active
theme.

## Theme model

`AppearanceSettings.theme` becomes a `ThemeId`:
`'light' | 'dark' | 'nightops' | 'midnight' | 'dracula' | 'nord' | 'blurple' | 'custom'`
(backend `Theme` is already a plain string — no migration). New
`appearance.customColors: { accent, background, panel, text }` (hex), persisted via a
`customColors` dictionary on the backend `AppearanceSettings`.

## Rendering mechanism

The app is already fully variable-driven: `index.css` defines `--surface-*`,
`--accent-*`, `--gray-*`, `--text-foreground`, scrollbar colors per theme class, and all
gradients/shadows derive from those. So:

- `light`/`dark`: unchanged — the existing CSS classes, no inline variables.
- Presets/custom: apply the `dark` class as the base (all presets are dark), then set
  inline CSS variables on `<html>` via a new `theme/themes.ts` module. Switching back
  to light/dark clears the inline variables.

`themes.ts` exports:
- `PRESETS`: per-preset palette (bg, surfaceAlt, panel, card, border, text, muted,
  accent, accent2) using authentic hexes:
  - **Night Ops** (from SDRLogger+ `:root`): bg `#0a0d12`, panel `#111620`, card
    `#161c28`, border `#1e2a3a`, text `#cdd9e5`, muted `#5a7080`, accent `#00e5ff`,
    accent2 `#39ff14`.
  - **Midnight** (GitHub-Dark/VS Code family): bg `#0d1117`, panel `#161b22`,
    border `#30363d`, text `#e6edf3`, muted `#8b949e`, accent `#58a6ff`.
  - **Dracula**: bg `#21222c`, panel `#282a36`, border `#44475a`, text `#f8f8f2`,
    muted `#6272a4`, accent `#bd93f9`, accent2 `#ff79c6`.
  - **Nord**: bg `#2e3440`, panel `#3b4252`, card `#434c5e`, border `#4c566a`,
    text `#eceff4`, muted `#7b88a1`, accent `#88c0d0`, accent2 `#81a1c1`.
  - **Blurple** (Discord family): bg `#1e1f22`, panel `#2b2d31`, card `#313338`,
    border `#3f4147`, text `#f2f3f5`, muted `#949ba4`, accent `#5865f2`.
- `buildVariables(palette)`: derives the full CSS-variable map (surface scale 900→200,
  gray scale, text-foreground, accent primary/secondary, scrollbar) — missing palette
  entries (card/border/muted/surfaceAlt/accent2) are interpolated with a color `mix()`.
  Semantic accents (success/warning/danger/info) and ham-mode colors stay from `.dark`.
- `applyThemeToDocument(theme, customColors)` and `getSeedColors(theme)` (seeds the
  Custom pickers from the previously active theme).

Custom = `buildVariables` over exactly the four picked colors, everything else derived.

## UI

The Appearance section's existing theme-card grid grows from 2 cards to 8 (each preset
card shows its real bg/panel/accent swatches). Selecting **Custom** seeds
`customColors` from the current theme and reveals four color rows (native color input +
hex text input): Accent, Background, Panels, Text.

## Testing

`themes.test.ts`: hex/mix helpers, every preset builds a complete variable map,
apply/clear behavior on jsdom `documentElement`. Update `settingsStore.test.ts`
defaults. Backend: none beyond the settings round-trip (dictionary field).

## Out of scope

Hardcoded canvas/overlay colors (globe beam ambers, map markers) — they don't read CSS
variables; revisit only if presets make them clash badly.
