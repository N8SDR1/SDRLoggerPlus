/**
 * Keyboard-shortcut registry types and pure helpers.
 *
 * Panels declare their bindings next to the handler that implements them, so
 * the `?` overlay is generated from the registry rather than a hand-maintained
 * list that silently rots. Registration is scoped per panel and lives only
 * while that panel is mounted — the overlay shows what is actually available
 * right now, not everything the app could theoretically do.
 */

export interface Shortcut {
  /** Display form of the chord, e.g. "Enter", "Shift+Enter", "?". */
  keys: string;
  /** What it does, phrased as an action: "Log the QSO". */
  label: string;
}

/** Scope name for app-wide bindings; sorted to the top of the overlay. */
export const GLOBAL_SCOPE = 'Global';

export type ShortcutRegistrations = Record<string, Shortcut[]>;

export interface ShortcutGroup {
  scope: string;
  shortcuts: Shortcut[];
}

/**
 * Registry → display order: Global first, then scopes alphabetically.
 * Empty scopes are dropped so a panel that registers nothing adds no heading.
 */
export function groupShortcuts(registrations: ShortcutRegistrations): ShortcutGroup[] {
  return Object.entries(registrations)
    .filter(([, list]) => list.length > 0)
    .sort(([a], [b]) => {
      if (a === b) return 0;
      if (a === GLOBAL_SCOPE) return -1;
      if (b === GLOBAL_SCOPE) return 1;
      return a.localeCompare(b);
    })
    .map(([scope, shortcuts]) => ({ scope, shortcuts }));
}

/**
 * True when the event target is somewhere the operator is typing. Global keys
 * must not fire mid-callsign — "?" is a character before it is a command.
 */
export function isEditableTarget(target: EventTarget | null): boolean {
  const el = target as HTMLElement | null;
  if (!el || typeof el !== 'object' || !('tagName' in el)) return false;
  if (el.isContentEditable) return true;
  const tag = el.tagName;
  return tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT';
}

/** Minimal shape of a keydown, so the chord test is unit-testable. */
export interface KeyChordLike {
  key: string;
  ctrlKey?: boolean;
  metaKey?: boolean;
  target?: EventTarget | null;
}

/**
 * Does this keystroke toggle the shortcut overlay?
 *
 * Two chords on purpose. Plain "?" is the discoverable one, but it must yield
 * to typing — and the Log Entry callsign field takes focus on startup, so "?"
 * alone would be unreachable on a fresh window. Ctrl+/ (or Cmd+/) types no
 * character, so it stays available even mid-callsign.
 */
export function isHelpChord(e: KeyChordLike): boolean {
  if (e.key === '/' && (e.ctrlKey || e.metaKey)) return true;
  return e.key === '?' && !isEditableTarget(e.target ?? null);
}
