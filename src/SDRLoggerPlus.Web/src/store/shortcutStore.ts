import { create } from 'zustand';
import type { Shortcut, ShortcutRegistrations } from '../utils/shortcuts';

/**
 * Live registry of keyboard shortcuts, keyed by scope (usually a panel name).
 * Panels register on mount and unregister on unmount via useRegisterShortcuts,
 * so the `?` overlay always reflects the panels currently open.
 *
 * Registering an existing scope replaces it, which keeps the operation
 * idempotent under React strict-mode double effects.
 */
interface ShortcutState {
  registrations: ShortcutRegistrations;
  register: (scope: string, shortcuts: Shortcut[]) => void;
  unregister: (scope: string) => void;
}

export const useShortcutStore = create<ShortcutState>((set) => ({
  registrations: {},
  register: (scope, shortcuts) =>
    set((s) => ({ registrations: { ...s.registrations, [scope]: shortcuts } })),
  unregister: (scope) =>
    set((s) => {
      if (!(scope in s.registrations)) return s;
      const next = { ...s.registrations };
      delete next[scope];
      return { registrations: next };
    }),
}));
