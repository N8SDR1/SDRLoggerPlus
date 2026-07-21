import { useEffect } from 'react';
import { useShortcutStore } from '../store/shortcutStore';
import type { Shortcut } from '../utils/shortcuts';

/**
 * Declare the keyboard shortcuts a panel provides. They appear in the `?`
 * overlay while the panel is mounted and vanish when it closes.
 *
 * Declare the `shortcuts` array as a module-level constant rather than an
 * inline literal — a fresh array each render would re-register on every pass.
 */
export function useRegisterShortcuts(scope: string, shortcuts: Shortcut[]): void {
  const register = useShortcutStore((s) => s.register);
  const unregister = useShortcutStore((s) => s.unregister);

  useEffect(() => {
    register(scope, shortcuts);
    return () => unregister(scope);
  }, [scope, shortcuts, register, unregister]);
}
