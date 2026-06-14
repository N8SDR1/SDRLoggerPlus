import { useCallback, useSyncExternalStore } from 'react';

// Per-panel "compact density" preference. Each panel owns an independent flag
// keyed by a stable id; the choice persists across restarts in localStorage and
// is shared between any components observing the same id within the session.
const PREFIX = 'panelCompact:';
const listeners = new Set<() => void>();

function read(id: string): boolean {
  try {
    return localStorage.getItem(PREFIX + id) === '1';
  } catch {
    return false;
  }
}

function write(id: string, value: boolean): void {
  try {
    if (value) localStorage.setItem(PREFIX + id, '1');
    else localStorage.removeItem(PREFIX + id);
  } catch {
    /* ignore quota / privacy-mode failures */
  }
  listeners.forEach((l) => l());
}

export function usePanelCompact(id: string): [boolean, () => void] {
  const subscribe = useCallback((cb: () => void) => {
    listeners.add(cb);
    return () => {
      listeners.delete(cb);
    };
  }, []);

  const compact = useSyncExternalStore(
    subscribe,
    () => read(id),
    () => false,
  );

  const toggle = useCallback(() => write(id, !read(id)), [id]);

  return [compact, toggle];
}
