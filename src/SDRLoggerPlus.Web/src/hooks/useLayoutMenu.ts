import { useCallback, useEffect } from 'react';
import { Model } from 'flexlayout-react';
import { api } from '../api/client';
import { useLayoutStore } from '../store/layoutStore';
import { useSettingsStore } from '../store/settingsStore';
import { useAppStore } from '../store/appStore';
import { STARTER_LAYOUTS, findStarterLayout } from '../layouts/starterLayouts';

// Which layout is currently loaded, for the menu's radio check. The backend
// stores the presets but not which one is in use (the live layout is persisted
// as JSON, not by name), so track it here. Stored as "starter:Contest" /
// "saved:Contest" because a built-in and a user preset may share a name.
export const ACTIVE_KEY = 'sdrl_active_layout';

/** Tell the rest of the app the saved-layout list changed, so the menu refreshes. */
export const LAYOUTS_CHANGED_EVENT = 'sdrl-layouts-changed';

/**
 * @param active "starter:Name" / "saved:Name" to mark one as loaded, null to
 * clear the mark, or undefined to leave it as-is (e.g. after a delete).
 */
export function notifyLayoutsChanged(active?: string | null) {
  if (active !== undefined) {
    if (active) localStorage.setItem(ACTIVE_KEY, active);
    else localStorage.removeItem(ACTIVE_KEY);
  }
  window.dispatchEvent(new Event(LAYOUTS_CHANGED_EVENT));
}

/**
 * Wires the Electron View > Layouts menu to the app: pushes the saved-preset
 * list up so the native menu can list them, and handles the three actions it
 * sends back (apply a preset, save the current one, reset to default).
 *
 * No-ops in a browser, where window.electronAPI is undefined — the Settings >
 * Appearance > Layout Presets panel remains the canonical UI in both cases.
 */
export function useLayoutMenu() {
  const setLayout = useLayoutStore((s) => s.setLayout);
  const resetLayout = useLayoutStore((s) => s.resetLayout);

  const pushLayouts = useCallback(async () => {
    if (!window.electronAPI?.notifyLayouts) return;
    const starters = STARTER_LAYOUTS.map((l) => l.name);
    try {
      const list = await api.getSavedLayouts();
      const stored = localStorage.getItem(ACTIVE_KEY);
      // Only claim an active layout if it still exists (a preset may have been
      // deleted since it was loaded).
      const known =
        stored?.startsWith('starter:')
          ? starters.includes(stored.slice(8))
          : list.some((l) => `saved:${l.name}` === stored);
      await window.electronAPI.notifyLayouts(
        list.map((l) => l.name),
        starters,
        known ? stored : null,
      );
    } catch {
      // Backend unreachable — still offer the built-ins, which need no server.
      await window.electronAPI.notifyLayouts([], starters, null).catch(() => {});
    }
  }, []);

  // Seed the menu, and refresh it whenever a preset is saved or deleted.
  useEffect(() => {
    pushLayouts();
    const handler = () => { pushLayouts(); };
    window.addEventListener(LAYOUTS_CHANGED_EVENT, handler);
    return () => window.removeEventListener(LAYOUTS_CHANGED_EVENT, handler);
  }, [pushLayouts]);

  useEffect(() => {
    const electron = window.electronAPI;
    if (!electron?.onApplyLayout) return;

    electron.onApplyLayout(async ({ kind, name }) => {
      try {
        let json: unknown;
        if (kind === 'starter') {
          const starter = findStarterLayout(name);
          if (!starter) return;
          json = starter.layout;
        } else {
          const list = await api.getSavedLayouts();
          const slot = list.find((l) => l.name === name);
          if (!slot) return;
          json = JSON.parse(slot.layoutJson);
        }
        Model.fromJson(json as Parameters<typeof Model.fromJson>[0]); // throws on a bad model
        setLayout(json as Parameters<typeof setLayout>[0]);
        notifyLayoutsChanged(`${kind}:${name}`);
      } catch (e) {
        console.error(`[layout-menu] failed to apply ${kind} "${name}"`, e);
      }
    });

    // Electron has no prompt(), so naming happens in the renderer: open the
    // Layout Presets panel and ask it to focus its name input.
    electron.onSaveLayout?.(() => {
      useSettingsStore.getState().openSettings();
      useSettingsStore.getState().setActiveSection('appearance');
      useAppStore.getState().requestLayoutSave();
    });

    electron.onResetLayout?.(() => {
      resetLayout();
      notifyLayoutsChanged(null);
    });

    return () => electron.removeLayoutMenuListeners?.();
  }, [setLayout, resetLayout]);
}
