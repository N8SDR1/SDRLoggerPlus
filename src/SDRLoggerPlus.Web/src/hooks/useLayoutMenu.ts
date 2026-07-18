import { useCallback, useEffect } from 'react';
import { Model } from 'flexlayout-react';
import { api } from '../api/client';
import { useLayoutStore } from '../store/layoutStore';
import { useSettingsStore } from '../store/settingsStore';
import { useAppStore } from '../store/appStore';

// Which preset is currently loaded, for the menu's radio check. The backend
// stores the presets but not which one is in use (the live layout is persisted
// as JSON, not by name), so track it here.
const ACTIVE_KEY = 'sdrl_active_layout';

/** Tell the rest of the app the saved-layout list changed, so the menu refreshes. */
export const LAYOUTS_CHANGED_EVENT = 'sdrl-layouts-changed';
export function notifyLayoutsChanged(activeName?: string | null) {
  if (activeName !== undefined) {
    if (activeName) localStorage.setItem(ACTIVE_KEY, activeName);
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
    try {
      const list = await api.getSavedLayouts();
      const stored = localStorage.getItem(ACTIVE_KEY);
      // Only claim an active preset if it still exists (it may have been deleted).
      const active = stored && list.some((l) => l.name === stored) ? stored : null;
      await window.electronAPI.notifyLayouts(list.map((l) => l.name), active);
    } catch {
      // A failed push just leaves the menu showing its last known list.
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

    electron.onApplyLayout(async (name: string) => {
      try {
        const list = await api.getSavedLayouts();
        const slot = list.find((l) => l.name === name);
        if (!slot) return;
        const json = JSON.parse(slot.layoutJson);
        Model.fromJson(json); // throws if the stored JSON isn't a valid model
        setLayout(json);
        notifyLayoutsChanged(name);
      } catch (e) {
        console.error(`[layout-menu] failed to apply "${name}"`, e);
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
