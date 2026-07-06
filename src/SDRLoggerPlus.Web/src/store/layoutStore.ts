import { create } from 'zustand';
import type { IJsonModel } from 'flexlayout-react';

// Default layout configuration
export const defaultLayout: IJsonModel = {
  global: {
    tabEnableFloat: false,
    tabSetMinWidth: 100,
    tabSetMinHeight: 40,
    borderMinSize: 100,
  },
  borders: [],
  layout: {
    type: 'row',
    weight: 100,
    children: [
      {
        type: 'row',
        weight: 100,
        children: [
          // Header bar across the top (thin strip)
          {
            type: 'tabset',
            weight: 6,
            children: [
              {
                type: 'tab',
                name: 'Header Bar',
                component: 'header-bar',
                enableClose: false,
              },
            ],
          },
          // Main content area below
          {
            type: 'row',
            weight: 94,
            children: [
              {
                type: 'tabset',
                weight: 30,
                children: [
                  {
                    type: 'tab',
                    name: 'Log Entry',
                    component: 'log-entry',
                  },
                  {
                    type: 'tab',
                    name: '3D Globe',
                    component: 'globe-3d',
                  },
                ],
              },
              {
                type: 'row',
                weight: 70,
                children: [
                  {
                    type: 'tabset',
                    weight: 60,
                    children: [
                      {
                        type: 'tab',
                        name: 'Log History',
                        component: 'log-history',
                      },
                      {
                        type: 'tab',
                        name: 'Rig',
                        component: 'rig',
                      },
                    ],
                  },
                  {
                    type: 'tabset',
                    weight: 40,
                    children: [
                      {
                        type: 'tab',
                        name: 'DX Cluster',
                        component: 'cluster',
                      },
                    ],
                  },
                ],
              },
            ],
          },
        ],
      },
    ],
  },
};

interface LayoutState {
  // Layout data
  layout: IJsonModel;
  isLoaded: boolean;
  // True once loadFromBackend has finished (whether it found a saved
  // layout or not). We refuse to persist any user-driven layout changes
  // before this flips — otherwise a stray FlexLayout initial onModelChange
  // emit (or an early user drag) can overwrite the just-loaded layout with
  // the pre-load default via the App.tsx 1 s debounce.
  hasEverLoaded: boolean;

  // Actions
  setLayout: (layout: IJsonModel) => void;
  resetLayout: () => void;
  syncToBackend: (layout: IJsonModel) => Promise<void>;
  syncToBackendSync: (layout: IJsonModel) => void;
  loadFromBackend: () => Promise<void>;
  setNotLoaded: () => void;
}

// Layout is persisted on the backend (LiteDB), not localStorage, so it survives
// app upgrades and reinstalls.
export const useLayoutStore = create<LayoutState>()((set, get) => ({
  layout: defaultLayout,
  isLoaded: false,
  hasEverLoaded: false,

  setLayout: (layout) => {
    // Guard: if a save fires before the initial load has completed, the
    // pre-load state would overwrite whatever's saved on the backend.
    // Skip the save but still update the local state so the UI reacts.
    if (!get().hasEverLoaded) {
      console.warn('[layoutStore] setLayout before initial load — updating locally only, not persisting');
      set({ layout, isLoaded: true });
      return;
    }
    set({ layout, isLoaded: true });
    // Sync to backend in background
    get().syncToBackend(layout);
  },

  resetLayout: () => {
    set({ layout: defaultLayout, isLoaded: true });
    get().syncToBackend(defaultLayout);
  },

  // Sync layout to backend (background, non-blocking)
  syncToBackend: async (layout) => {
    const { isLoaded } = get();
    if (!isLoaded) {
      console.warn('[layoutStore] Layout not loaded yet, skipping save to prevent stale data');
      return;
    }

    try {
      console.log('[layoutStore] Syncing layout to backend');
      const layoutJson = JSON.stringify(layout);
      const response = await fetch('/api/settings/layout', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(layoutJson),
      });

      if (response.ok) {
        console.log('[layoutStore] Layout saved successfully');
      } else {
        console.error('[layoutStore] Failed to save layout, status:', response.status);
      }
    } catch (e) {
      console.error('[layoutStore] Failed to sync layout to backend:', e);
    }
  },

  // Sync layout to backend synchronously (for beforeunload)
  // Uses sendBeacon for reliable shutdown saves
  syncToBackendSync: (layout) => {
    const { isLoaded } = get();
    if (!isLoaded) {
      console.warn('[layoutStore] Layout not loaded yet, skipping save to prevent stale data');
      return;
    }

    try {
      console.log('[layoutStore] Syncing layout to backend (sync)');
      const layoutJson = JSON.stringify(layout);

      // Try sendBeacon first (best for beforeunload)
      // Note: sendBeacon doesn't support custom headers, so we send the raw JSON string
      // The server endpoint expects a JSON string (not an object), so this works correctly
      if (navigator.sendBeacon) {
        const blob = new Blob([JSON.stringify(layoutJson)], { type: 'application/json' });
        const sent = navigator.sendBeacon('/api/settings/layout', blob);
        if (sent) {
          console.log('[layoutStore] Layout sent via sendBeacon');
          return;
        }
      }

      // Fallback: synchronous XHR (not ideal, but works)
      const xhr = new XMLHttpRequest();
      xhr.open('PUT', '/api/settings/layout', false); // false = synchronous
      xhr.setRequestHeader('Content-Type', 'application/json');
      xhr.send(JSON.stringify(layoutJson));
      console.log('[layoutStore] Layout saved synchronously via XHR');
    } catch (e) {
      console.error('[layoutStore] Failed to sync layout synchronously:', e);
    }
  },

  // Load layout from backend on app startup. Always flips hasEverLoaded
  // to true on completion (success or failure) so setLayout is unblocked
  // for the rest of the session. Without this flip, setLayout stays in
  // "local-only" guard mode forever and no drag ever gets persisted.
  loadFromBackend: async () => {
    try {
      console.log('[layoutStore] Loading layout from backend');
      const response = await fetch('/api/settings');
      if (response.ok) {
        const settings = await response.json();
        if (settings.layoutJson) {
          const layout = JSON.parse(settings.layoutJson);
          console.log('[layoutStore] Layout loaded successfully');
          set({ layout, isLoaded: true, hasEverLoaded: true });
        } else {
          console.log('[layoutStore] No saved layout found, using default');
          set({ isLoaded: true, hasEverLoaded: true });
        }
      } else {
        console.error('[layoutStore] Failed to load layout, status:', response.status);
        set({ isLoaded: true, hasEverLoaded: true });
      }
    } catch (e) {
      console.error('[layoutStore] Failed to load layout from backend:', e);
      set({ isLoaded: true, hasEverLoaded: true });
    }
  },

  // Mark layout as not loaded (e.g., during disconnection)
  setNotLoaded: () => {
    console.log('[layoutStore] Marking layout as not loaded');
    set({ isLoaded: false });
  },
}));
