import { describe, it, expect, vi, beforeEach } from 'vitest';
import { useLayoutStore, defaultLayout } from '../../store/layoutStore';
import { STARTER_LAYOUTS } from '../../layouts/starterLayouts';
import type { IJsonModel } from 'flexlayout-react';

// Height share of the top tabset (the header strip) in a layout model.
function headerWeight(model: IJsonModel): number | undefined {
  const outer = (model.layout as { children?: { children?: { weight?: number }[] }[] }).children?.[0];
  return outer?.children?.[0]?.weight;
}

// Mock fetch for settings API
const mockFetch = vi.fn();
globalThis.fetch = mockFetch;

describe('layoutStore', () => {
  beforeEach(() => {
    mockFetch.mockReset();
    vi.restoreAllMocks();
    // Re-assign fetch mock after restoreAllMocks
    globalThis.fetch = mockFetch;
    // Reset store to defaults
    useLayoutStore.setState({
      layout: defaultLayout,
      isLoaded: false,
    });
  });

  describe('defaultLayout header strip', () => {
    it('gives the header enough height to read (6 was too short)', () => {
      expect(headerWeight(defaultLayout)).toBeGreaterThanOrEqual(10);
    });

    it('matches the starter layouts, so Reset does not shrink the header', () => {
      const starters = STARTER_LAYOUTS.map((s) => headerWeight(s.layout));
      for (const w of starters) expect(w).toBe(headerWeight(defaultLayout));
    });
  });

  describe('loadFromBackend', () => {
    it('loads layout from backend successfully', async () => {
      const savedLayout: IJsonModel = {
        global: { tabEnableFloat: true },
        borders: [],
        layout: {
          type: 'row',
          weight: 100,
          children: [],
        },
      };

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({
          layoutJson: JSON.stringify(savedLayout),
        }),
      });

      await useLayoutStore.getState().loadFromBackend();

      const state = useLayoutStore.getState();
      expect(state.isLoaded).toBe(true);
      expect(state.layout).toEqual(savedLayout);
    });

    it('uses default layout when no saved layout exists', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: () => Promise.resolve({
          layoutJson: null,
        }),
      });

      await useLayoutStore.getState().loadFromBackend();

      const state = useLayoutStore.getState();
      expect(state.isLoaded).toBe(true);
      expect(state.layout).toEqual(defaultLayout);
    });

    it('handles API errors gracefully', async () => {
      mockFetch.mockRejectedValueOnce(new Error('Network error'));

      await useLayoutStore.getState().loadFromBackend();

      const state = useLayoutStore.getState();
      expect(state.isLoaded).toBe(true);
      // Should keep default layout on error
      expect(state.layout).toEqual(defaultLayout);
    });

    it('handles non-OK response', async () => {
      mockFetch.mockResolvedValueOnce({ ok: false, status: 500 });

      await useLayoutStore.getState().loadFromBackend();

      const state = useLayoutStore.getState();
      expect(state.isLoaded).toBe(true);
      expect(state.layout).toEqual(defaultLayout);
    });
  });

  describe('syncToBackend', () => {
    it('saves layout to backend using dedicated endpoint', async () => {
      const customLayout: IJsonModel = {
        global: { tabEnableFloat: false },
        borders: [],
        layout: {
          type: 'row',
          weight: 100,
          children: [],
        },
      };

      // Mark as loaded first
      useLayoutStore.setState({ isLoaded: true });
      mockFetch.mockResolvedValueOnce({ ok: true });

      await useLayoutStore.getState().syncToBackend(customLayout);

      // Verify the correct endpoint was called with PUT
      expect(mockFetch).toHaveBeenCalledWith(
        '/api/settings/layout',
        expect.objectContaining({
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(JSON.stringify(customLayout)),
        })
      );
    });

    it('skips save when layout is not loaded to prevent stale data', async () => {
      const customLayout: IJsonModel = {
        global: {},
        borders: [],
        layout: { type: 'row', weight: 100, children: [] },
      };

      // Keep isLoaded as false (default state)
      useLayoutStore.setState({ isLoaded: false });

      await useLayoutStore.getState().syncToBackend(customLayout);

      // Should NOT have called the API
      expect(mockFetch).not.toHaveBeenCalled();
    });

    it('handles save errors gracefully without throwing', async () => {
      const customLayout: IJsonModel = {
        global: {},
        borders: [],
        layout: { type: 'row', weight: 100, children: [] },
      };

      // Mark as loaded first
      useLayoutStore.setState({ isLoaded: true });
      mockFetch.mockRejectedValueOnce(new Error('Network error'));

      // Should not throw
      await expect(
        useLayoutStore.getState().syncToBackend(customLayout)
      ).resolves.toBeUndefined();
    });

    it('logs error on non-OK response', async () => {
      const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
      const customLayout: IJsonModel = {
        global: {},
        borders: [],
        layout: { type: 'row', weight: 100, children: [] },
      };

      // Mark as loaded first
      useLayoutStore.setState({ isLoaded: true });
      mockFetch.mockResolvedValueOnce({ ok: false, status: 500 });

      await useLayoutStore.getState().syncToBackend(customLayout);

      expect(consoleSpy).toHaveBeenCalledWith(
        '[layoutStore] Failed to save layout, status:',
        500
      );
      consoleSpy.mockRestore();
    });
  });

  describe('setLayout', () => {
    it('updates layout and triggers sync to backend', async () => {
      const customLayout: IJsonModel = {
        global: { tabEnableFloat: true },
        borders: [],
        layout: {
          type: 'row',
          weight: 100,
          children: [],
        },
      };

      mockFetch.mockResolvedValueOnce({ ok: true });

      useLayoutStore.getState().setLayout(customLayout);

      const state = useLayoutStore.getState();
      expect(state.layout).toEqual(customLayout);
      expect(state.isLoaded).toBe(true);

      // Wait for async sync to complete
      await new Promise(resolve => setTimeout(resolve, 10));

      expect(mockFetch).toHaveBeenCalledWith(
        '/api/settings/layout',
        expect.objectContaining({
          method: 'PUT',
        })
      );
    });
  });

  describe('resetLayout', () => {
    it('resets to default layout and syncs to backend', async () => {
      const customLayout: IJsonModel = {
        global: { tabEnableFloat: true },
        borders: [],
        layout: {
          type: 'row',
          weight: 100,
          children: [],
        },
      };

      // Set custom layout first
      useLayoutStore.setState({ layout: customLayout, isLoaded: true });
      mockFetch.mockReset();
      mockFetch.mockResolvedValueOnce({ ok: true });

      useLayoutStore.getState().resetLayout();

      const state = useLayoutStore.getState();
      expect(state.layout).toEqual(defaultLayout);
      expect(state.isLoaded).toBe(true);

      // Wait for async sync to complete
      await new Promise(resolve => setTimeout(resolve, 10));

      expect(mockFetch).toHaveBeenCalledWith(
        '/api/settings/layout',
        expect.objectContaining({
          method: 'PUT',
          body: JSON.stringify(JSON.stringify(defaultLayout)),
        })
      );
    });
  });

  describe('setNotLoaded', () => {
    it('marks layout as not loaded', () => {
      // Start with loaded state
      useLayoutStore.setState({ isLoaded: true });

      useLayoutStore.getState().setNotLoaded();

      const state = useLayoutStore.getState();
      expect(state.isLoaded).toBe(false);
    });

    it('prevents saving after marking as not loaded', async () => {
      const customLayout: IJsonModel = {
        global: {},
        borders: [],
        layout: { type: 'row', weight: 100, children: [] },
      };

      // Start with loaded state
      useLayoutStore.setState({ isLoaded: true });

      // Mark as not loaded
      useLayoutStore.getState().setNotLoaded();

      // Try to save
      await useLayoutStore.getState().syncToBackend(customLayout);

      // Should NOT have called the API
      expect(mockFetch).not.toHaveBeenCalled();
    });
  });

  describe('syncToBackendSync', () => {
    it('skips save when layout is not loaded', () => {
      const customLayout: IJsonModel = {
        global: {},
        borders: [],
        layout: { type: 'row', weight: 100, children: [] },
      };

      // Mock XMLHttpRequest
      const mockXHR = {
        open: vi.fn(),
        setRequestHeader: vi.fn(),
        send: vi.fn(),
      };
      global.XMLHttpRequest = vi.fn(() => mockXHR) as unknown as typeof XMLHttpRequest;

      // Keep isLoaded as false
      useLayoutStore.setState({ isLoaded: false });

      useLayoutStore.getState().syncToBackendSync(customLayout);

      // Should NOT have opened the XHR connection
      expect(mockXHR.open).not.toHaveBeenCalled();
    });
  });
});
