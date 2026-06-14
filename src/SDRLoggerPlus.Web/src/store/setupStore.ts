import { create } from 'zustand';

export interface SetupStatus {
  isConfigured: boolean;
  isConnected: boolean;
  provider?: string;
  configuredAt?: string;
  databaseName?: string;
}

export interface ConfigureResult {
  success: boolean;
  message: string;
  restartRequired: boolean;
}

interface SetupState {
  status: SetupStatus | null;
  isLoading: boolean;
  error: string | null;

  fetchStatus: () => Promise<void>;
  configureLocal: () => Promise<ConfigureResult | null>;
  clearError: () => void;
}

// SDRLoggerPlus is LiteDB-only; the setup flow just confirms the local database.
export const useSetupStore = create<SetupState>((set, get) => ({
  status: null,
  isLoading: false,
  error: null,

  clearError: () => set({ error: null }),

  fetchStatus: async () => {
    set({ isLoading: true, error: null });
    try {
      const response = await fetch('/api/setup/status');
      if (!response.ok) throw new Error('Failed to fetch status');
      const status = await response.json();
      set({ status, isLoading: false });
    } catch (err) {
      set({
        error: err instanceof Error ? err.message : 'Failed to fetch status',
        isLoading: false,
      });
    }
  },

  configureLocal: async () => {
    set({ isLoading: true, error: null });
    try {
      const response = await fetch('/api/setup/configure', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ provider: 'local' }),
      });

      const result: ConfigureResult = await response.json();

      if (result.success) {
        if (!result.restartRequired) {
          await get().fetchStatus();
        }
        set({ isLoading: false });
        return result;
      }

      set({ error: result.message, isLoading: false });
      return null;
    } catch (err) {
      set({
        error: err instanceof Error ? err.message : 'Setup failed',
        isLoading: false,
      });
      return null;
    }
  },
}));
