import { create } from 'zustand';
import type { LightningStatus, WindStatus } from '../api/client';

/**
 * Ephemeral preview state for the WeatherAlertBanner. Settings → Weather
 * exposes "Preview" buttons that inject a fake lightning/wind status via
 * startPreview(); the banner reads from this store instead of the real
 * backend polls while a preview is active.
 *
 * Auto-expires after the requested duration (default 20 s) so a click-
 * away doesn't leave a fake alert stuck on-screen.
 */
interface WeatherPreviewState {
  lightning: LightningStatus | null;
  wind: WindStatus | null;
  expiresAt: number; // epoch ms; 0 = no active preview
  startPreview: (opts: {
    lightning?: LightningStatus;
    wind?: WindStatus;
    durationMs?: number;
  }) => void;
  clearPreview: () => void;
}

let expireTimer: ReturnType<typeof setTimeout> | null = null;

export const useWeatherPreviewStore = create<WeatherPreviewState>((set) => ({
  lightning: null,
  wind: null,
  expiresAt: 0,
  startPreview: ({ lightning, wind, durationMs = 20_000 }) => {
    if (expireTimer) clearTimeout(expireTimer);
    set({
      lightning: lightning ?? null,
      wind: wind ?? null,
      expiresAt: Date.now() + durationMs,
    });
    expireTimer = setTimeout(() => {
      set({ lightning: null, wind: null, expiresAt: 0 });
      expireTimer = null;
    }, durationMs);
  },
  clearPreview: () => {
    if (expireTimer) { clearTimeout(expireTimer); expireTimer = null; }
    set({ lightning: null, wind: null, expiresAt: 0 });
  },
}));
