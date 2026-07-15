import { create } from 'zustand';
import type { WsjtxDecodeEvent } from '../api/signalr';

/**
 * Ephemeral in-memory buffer of the most recent WSJT-X/JTDX/MSHV decodes,
 * newest first. Fed live by the SignalR `onWsjtxDecode` handler and seeded on
 * panel open from GET /api/wsjtx/decodes. Not persisted — decodes are transient.
 */
const MAX_DECODES = 300;

interface WsjtxDecodeState {
  decodes: WsjtxDecodeEvent[];
  /** Live push of a single decode (prepended). */
  addDecode: (d: WsjtxDecodeEvent) => void;
  /** Seed from the backend backfill (oldest-first) — becomes newest-first. */
  seed: (list: WsjtxDecodeEvent[]) => void;
  clear: () => void;
}

export const useWsjtxDecodeStore = create<WsjtxDecodeState>((set) => ({
  decodes: [],
  addDecode: (d) => set((s) => ({ decodes: [d, ...s.decodes].slice(0, MAX_DECODES) })),
  seed: (list) => set(() => ({ decodes: [...list].reverse().slice(0, MAX_DECODES) })),
  clear: () => set({ decodes: [] }),
}));
