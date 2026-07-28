import { create } from 'zustand';
import type { StationPresenceEvent, OperatorMessageEvent } from '../api/signalr';

/**
 * Live multi-op coordination state (S-COORD). SignalR handlers push into it; the coordination panel
 * reads it. Presence is one entry per station (latest wins); messages are a bounded rolling log.
 */
interface MultiOpState {
  presence: StationPresenceEvent[];
  messages: OperatorMessageEvent[];
  setPresence: (board: StationPresenceEvent[]) => void;
  upsertPresence: (p: StationPresenceEvent) => void;
  addMessage: (m: OperatorMessageEvent) => void;
}

export const useMultiOpStore = create<MultiOpState>((set) => ({
  presence: [],
  messages: [],
  setPresence: (board) => set({ presence: board }),
  upsertPresence: (p) =>
    set((s) => ({ presence: [...s.presence.filter((x) => x.stationId !== p.stationId), p] })),
  addMessage: (m) => set((s) => ({ messages: [...s.messages.slice(-199), m] })), // keep last 200
}));
