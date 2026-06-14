import { create } from 'zustand';

export interface Toast {
  id: number;
  text: string;
  kind: 'info' | 'success' | 'warn' | 'error';
}

interface ToastState {
  toasts: Toast[];
  push: (text: string, kind?: Toast['kind'], durationMs?: number) => void;
  dismiss: (id: number) => void;
}

let nextId = 1;

/** Lightweight app-wide transient notifications (OOB warnings, band openings, …). */
export const useToastStore = create<ToastState>((set) => ({
  toasts: [],
  push: (text, kind = 'info', durationMs = 8000) => {
    const id = nextId++;
    set((s) => ({ toasts: [...s.toasts, { id, text, kind }] }));
    setTimeout(() => {
      set((s) => ({ toasts: s.toasts.filter((t) => t.id !== id) }));
    }, durationMs);
  },
  dismiss: (id) => set((s) => ({ toasts: s.toasts.filter((t) => t.id !== id) })),
}));
