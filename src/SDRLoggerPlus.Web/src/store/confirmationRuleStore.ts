import { create } from 'zustand';

/**
 * Which confirmation channels count as "confirmed" in the grid views
 * (Grid Tracker, VUCC, the 2D map's grid overlay). Mirrors the backend's
 * ConfirmationRule:
 *
 *  - 'awardRules' — LoTW + paper card only, what ARRL actually credits.
 *  - 'any'        — also eQSL and QRZ Logbook.
 *
 * Defaults to award rules so a green grid means the same thing on the map as
 * it does on the FFMA/VUCC screens (issue #46). Shared by all grid surfaces —
 * one preference, so two panels can never disagree about what green means.
 */
export type ConfirmationRule = 'awardRules' | 'any';

const STORAGE_KEY = 'awards.confirmationRule';

interface ConfirmationRuleState {
  rule: ConfirmationRule;
  setRule: (rule: ConfirmationRule) => void;
}

export const useConfirmationRuleStore = create<ConfirmationRuleState>((set) => ({
  rule: localStorage.getItem(STORAGE_KEY) === 'any' ? 'any' : 'awardRules',
  setRule: (rule) => {
    localStorage.setItem(STORAGE_KEY, rule);
    set({ rule });
  },
}));
