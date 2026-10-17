import { afterEach, describe, expect, it } from 'vitest';
import { AUTO_HISTORY_KEY, isAutoHistoryEnabled } from './autoHistory';

afterEach(() => {
  localStorage.removeItem(AUTO_HISTORY_KEY);
});

describe('isAutoHistoryEnabled', () => {
  it('defaults to on when nothing is stored', () => {
    expect(isAutoHistoryEnabled()).toBe(true);
  });

  it('is on for the stored on value', () => {
    localStorage.setItem(AUTO_HISTORY_KEY, '1');
    expect(isAutoHistoryEnabled()).toBe(true);
  });

  it('is off only for the stored off value', () => {
    localStorage.setItem(AUTO_HISTORY_KEY, '0');
    expect(isAutoHistoryEnabled()).toBe(false);
  });
});
