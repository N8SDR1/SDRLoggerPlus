import { describe, it, expect } from 'vitest';
import { cqBadge, matchesCqModifier } from './cqModifier';

const cq = (cqModifier?: string) => ({ isCq: true, cqModifier });
const exchange = { isCq: false, cqModifier: undefined };

describe('cqBadge', () => {
  it('is CQ for a plain CQ', () => expect(cqBadge(cq())).toBe('CQ'));
  it('includes the modifier', () => expect(cqBadge(cq('POTA'))).toBe('CQ POTA'));
  it('joins a compound modifier', () => expect(cqBadge(cq('DX POTA'))).toBe('CQ DX POTA'));
  it('is null for a non-CQ', () => expect(cqBadge(exchange)).toBeNull());
});

describe('matchesCqModifier', () => {
  it('blank query keeps every decode', () => {
    expect(matchesCqModifier(cq(), '  ')).toBe(true);
    expect(matchesCqModifier(exchange, '')).toBe(true);
  });

  it('CQ matches every CQ and hides exchanges', () => {
    expect(matchesCqModifier(cq(), 'cq')).toBe(true);
    expect(matchesCqModifier(cq('POTA'), 'CQ')).toBe(true);
    expect(matchesCqModifier(exchange, 'CQ')).toBe(false);
  });

  it('POTA matches the modifier and the badge', () => {
    expect(matchesCqModifier(cq('POTA'), 'pota')).toBe(true);
    expect(matchesCqModifier(cq('DX POTA'), 'pota')).toBe(true);
    expect(matchesCqModifier(cq(), 'POTA')).toBe(false);
    expect(matchesCqModifier(cq('SOTA'), 'POTA')).toBe(false);
  });

  it('a directed frequency matches on its own', () => {
    expect(matchesCqModifier(cq('070'), '070')).toBe(true);
    expect(matchesCqModifier(cq('DX'), '070')).toBe(false);
  });
});
