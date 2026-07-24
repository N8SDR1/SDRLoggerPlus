import { describe, it, expect } from 'vitest';
import { parseLayoutRef, layoutRefLabel } from './applyLayout';

describe('parseLayoutRef', () => {
  it('splits a starter ref', () => {
    expect(parseLayoutRef('starter:Satellite')).toEqual({ kind: 'starter', name: 'Satellite' });
  });

  it('splits a saved ref', () => {
    expect(parseLayoutRef('saved:My Contest')).toEqual({ kind: 'saved', name: 'My Contest' });
  });

  it('keeps colons that are part of the name', () => {
    expect(parseLayoutRef('saved:20:1 ratio')).toEqual({ kind: 'saved', name: '20:1 ratio' });
  });

  it('rejects an unknown kind', () => {
    expect(parseLayoutRef('bogus:Thing')).toBeNull();
  });

  it('rejects a ref with no kind separator', () => {
    expect(parseLayoutRef('Satellite')).toBeNull();
  });
});

describe('layoutRefLabel', () => {
  it('marks a built-in starter', () => {
    expect(layoutRefLabel('starter:Satellite')).toBe('Satellite (built-in)');
  });

  it('shows a saved layout name plainly', () => {
    expect(layoutRefLabel('saved:My Contest')).toBe('My Contest');
  });

  it('passes an unparseable ref through unchanged', () => {
    expect(layoutRefLabel('whatever')).toBe('whatever');
  });
});
