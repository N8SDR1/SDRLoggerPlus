import { describe, it, expect } from 'vitest';
import { Model } from 'flexlayout-react';
import { STARTER_LAYOUTS, findStarterLayout } from './starterLayouts';

// Panel ids registered in App.tsx's PLUGINS map. A starter referencing an id
// that isn't registered renders an empty tab, so keep this in sync.
const KNOWN_PANELS = new Set([
  'log-entry', 'log-history', 'cluster', 'wsjtx-decodes', 'grid-tracker', 'rotator',
  'globe-3d', 'qrz-profile', 'contests', 'contest-entry', 'contest-mults',
  'contest-bandmap', 'contest-score', 'header-bar', 'sat-controller', 'sat-web', 'dxpeditions',
  'chat-ai', 'pota', 'dx-coach', 'propagation', 'meters', 'panadapter',
  'statistics', 'map',
]);

function componentsIn(node: unknown): string[] {
  if (!node || typeof node !== 'object') return [];
  const n = node as { component?: string; children?: unknown[] };
  const here = typeof n.component === 'string' ? [n.component] : [];
  const kids = Array.isArray(n.children) ? n.children.flatMap(componentsIn) : [];
  return [...here, ...kids];
}

describe('starter layouts', () => {
  it('ships a non-empty set with unique names', () => {
    expect(STARTER_LAYOUTS.length).toBeGreaterThan(0);
    const names = STARTER_LAYOUTS.map((l) => l.name);
    expect(new Set(names).size).toBe(names.length);
  });

  it.each(STARTER_LAYOUTS.map((l) => [l.name, l] as const))(
    '"%s" is a valid FlexLayout model',
    (_name, starter) => {
      // Model.fromJson throws on a malformed model — this is the same call the
      // menu makes before applying, so a broken starter fails here not at runtime.
      expect(() => Model.fromJson(starter.layout)).not.toThrow();
    },
  );

  it.each(STARTER_LAYOUTS.map((l) => [l.name, l] as const))(
    '"%s" references only registered panels and includes the header bar',
    (_name, starter) => {
      const components = componentsIn(starter.layout.layout);
      expect(components).toContain('header-bar');
      for (const c of components) expect(KNOWN_PANELS).toContain(c);
    },
  );

  it.each(STARTER_LAYOUTS.map((l) => [l.name, l] as const))(
    '"%s" gives the header bar enough height to be readable',
    (_name, starter) => {
      // The header sits in the first tabset of the outer row. At the 6 the app's
      // defaultLayout uses it renders too short to read, so starters use 10.
      const outer = (starter.layout.layout as { children?: { children?: { weight?: number }[] }[] })
        .children?.[0];
      const headerWeight = outer?.children?.[0]?.weight;
      expect(headerWeight).toBeGreaterThanOrEqual(10);
    },
  );

  it('finds a starter by name and misses cleanly', () => {
    expect(findStarterLayout('Contest')?.name).toBe('Contest');
    expect(findStarterLayout('Nope')).toBeUndefined();
  });
});
