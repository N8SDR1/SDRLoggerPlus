import { describe, it, expect, beforeEach } from 'vitest';
import { groupShortcuts, isEditableTarget, isHelpChord, GLOBAL_SCOPE, type Shortcut } from './shortcuts';
import { useShortcutStore } from '../store/shortcutStore';

const s = (keys: string, label = 'does a thing'): Shortcut => ({ keys, label });

describe('groupShortcuts', () => {
  it('sorts Global first, then scopes alphabetically', () => {
    const groups = groupShortcuts({
      Rotator: [s('Enter')],
      'Chat AI': [s('Enter')],
      [GLOBAL_SCOPE]: [s('?')],
      'Log Entry': [s('Esc')],
    });

    expect(groups.map((g) => g.scope)).toEqual([
      GLOBAL_SCOPE,
      'Chat AI',
      'Log Entry',
      'Rotator',
    ]);
  });

  it('drops scopes that registered nothing', () => {
    const groups = groupShortcuts({ Rotator: [], 'Log Entry': [s('Esc')] });
    expect(groups.map((g) => g.scope)).toEqual(['Log Entry']);
  });

  it('preserves each scope declared order', () => {
    const groups = groupShortcuts({ 'Chat AI': [s('Enter', 'Send'), s('Shift+Enter', 'New line')] });
    expect(groups[0].shortcuts.map((x) => x.keys)).toEqual(['Enter', 'Shift+Enter']);
  });

  it('returns nothing for an empty registry', () => {
    expect(groupShortcuts({})).toEqual([]);
  });
});

describe('isEditableTarget', () => {
  it.each(['input', 'textarea', 'select'])('treats <%s> as editable', (tag) => {
    expect(isEditableTarget(document.createElement(tag))).toBe(true);
  });

  it('treats a contenteditable element as editable', () => {
    const div = document.createElement('div');
    div.contentEditable = 'true';
    // jsdom does not derive isContentEditable from the attribute.
    Object.defineProperty(div, 'isContentEditable', { value: true });
    expect(isEditableTarget(div)).toBe(true);
  });

  it('treats ordinary elements as not editable', () => {
    expect(isEditableTarget(document.createElement('div'))).toBe(false);
    expect(isEditableTarget(document.createElement('button'))).toBe(false);
  });

  it('handles a null or non-element target', () => {
    expect(isEditableTarget(null)).toBe(false);
    expect(isEditableTarget(new EventTarget())).toBe(false);
  });
});

describe('isHelpChord', () => {
  const input = () => document.createElement('input');

  it('opens on plain "?" outside a text field', () => {
    expect(isHelpChord({ key: '?', target: document.createElement('div') })).toBe(true);
  });

  it('does NOT steal plain "?" while typing in a field', () => {
    expect(isHelpChord({ key: '?', target: input() })).toBe(false);
  });

  // Regression: the Log Entry callsign field autofocuses on startup, so a
  // chord that only worked outside inputs left the overlay unreachable.
  it('opens on Ctrl+/ even while typing in a field', () => {
    expect(isHelpChord({ key: '/', ctrlKey: true, target: input() })).toBe(true);
  });

  it('opens on Cmd+/ for macOS', () => {
    expect(isHelpChord({ key: '/', metaKey: true, target: input() })).toBe(true);
  });

  it('ignores an unmodified slash', () => {
    expect(isHelpChord({ key: '/', target: document.createElement('div') })).toBe(false);
  });

  it('ignores unrelated keys', () => {
    expect(isHelpChord({ key: 'Enter', target: document.createElement('div') })).toBe(false);
    expect(isHelpChord({ key: 'a', ctrlKey: true, target: input() })).toBe(false);
  });
});

describe('shortcutStore', () => {
  beforeEach(() => useShortcutStore.setState({ registrations: {} }));

  it('registers and unregisters a scope', () => {
    const { register, unregister } = useShortcutStore.getState();

    register('Rotator', [s('Enter')]);
    expect(useShortcutStore.getState().registrations).toHaveProperty('Rotator');

    unregister('Rotator');
    expect(useShortcutStore.getState().registrations).not.toHaveProperty('Rotator');
  });

  it('replaces rather than duplicates when the same scope registers twice', () => {
    const { register } = useShortcutStore.getState();

    register('Rotator', [s('Enter')]);
    register('Rotator', [s('Enter'), s('Esc')]);

    expect(useShortcutStore.getState().registrations.Rotator).toHaveLength(2);
    expect(Object.keys(useShortcutStore.getState().registrations)).toEqual(['Rotator']);
  });

  it('keeps scopes independent', () => {
    const { register, unregister } = useShortcutStore.getState();

    register('Rotator', [s('Enter')]);
    register('Log Entry', [s('Esc')]);
    unregister('Rotator');

    expect(Object.keys(useShortcutStore.getState().registrations)).toEqual(['Log Entry']);
  });

  it('unregistering an unknown scope is a no-op', () => {
    const before = useShortcutStore.getState().registrations;
    useShortcutStore.getState().unregister('Nope');
    expect(useShortcutStore.getState().registrations).toBe(before);
  });
});
