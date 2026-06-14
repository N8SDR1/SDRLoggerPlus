import { describe, expect, it, vi } from 'vitest';
import { createCallbackSet } from './callbackSet';

describe('createCallbackSet', () => {
  it('notifies every listener and removes only the requested listener', () => {
    const callbacks = createCallbackSet<number>();
    const first = vi.fn();
    const second = vi.fn();

    callbacks.add(first);
    callbacks.add(second);
    callbacks.emit(7);
    callbacks.remove(first);
    callbacks.emit(9);

    expect(first).toHaveBeenCalledTimes(1);
    expect(first).toHaveBeenCalledWith(7);
    expect(second).toHaveBeenNthCalledWith(1, 7);
    expect(second).toHaveBeenNthCalledWith(2, 9);
  });

  it('clears every listener', () => {
    const callbacks = createCallbackSet<number>();
    const listener = vi.fn();
    callbacks.add(listener);

    callbacks.clear();
    callbacks.emit(7);

    expect(listener).not.toHaveBeenCalled();
  });
});
