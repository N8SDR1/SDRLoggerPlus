export type EventCallback<T> = (event: T) => void;

export function createCallbackSet<T>() {
  const callbacks = new Set<EventCallback<T>>();

  return {
    add(callback: EventCallback<T>) {
      callbacks.add(callback);
    },
    remove(callback: EventCallback<T>) {
      callbacks.delete(callback);
    },
    clear() {
      callbacks.clear();
    },
    emit(event: T) {
      for (const callback of callbacks) callback(event);
    },
  };
}
