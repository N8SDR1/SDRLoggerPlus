using System.Collections.Concurrent;

namespace SDRLoggerPlus.Server.Core.Events;

public interface IEventBus
{
    void Publish<T>(T eventData) where T : class;
    IDisposable Subscribe<T>(Action<T> handler) where T : class;
    T? GetLastValue<T>() where T : class;
}

public class EventBus : IEventBus
{
    private readonly ConcurrentDictionary<Type, List<Delegate>> _subscribers = new();
    private readonly ConcurrentDictionary<Type, object?> _lastValues = new();
    private readonly object _lock = new();

    public void Publish<T>(T eventData) where T : class
    {
        _lastValues[typeof(T)] = eventData;

        if (_subscribers.TryGetValue(typeof(T), out var handlers))
        {
            List<Delegate> handlersCopy;
            lock (_lock)
            {
                handlersCopy = handlers.ToList();
            }

            foreach (var handler in handlersCopy)
            {
                try
                {
                    ((Action<T>)handler)(eventData);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Event handler error: {ex.Message}");
                }
            }
        }
    }

    public IDisposable Subscribe<T>(Action<T> handler) where T : class
    {
        var type = typeof(T);

        lock (_lock)
        {
            if (!_subscribers.ContainsKey(type))
            {
                _subscribers[type] = new List<Delegate>();
            }
            _subscribers[type].Add(handler);
        }

        return new Subscription(() =>
        {
            lock (_lock)
            {
                if (_subscribers.TryGetValue(type, out var handlers))
                {
                    handlers.Remove(handler);
                }
            }
        });
    }

    public T? GetLastValue<T>() where T : class
    {
        if (_lastValues.TryGetValue(typeof(T), out var value))
        {
            return value as T;
        }
        return null;
    }

    private class Subscription : IDisposable
    {
        private readonly Action _unsubscribe;

        public Subscription(Action unsubscribe)
        {
            _unsubscribe = unsubscribe;
        }

        public void Dispose()
        {
            _unsubscribe();
        }
    }
}
