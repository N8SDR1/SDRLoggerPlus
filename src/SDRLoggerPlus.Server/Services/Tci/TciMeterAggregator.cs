using SDRLoggerPlus.Contracts.Events;

namespace SDRLoggerPlus.Server.Services.Tci;

/// <summary>
/// Accumulates the latest TCI sensor readings and coalesces them into at most
/// one <see cref="TciMetersEvent"/> per throttle window, and only when new
/// data arrived since the last emission. The window (80 ms) sits below the
/// 100 ms sensor enable interval so timer jitter cannot alias the emit rate
/// down to every-other-frame. Callers pass the current time explicitly,
/// which keeps this class clock-free and deterministic. Thread-safe.
/// Note: there is no flush timer — data arriving inside a window is emitted
/// when the NEXT frame triggers a snapshot. With a continuously streaming
/// radio that bounds staleness at one sensor interval; if the stream stops,
/// the final partial window's readings are intentionally never sent.
/// </summary>
public sealed class TciMeterAggregator
{
    private static readonly TimeSpan ThrottleWindow = TimeSpan.FromMilliseconds(80);

    private readonly object _lock = new();

    private double? _rxSignalDbm;
    private double? _rxAvgSignalDbm;
    private double? _txMicDbm;
    private double? _txPowerWatts;
    private double? _txPeakPowerWatts;
    private double? _txSwr;

    private bool _dirty;
    private DateTime? _lastEmitUtc;

    public void UpdateRx(TciRxChannelSensorReading reading)
    {
        lock (_lock)
        {
            _rxSignalDbm = reading.Dbm;
            if (reading.AvgDbm.HasValue) _rxAvgSignalDbm = reading.AvgDbm;
            _dirty = true;
        }
    }

    public void UpdateRx(TciRxSensorReading reading)
    {
        lock (_lock)
        {
            _rxSignalDbm = reading.Dbm;
            _dirty = true;
        }
    }

    public void UpdateTx(TciTxSensorReading reading)
    {
        lock (_lock)
        {
            _txMicDbm = reading.MicDbm;
            _txPowerWatts = reading.PowerWatts;
            _txPeakPowerWatts = reading.PeakPowerWatts;
            _txSwr = reading.Swr;
            _dirty = true;
        }
    }

    /// <summary>
    /// Returns true (with the event) when there is unreported data and the
    /// throttle window since the previous emission has elapsed.
    /// </summary>
    public bool TryGetSnapshot(DateTime nowUtc, string radioId, bool isTransmitting, out TciMetersEvent? evt)
    {
        lock (_lock)
        {
            if (!_dirty || (_lastEmitUtc.HasValue && nowUtc - _lastEmitUtc.Value < ThrottleWindow))
            {
                evt = null;
                return false;
            }

            evt = new TciMetersEvent(
                radioId,
                _rxSignalDbm,
                _rxAvgSignalDbm,
                _txMicDbm,
                _txPowerWatts,
                _txPeakPowerWatts,
                _txSwr,
                isTransmitting,
                nowUtc);

            _dirty = false;
            _lastEmitUtc = nowUtc;
            return true;
        }
    }
}
