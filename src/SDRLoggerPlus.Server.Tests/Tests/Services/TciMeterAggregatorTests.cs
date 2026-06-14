using FluentAssertions;
using SDRLoggerPlus.Contracts.Events;
using SDRLoggerPlus.Server.Services.Tci;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

/// <summary>
/// TciMeterAggregator coalesces high-rate sensor frames into at most one
/// TciMetersEvent per 80 ms window, and only when new data arrived since the
/// last emission. Thetis can be configured down to 30 ms sensor intervals and
/// RX + TX frames arrive independently — without coalescing every frame would
/// become a SignalR broadcast. Time is passed in explicitly so tests are
/// deterministic.
/// </summary>
[Trait("Category", "Unit")]
public class TciMeterAggregatorTests
{
    private static readonly DateTime T0 = new(2026, 6, 12, 12, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void TryGetSnapshot_AfterRxUpdate_EmitsEventWithRxValues()
    {
        var agg = new TciMeterAggregator();
        agg.UpdateRx(new TciRxChannelSensorReading(0, 0, -97.4, -99.1, -85.0));

        var emitted = agg.TryGetSnapshot(T0, "radio1", isTransmitting: false, out var evt);

        emitted.Should().BeTrue();
        evt!.RadioId.Should().Be("radio1");
        evt.RxSignalDbm.Should().BeApproximately(-97.4, 0.001);
        evt.RxAvgSignalDbm.Should().BeApproximately(-99.1, 0.001);
        evt.TxPowerWatts.Should().BeNull("TX has not reported yet");
        evt.IsTransmitting.Should().BeFalse();
        evt.TimestampUtc.Should().Be(T0);
    }

    [Fact]
    public void TryGetSnapshot_WithinThrottleWindow_DoesNotEmit()
    {
        var agg = new TciMeterAggregator();
        agg.UpdateRx(new TciRxChannelSensorReading(0, 0, -97.4, null, null));
        agg.TryGetSnapshot(T0, "radio1", false, out _).Should().BeTrue();

        agg.UpdateRx(new TciRxChannelSensorReading(0, 0, -95.0, null, null));
        var emitted = agg.TryGetSnapshot(T0.AddMilliseconds(50), "radio1", false, out var evt);

        emitted.Should().BeFalse();
        evt.Should().BeNull();
    }

    [Fact]
    public void TryGetSnapshot_AfterThrottleWindow_EmitsLatestValues()
    {
        var agg = new TciMeterAggregator();
        agg.UpdateRx(new TciRxChannelSensorReading(0, 0, -97.4, null, null));
        agg.TryGetSnapshot(T0, "radio1", false, out _);

        agg.UpdateRx(new TciRxChannelSensorReading(0, 0, -95.0, null, null));
        var emitted = agg.TryGetSnapshot(T0.AddMilliseconds(150), "radio1", false, out var evt);

        emitted.Should().BeTrue();
        evt!.RxSignalDbm.Should().BeApproximately(-95.0, 0.001);
    }

    [Fact]
    public void TryGetSnapshot_NoNewDataSinceLastEmit_DoesNotEmit()
    {
        var agg = new TciMeterAggregator();
        agg.UpdateRx(new TciRxChannelSensorReading(0, 0, -97.4, null, null));
        agg.TryGetSnapshot(T0, "radio1", false, out _).Should().BeTrue();

        // Window has passed but nothing arrived — stay quiet.
        var emitted = agg.TryGetSnapshot(T0.AddSeconds(5), "radio1", false, out _);

        emitted.Should().BeFalse();
    }

    [Fact]
    public void TryGetSnapshot_RxAndTxUpdates_CarriesBoth()
    {
        var agg = new TciMeterAggregator();
        agg.UpdateRx(new TciRxChannelSensorReading(0, 0, -97.4, null, null));
        agg.UpdateTx(new TciTxSensorReading(0, -12.3, 4.8, 5.0, 1.4));

        agg.TryGetSnapshot(T0, "radio1", true, out var evt).Should().BeTrue();

        evt!.RxSignalDbm.Should().BeApproximately(-97.4, 0.001);
        evt.TxMicDbm.Should().BeApproximately(-12.3, 0.001);
        evt.TxPowerWatts.Should().BeApproximately(4.8, 0.001);
        evt.TxPeakPowerWatts.Should().BeApproximately(5.0, 0.001);
        evt.TxSwr.Should().BeApproximately(1.4, 0.001);
        evt.IsTransmitting.Should().BeTrue();
    }

    [Fact]
    public void TryGetSnapshot_TxOnlyUpdateAfterEmit_EmitsAgain()
    {
        var agg = new TciMeterAggregator();
        agg.UpdateRx(new TciRxChannelSensorReading(0, 0, -97.4, null, null));
        agg.TryGetSnapshot(T0, "radio1", false, out _);

        agg.UpdateTx(new TciTxSensorReading(0, -12.3, 4.8, 5.0, 1.4));
        var emitted = agg.TryGetSnapshot(T0.AddMilliseconds(150), "radio1", true, out var evt);

        emitted.Should().BeTrue("a TX frame is new data");
        evt!.RxSignalDbm.Should().BeApproximately(-97.4, 0.001, "last RX value is retained");
    }

    [Fact]
    public void ParallelUpdates_DoNotThrow()
    {
        var agg = new TciMeterAggregator();

        Parallel.For(0, 1000, i =>
        {
            agg.UpdateRx(new TciRxChannelSensorReading(0, 0, -100 + i % 30, null, null));
            agg.UpdateTx(new TciTxSensorReading(0, -12, i % 5, 5, 1.1));
            agg.TryGetSnapshot(T0.AddMilliseconds(i), "radio1", false, out _);
        });
    }
}
