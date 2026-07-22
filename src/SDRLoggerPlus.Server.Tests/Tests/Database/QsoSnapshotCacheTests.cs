using FluentAssertions;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Database;

[Trait("Category", "Unit")]
public class QsoSnapshotCacheTests
{
    private static List<Qso> Log(params string[] callsigns)
        => callsigns.Select(c => new Qso { Callsign = c, Band = "20m", Mode = "FT8" }).ToList();

    [Fact]
    public async Task SecondRead_ServesTheSnapshotWithoutReloading()
    {
        var cache = new QsoSnapshotCache();
        var loads = 0;
        Task<IEnumerable<Qso>> Load() { loads++; return Task.FromResult<IEnumerable<Qso>>(Log("K1ABC")); }

        await cache.GetAsync(Load);
        var second = await cache.GetAsync(Load);

        loads.Should().Be(1);
        second.Should().ContainSingle(q => q.Callsign == "K1ABC");
    }

    [Fact]
    public async Task Invalidate_ForcesTheNextReadToReload()
    {
        var cache = new QsoSnapshotCache();
        var current = Log("K1ABC");
        Task<IEnumerable<Qso>> Load() => Task.FromResult<IEnumerable<Qso>>(current);

        await cache.GetAsync(Load);
        current = Log("K1ABC", "W1AW");   // a QSO gets logged
        cache.Invalidate();

        (await cache.GetAsync(Load)).Should().HaveCount(2);
    }

    [Fact]
    public async Task StaleReadIsNotPublished_WhenAWriteLandsMidLoad()
    {
        var cache = new QsoSnapshotCache();
        var released = new TaskCompletionSource();
        var current = Log("K1ABC");
        var loads = 0;

        async Task<IEnumerable<Qso>> SlowLoad()
        {
            loads++;
            var asOfReadStart = current;      // what the database held when the read began
            await released.Task;              // hold the read open across the write
            return asOfReadStart;
        }

        var reading = cache.GetAsync(SlowLoad);

        // A QSO is logged while that read is still in flight: the data it is
        // about to return predates the write, so it must not become the
        // cached snapshot — otherwise the new QSO stays invisible to the
        // awards panel until some later, unrelated write.
        current = Log("K1ABC", "W1AW");
        cache.Invalidate();
        released.SetResult();

        (await reading).Should().ContainSingle();   // this caller sees its own read

        var next = await cache.GetAsync(() => Task.FromResult<IEnumerable<Qso>>(current));
        next.Should().HaveCount(2);                 // but the next read sees the write
        loads.Should().Be(1);
    }

    [Fact]
    public async Task ConcurrentMisses_BothGetTheData()
    {
        var cache = new QsoSnapshotCache();
        var gate = new TaskCompletionSource();
        async Task<IEnumerable<Qso>> Load() { await gate.Task; return Log("K1ABC"); }

        var a = cache.GetAsync(Load);
        var b = cache.GetAsync(Load);
        gate.SetResult();

        (await a).Should().ContainSingle();
        (await b).Should().ContainSingle();
    }

    [Fact]
    public async Task EmptyLog_IsCachedRatherThanReloadedEveryTime()
    {
        var cache = new QsoSnapshotCache();
        var loads = 0;
        Task<IEnumerable<Qso>> Load() { loads++; return Task.FromResult<IEnumerable<Qso>>(new List<Qso>()); }

        await cache.GetAsync(Load);
        await cache.GetAsync(Load);

        // An empty list is a real answer, not a miss — a brand-new log must
        // not re-query on every award request.
        loads.Should().Be(1);
    }
}
