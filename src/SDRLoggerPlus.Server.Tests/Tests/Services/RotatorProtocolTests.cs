using System.Globalization;
using FluentAssertions;
using SDRLoggerPlus.Server.Services.Rotator;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

/// <summary>
/// Wire-protocol strategy tests. The protocols talk to an <see cref="IRotatorChannel"/>,
/// so a fake queue stands in for the socket — no rotator hardware, no timing.
///
/// Several of these exist because of a field report against PSTRotator: the reported
/// heading fell minutes behind the real one and only a reconnect fixed it. The cause was
/// framing, not speed — a controller answering with one line more (or fewer) than rotctld
/// documents left the reader permanently offset.
/// </summary>
[Trait("Category", "Unit")]
public class RotatorProtocolTests
{
    /// <summary>
    /// Stands in for the socket: lines can be waiting before a request (unsolicited, or an
    /// answer to something earlier), and a responder queues the reply to each command.
    /// An empty queue means "nothing arrived in time" — what the real channel returns on
    /// timeout — so no test has to wait for one.
    /// </summary>
    private sealed class FakeChannel : IRotatorChannel
    {
        private readonly Queue<string> _queue = new();
        private readonly Func<string, IEnumerable<string>> _respond;

        public List<string> Writes { get; } = new();
        public int DrainedLines { get; private set; }

        public FakeChannel(Func<string, IEnumerable<string>>? respond = null, params string[] alreadyWaiting)
        {
            _respond = respond ?? (_ => Array.Empty<string>());
            foreach (var line in alreadyWaiting) _queue.Enqueue(line);
        }

        public int Drain()
        {
            var count = _queue.Count;
            DrainedLines += count;
            _queue.Clear();
            return count;
        }

        public Task WriteAsync(string text, CancellationToken ct)
        {
            Writes.Add(text);
            foreach (var line in _respond(text)) _queue.Enqueue(line);
            return Task.CompletedTask;
        }

        public Task<string?> ReadLineAsync(TimeSpan timeout, CancellationToken ct) =>
            Task.FromResult(_queue.Count > 0 ? _queue.Dequeue() : null);
    }

    private static FakeChannel Rotctld(string azimuth, string? elevation = "0.0", params string[] alreadyWaiting)
    {
        IEnumerable<string> Respond(string cmd)
        {
            if (!cmd.StartsWith('p')) return new[] { "RPRT 0" };
            return elevation == null ? new[] { azimuth } : new[] { azimuth, elevation };
        }
        return new FakeChannel(Respond, alreadyWaiting);
    }

    // ───────────────────────── microHAM ARCO ─────────────────────────

    [Fact]
    public async Task Arco_Poll_WritesC2_AndParsesAzimuth()
    {
        var channel = new FakeChannel(_ => new[] { "+0270+0000" });

        var az = await new ArcoTcpProtocol().PollAzimuthAsync(channel, default);

        az.Should().Be(270.0);
        channel.Writes.Should().ContainSingle().Which.Should().Be("C2\r");
    }

    [Theory]
    [InlineData("+0090+0000", 90.0)]
    [InlineData("+0270+0000", 270.0)]
    [InlineData("+0359+0010", 359.0)]
    [InlineData("+0000+0000", 0.0)]
    [InlineData("+0180", 180.0)]   // AZ only, no EL field
    public async Task Arco_Poll_ParsesAzimuthFromReply(string reply, double expected)
    {
        var az = await new ArcoTcpProtocol().PollAzimuthAsync(new FakeChannel(_ => new[] { reply }), default);
        az.Should().Be(expected);
    }

    [Theory]
    [InlineData("")]
    [InlineData("garbage")]
    [InlineData("?>")]
    public async Task Arco_Poll_ReturnsNull_OnUnparseableReply(string reply)
    {
        var az = await new ArcoTcpProtocol().PollAzimuthAsync(new FakeChannel(_ => new[] { reply }), default);
        az.Should().BeNull();
    }

    [Fact]
    public async Task Arco_Poll_ReturnsNull_WhenControllerSaysNothing()
    {
        var az = await new ArcoTcpProtocol().PollAzimuthAsync(new FakeChannel(), default);
        az.Should().BeNull();
    }

    [Theory]
    [InlineData(5.0, "M005\r")]
    [InlineData(270.0, "M270\r")]
    [InlineData(270.6, "M270\r")]   // truncated integer degrees, matching SDRLogger+ int(az)
    [InlineData(359.9, "M359\r")]
    [InlineData(0.0, "M000\r")]
    public async Task Arco_SetAzimuth_WritesMCommand(double azimuth, string expected)
    {
        var channel = new FakeChannel();
        await new ArcoTcpProtocol().SetAzimuthAsync(azimuth, channel, default);
        channel.Writes.Should().ContainSingle().Which.Should().Be(expected);
    }

    [Fact]
    public async Task Arco_Stop_WritesS()
    {
        var channel = new FakeChannel();
        await new ArcoTcpProtocol().StopAsync(channel, default);
        channel.Writes.Should().ContainSingle().Which.Should().Be("S\r");
    }

    // ───────────────────────── hamlib rotctld ─────────────────────────

    [Fact]
    public async Task Rotctld_Poll_WritesP_AndParsesAzimuth()
    {
        var channel = Rotctld("270.5");

        var az = await new RotctldProtocol().PollAzimuthAsync(channel, default);

        az.Should().Be(270.5);
        channel.Writes.Should().ContainSingle().Which.Should().Be("p\n");
    }

    [Fact]
    public async Task Rotctld_Poll_TerminatesWithBareLf()
    {
        // WriteLine put a CR in front of the LF on Windows. A controller that also treats CR
        // as a terminator answers the resulting empty command too, and that extra line is
        // what accumulated in the reader until the reported position was minutes old.
        var channel = Rotctld("12.0");

        await new RotctldProtocol().PollAzimuthAsync(channel, default);

        channel.Writes.Single().Should().NotContain("\r");
    }

    [Fact]
    public async Task Rotctld_Poll_ReturnsAzimuth_WhenControllerSendsNoElevationLine()
    {
        // Azimuth-only emulators answer `p` with one line. Waiting unconditionally for a
        // second one parked the service until unrelated traffic happened to arrive.
        var channel = Rotctld("147.0", elevation: null);

        var az = await new RotctldProtocol().PollAzimuthAsync(channel, default);

        az.Should().Be(147.0);
    }

    [Fact]
    public async Task Rotctld_Poll_DiscardsLinesLeftOverFromEarlierExchanges()
    {
        // Two stale lines are waiting (an extra reply the controller sent, an RPRT nobody
        // read). Without draining, this poll reports a heading the antenna left long ago.
        var channel = Rotctld("300.0", "0.0", alreadyWaiting: new[] { "10.0", "0.0", "RPRT 0" });

        var az = await new RotctldProtocol().PollAzimuthAsync(channel, default);

        az.Should().Be(300.0);
        channel.DrainedLines.Should().Be(3);
    }

    [Fact]
    public async Task Rotctld_Poll_StaysCurrent_WhenControllerAnswersWithAnExtraLine()
    {
        // Every `p` here yields three lines instead of two. Reading exactly two left one
        // behind, so the backlog — and the reported lag — grew with every poll.
        var headings = new Queue<string>(new[] { "10.0", "20.0", "30.0", "40.0", "50.0" });
        var channel = new FakeChannel(_ => new[] { headings.Dequeue(), "0.0", "RPRT 0" });
        var protocol = new RotctldProtocol();

        double? last = null;
        for (var i = 0; i < 5; i++) last = await protocol.PollAzimuthAsync(channel, default);

        last.Should().Be(50.0, "the fifth poll must report the fifth heading, not an earlier one");
    }

    [Fact]
    public async Task Rotctld_Poll_ReturnsNull_WhenControllerSaysNothing()
    {
        var az = await new RotctldProtocol().PollAzimuthAsync(new FakeChannel(), default);
        az.Should().BeNull();
    }

    [Fact]
    public async Task Rotctld_Poll_ReturnsNull_OnUnparseableReply()
    {
        var az = await new RotctldProtocol().PollAzimuthAsync(new FakeChannel(_ => new[] { "RPRT -1" }), default);
        az.Should().BeNull();
    }

    [Fact]
    public async Task Rotctld_SetAzimuth_WritesPCommand_AndReadsResponse()
    {
        var channel = Rotctld("0.0");

        await new RotctldProtocol().SetAzimuthAsync(270.6, channel, default);

        channel.Writes.Should().ContainSingle().Which.Should().Be("P 270.6 0\n");
    }

    [Fact]
    public async Task Rotctld_Stop_WritesS()
    {
        var channel = Rotctld("0.0");
        await new RotctldProtocol().StopAsync(channel, default);
        channel.Writes.Should().ContainSingle().Which.Should().Be("S\n");
    }

    // ───────────────────── locale independence ─────────────────────

    [Theory]
    [InlineData("de-DE")]   // decimal comma, period as group separator
    [InlineData("fr-FR")]
    [InlineData("en-US")]
    public async Task Rotctld_IsLocaleIndependent(string culture)
    {
        // "270.000000" parsed under a comma-decimal culture is 270000000, not 270 — and a
        // command formatted under one goes out as "P 270,6 0", which no controller accepts.
        var previous = CultureInfo.CurrentCulture;
        CultureInfo.CurrentCulture = new CultureInfo(culture);
        try
        {
            var poll = Rotctld("270.000000");
            var az = await new RotctldProtocol().PollAzimuthAsync(poll, default);
            az.Should().Be(270.0);

            var set = Rotctld("0.0");
            await new RotctldProtocol().SetAzimuthAsync(270.6, set, default);
            set.Writes.Single().Should().Be("P 270.6 0\n");
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }
    }
}
