using System.Net;
using System.Net.Sockets;
using System.Text;
using FluentAssertions;
using SDRLoggerPlus.Server.Services.Rotator;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

/// <summary>
/// <see cref="RotatorConnection"/> against a stand-in controller on loopback. These cover
/// the properties the protocols now rely on: reading never blocks the caller indefinitely,
/// unsolicited lines can be discarded, and a controller that goes quiet — or away — surfaces
/// as a null rather than a hang. The last one is the field bug: with no timeout on the
/// socket read, a controller that sent one line fewer than expected parked the rotator
/// service until unrelated traffic arrived, sometimes minutes later.
/// </summary>
[Trait("Category", "Unit")]
public class RotatorConnectionTests : IDisposable
{
    private readonly TcpListener _listener;
    private readonly List<TcpClient> _serverSides = new();

    public RotatorConnectionTests()
    {
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
    }

    /// <summary>Connect a client and hand back both ends.</summary>
    private async Task<(RotatorConnection client, StreamWriter server, StreamReader serverIn)> ConnectAsync()
    {
        var accept = _listener.AcceptTcpClientAsync();
        var client = new TcpClient();
        await client.ConnectAsync((IPEndPoint)_listener.LocalEndpoint);
        var serverSide = await accept;
        _serverSides.Add(serverSide);

        var stream = serverSide.GetStream();
        return (new RotatorConnection(client),
                new StreamWriter(stream, Encoding.ASCII) { AutoFlush = true },
                new StreamReader(stream, Encoding.ASCII));
    }

    private static readonly TimeSpan Patience = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan ShortWait = TimeSpan.FromMilliseconds(200);

    [Fact]
    public async Task DeliversLinesInOrder()
    {
        var (connection, server, _) = await ConnectAsync();
        using var _c = connection;

        await server.WriteAsync("270.0\n0.0\n");

        (await connection.ReadLineAsync(Patience, default)).Should().Be("270.0");
        (await connection.ReadLineAsync(Patience, default)).Should().Be("0.0");
    }

    [Fact]
    public async Task ReadReturnsNull_WhenControllerSaysNothing()
    {
        var (connection, _, _) = await ConnectAsync();
        using var _c = connection;

        var line = await connection.ReadLineAsync(ShortWait, default);

        line.Should().BeNull("a silent controller must time out, not park the caller");
    }

    [Fact]
    public async Task ReadReturnsNull_WhenControllerDisconnects()
    {
        var (connection, server, _) = await ConnectAsync();
        using var _c = connection;

        server.BaseStream.Dispose();

        (await connection.ReadLineAsync(Patience, default)).Should().BeNull();
        connection.IsClosed.Should().BeTrue();
    }

    [Fact]
    public async Task DrainDiscardsEverythingAlreadyReceived()
    {
        var (connection, server, _) = await ConnectAsync();
        using var _c = connection;

        await server.WriteAsync("stale-1\nstale-2\nstale-3\n");
        // let the reader loop pick them up before draining
        (await connection.ReadLineAsync(Patience, default)).Should().Be("stale-1");

        var dropped = connection.Drain();
        await server.WriteAsync("fresh\n");

        dropped.Should().Be(2);
        (await connection.ReadLineAsync(Patience, default)).Should().Be("fresh");
    }

    [Fact]
    public async Task WritesReachTheController()
    {
        var (connection, _, serverIn) = await ConnectAsync();
        using var _c = connection;

        await connection.WriteAsync("p\n", default);

        (await serverIn.ReadLineAsync()).Should().Be("p");
    }

    [Fact]
    public async Task ConcurrentWritesDoNotInterleave()
    {
        // A heading commanded from the UI runs on a hub thread while the polling loop is
        // mid-exchange. Bytes from the two must not end up spliced into one nonsense command.
        var (connection, _, serverIn) = await ConnectAsync();
        using var _c = connection;

        await Task.WhenAll(
            Enumerable.Range(0, 20).Select(i =>
                connection.WriteAsync(i % 2 == 0 ? "p\n" : "P 123.0 0\n", default)));

        var seen = new List<string>();
        for (var i = 0; i < 20; i++) seen.Add((await serverIn.ReadLineAsync())!);

        seen.Should().OnlyContain(line => line == "p" || line == "P 123.0 0");
    }

    public void Dispose()
    {
        foreach (var s in _serverSides) { try { s.Dispose(); } catch { } }
        _listener.Stop();
    }
}
