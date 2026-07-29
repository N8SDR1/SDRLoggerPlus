using System.Net.Sockets;
using System.Text;
using System.Threading.Channels;

namespace SDRLoggerPlus.Server.Services.Rotator;

/// <summary>
/// One live rotator socket, presented to protocols as an <see cref="IRotatorChannel"/>.
///
/// A single background loop owns the reader and pushes every line it sees into a queue;
/// nothing else ever touches the socket's read side, so a command issued from a SignalR
/// hub thread can no longer race the polling loop for the controller's replies. Writes are
/// serialised for the same reason.
/// </summary>
public sealed class RotatorConnection : IRotatorChannel, IDisposable
{
    private readonly TcpClient? _client;
    private readonly Stream _stream;
    private readonly StreamReader _reader;
    private readonly StreamWriter _writer;
    private readonly Channel<string> _lines = Channel.CreateUnbounded<string>();
    private readonly CancellationTokenSource _cts = new();
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private readonly Task _readLoop;

    /// <summary>True once the peer closed the socket or the read loop failed.</summary>
    public bool IsClosed { get; private set; }

    public RotatorConnection(TcpClient client)
        : this(client.GetStream(), client) { }

    /// <summary>Stream-based entry point so tests can drive a connection without a socket.</summary>
    public RotatorConnection(Stream stream, TcpClient? client = null)
    {
        _client = client;
        _stream = stream;
        _reader = new StreamReader(stream, Encoding.ASCII);
        _writer = new StreamWriter(stream, Encoding.ASCII) { AutoFlush = true };
        _readLoop = Task.Run(ReadLoopAsync);
    }

    private async Task ReadLoopAsync()
    {
        try
        {
            while (!_cts.IsCancellationRequested)
            {
                var line = await _reader.ReadLineAsync(_cts.Token);
                if (line == null) break;              // peer closed
                _lines.Writer.TryWrite(line);
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception) { /* socket died; surfaced as IsClosed */ }
        finally
        {
            IsClosed = true;
            _lines.Writer.TryComplete();
        }
    }

    public int Drain()
    {
        var dropped = 0;
        while (_lines.Reader.TryRead(out _)) dropped++;
        return dropped;
    }

    public async Task WriteAsync(string text, CancellationToken ct)
    {
        await _writeLock.WaitAsync(ct);
        try
        {
            await _writer.WriteAsync(text.AsMemory(), ct);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    public async Task<string?> ReadLineAsync(TimeSpan timeout, CancellationToken ct)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(timeout);
        try
        {
            return await _lines.Reader.ReadAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
            ct.ThrowIfCancellationRequested();   // shutdown is real cancellation; a timeout is not
            return null;
        }
        catch (ChannelClosedException)
        {
            return null;                          // connection ended mid-wait
        }
    }

    public void Dispose()
    {
        try { _cts.Cancel(); } catch { }
        try { _stream.Dispose(); } catch { }
        try { _client?.Dispose(); } catch { }
        try { _readLoop.Wait(TimeSpan.FromSeconds(1)); } catch { }
        _cts.Dispose();
        _writeLock.Dispose();
    }
}
