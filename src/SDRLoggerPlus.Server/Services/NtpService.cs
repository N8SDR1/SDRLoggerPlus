using System.Diagnostics;
using System.Net;
using System.Net.Sockets;

namespace SDRLoggerPlus.Server.Services;

public record NtpQueryResult(bool Reachable, DateTime ServerUtc, double OffsetMs, string Server, string? Error);
public record NtpResyncResult(bool Ok, string Detail);

/// <summary>
/// Queries public NTP (SNTP, UDP 123) so the banner clock can show how far the PC clock is off, and —
/// on Windows, with the user's UAC consent — corrects the system clock to the NTP time. Setting the
/// clock modifies a system setting, so it ALWAYS runs elevated and is only ever triggered by the
/// operator clicking "Sync now"; mac/Linux report the offset and leave the clock alone.
/// </summary>
public class NtpService
{
    private const string DefaultServer = "pool.ntp.org";
    private static readonly DateTime NtpEpoch = new(1900, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    private readonly ILogger<NtpService> _logger;

    public NtpService(ILogger<NtpService> logger) => _logger = logger;

    /// <summary>SNTP query: returns the server's UTC and this PC's offset (server − local), round-trip corrected.</summary>
    public async Task<NtpQueryResult> QueryAsync(string? server = null, CancellationToken ct = default)
    {
        server = string.IsNullOrWhiteSpace(server) ? DefaultServer : server.Trim();
        try
        {
            var request = new byte[48];
            request[0] = 0x1B; // LI = 0, VN = 3, Mode = 3 (client)

            var addresses = await Dns.GetHostAddressesAsync(server, ct);
            if (addresses.Length == 0)
                return new NtpQueryResult(false, default, 0, server, "Could not resolve the NTP server address.");
            var endpoint = new IPEndPoint(addresses[0], 123);

            using var udp = new UdpClient();
            var t0 = DateTime.UtcNow;
            await udp.SendAsync(request, request.Length, endpoint);

            var receive = udp.ReceiveAsync(ct).AsTask();
            var done = await Task.WhenAny(receive, Task.Delay(3000, ct));
            if (done != receive)
                return new NtpQueryResult(false, default, 0, server, "No response from the NTP server (timed out).");

            var response = (await receive).Buffer;
            var t3 = DateTime.UtcNow;
            if (response.Length < 48)
                return new NtpQueryResult(false, default, 0, server, "Malformed NTP response.");

            // Transmit timestamp — bytes 40..47 (seconds, then fraction), big-endian.
            ulong seconds = ((ulong)response[40] << 24) | ((ulong)response[41] << 16) | ((ulong)response[42] << 8) | response[43];
            ulong fraction = ((ulong)response[44] << 24) | ((ulong)response[45] << 16) | ((ulong)response[46] << 8) | response[47];
            var milliseconds = seconds * 1000 + fraction * 1000 / 0x100000000UL;
            var serverUtc = NtpEpoch.AddMilliseconds((long)milliseconds);

            // Correct for the half round-trip so the offset reflects "now".
            var halfRttMs = (t3 - t0).TotalMilliseconds / 2.0;
            var adjustedServerUtc = serverUtc.AddMilliseconds(halfRttMs);
            var offsetMs = (adjustedServerUtc - t3).TotalMilliseconds;

            return new NtpQueryResult(true, adjustedServerUtc, offsetMs, server, null);
        }
        catch (Exception ex)
        {
            _logger.LogInformation(ex, "NTP query to {Server} failed", server);
            return new NtpQueryResult(false, default, 0, server, ex.Message);
        }
    }

    /// <summary>
    /// Set the system clock to <paramref name="targetUtc"/>. Windows only, and always elevated: launches
    /// an elevated PowerShell (a UAC prompt the operator must approve). Declining UAC is reported, not fatal.
    /// </summary>
    public NtpResyncResult SetSystemClock(DateTime targetUtc)
    {
        if (!OperatingSystem.IsWindows())
            return new NtpResyncResult(false,
                "Automatic clock sync is Windows-only. On this OS, correct the clock in your date/time settings.");

        try
        {
            // Set-Date takes local wall-clock; convert the NTP UTC to local.
            var local = targetUtc.ToLocalTime();
            var psCommand = $"Set-Date -Date ([DateTime]'{local:yyyy-MM-ddTHH:mm:ss.fff}')";
            var psi = new ProcessStartInfo
            {
                FileName = "powershell.exe",
                Arguments = $"-NoProfile -NonInteractive -Command \"{psCommand}\"",
                UseShellExecute = true,   // required for Verb = runas
                Verb = "runas",           // triggers the UAC elevation prompt
                CreateNoWindow = true,
                WindowStyle = ProcessWindowStyle.Hidden,
            };

            using var proc = Process.Start(psi);
            if (proc is null)
                return new NtpResyncResult(false, "Could not start the clock-sync helper.");

            proc.WaitForExit(15000);
            if (!proc.HasExited)
                return new NtpResyncResult(false, "Clock-sync helper did not finish in time.");

            return proc.ExitCode == 0
                ? new NtpResyncResult(true, "PC clock synced from NTP.")
                : new NtpResyncResult(false, "Clock sync failed — administrator approval is required.");
        }
        catch (System.ComponentModel.Win32Exception wex) when (wex.NativeErrorCode == 1223)
        {
            // ERROR_CANCELLED — the operator dismissed the UAC prompt.
            return new NtpResyncResult(false, "Clock sync cancelled (admin approval was declined).");
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Set-system-clock failed");
            return new NtpResyncResult(false, $"Clock sync failed: {ex.Message}");
        }
    }
}
