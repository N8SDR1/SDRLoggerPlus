namespace SDRLoggerPlus.Server.Core.Security;

/// <summary>
/// Console commands for managing device access tokens, so a headless server can be
/// bootstrapped before any login UI exists (that arrives with the client work). The
/// launcher passes these instead of starting the web host:
///   SDRLoggerPlus --auth-add-device "my phone"   → mints + prints a token, then exits
///   SDRLoggerPlus --auth-list-devices
///   SDRLoggerPlus --auth-revoke &lt;device-id&gt;
/// The token is printed to the console ONCE (never persisted in plaintext, never logged).
/// </summary>
public static class AuthCli
{
    /// <summary>Handle a recognised auth command. Returns true if one ran (caller should exit).</summary>
    public static bool TryHandle(string[] args, AuthTokenStore store)
    {
        if (args.Length == 0) return false;
        switch (args[0])
        {
            case "--auth-add-device":
                var name = args.Length > 1 ? args[1] : "device";
                var (token, dev) = store.Issue(name);
                Console.WriteLine();
                Console.WriteLine($"Access token for \"{dev.Name}\"  (device id {dev.Id}):");
                Console.WriteLine();
                Console.WriteLine("    " + token);
                Console.WriteLine();
                Console.WriteLine("Save it now — it is shown only once and is not stored anywhere in readable form.");
                Console.WriteLine("Enter it on the device under Settings → Server.");
                return true;

            case "--auth-list-devices":
                var devices = store.List();
                if (devices.Count == 0) { Console.WriteLine("(no devices registered)"); return true; }
                foreach (var d in devices)
                    Console.WriteLine($"{d.Id}  {d.Name,-24}  created {d.CreatedUtc:u}  last-seen {(d.LastSeenUtc?.ToString("u") ?? "never")}");
                return true;

            case "--auth-revoke":
                if (args.Length < 2) { Console.WriteLine("usage: --auth-revoke <device-id>"); return true; }
                Console.WriteLine(store.Revoke(args[1])
                    ? $"Revoked device {args[1]}."
                    : $"No device with id {args[1]}.");
                return true;
        }
        return false;
    }
}
