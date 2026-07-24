using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Text.RegularExpressions;
using System.IO.Ports;

namespace SDRLoggerPlus.Server.Native.Serial;

/// <summary>
/// Serial ports with enough context to pick the right one.
/// </summary>
/// <param name="PortName">"COM5" — what actually gets opened.</param>
/// <param name="Description">The device's friendly name, e.g. "Silicon Labs CP210x USB to UART Bridge".</param>
/// <param name="HardwareId">Raw USB id, e.g. "USB\VID_10C4&amp;PID_EA60" — null for non-USB ports.</param>
/// <param name="IsUsb">True for a USB device. Radios are USB; an on-board COM1 almost never is.</param>
public record SerialPortInfo(string PortName, string? Description, string? HardwareId, bool IsUsb);

/// <summary>
/// Lists serial ports WITH their device names.
///
/// SerialPort.GetPortNames() returns bare "COM3, COM5, COM7", which is not enough to
/// choose from: modern Icom and Yaesu radios expose TWO ports over one USB cable, and
/// picking the wrong one is indistinguishable from a broken cable. Windows knows the
/// device name behind each port, so we ask it.
///
/// Uses SetupAPI directly rather than taking a WMI dependency — the same P/Invoke
/// approach already used for Hamlib. Any failure degrades to bare port names, which is
/// exactly what the caller had before.
/// </summary>
public static class SerialPortEnumerator
{
    /// <summary>USB vendor ids of the USB-to-serial bridges radios are built around.</summary>
    private static readonly Dictionary<string, string> KnownUsbVendors = new(StringComparer.OrdinalIgnoreCase)
    {
        ["10C4"] = "Silicon Labs",   // CP210x — Icom IC-7300/7610/705, Yaesu FTDX10/FT-710
        ["0403"] = "FTDI",           // many rigs and interfaces
        ["067B"] = "Prolific",
        ["1A86"] = "CH340",
        ["04D8"] = "Microchip",
        ["0C26"] = "Kenwood",
    };

    public static List<SerialPortInfo> List()
    {
        try
        {
            if (OperatingSystem.IsWindows())
            {
                var enriched = ListWindows();
                if (enriched.Count > 0) return enriched;
            }
        }
        catch
        {
            // Fall through to bare names — a port list without descriptions is still usable.
        }

        return SerialPort.GetPortNames()
            .OrderBy(p => p, PortComparer)
            .Select(p => new SerialPortInfo(p, null, null, false))
            .ToList();
    }

    /// <summary>"COM9" before "COM10" — plain string order gets this wrong.</summary>
    private static readonly IComparer<string> PortComparer = Comparer<string>.Create((a, b) =>
    {
        static int Num(string s)
        {
            var m = Regex.Match(s, @"(\d+)$");
            return m.Success && int.TryParse(m.Groups[1].Value, out var n) ? n : int.MaxValue;
        }
        var d = Num(a).CompareTo(Num(b));
        return d != 0 ? d : string.CompareOrdinal(a, b);
    });

    [SupportedOSPlatform("windows")]
    private static List<SerialPortInfo> ListWindows()
    {
        var results = new List<SerialPortInfo>();
        // Ports are named directly by the driver, so the reliable route is the "Ports"
        // setup class rather than trying to correlate names after the fact.
        var portsClass = GUID_DEVCLASS_PORTS;
        var devInfo = SetupDiGetClassDevs(ref portsClass, null, IntPtr.Zero, DIGCF_PRESENT);
        if (devInfo == IntPtr.Zero || devInfo == INVALID_HANDLE_VALUE) return results;

        try
        {
            var data = new SP_DEVINFO_DATA { cbSize = (uint)Marshal.SizeOf<SP_DEVINFO_DATA>() };
            for (uint i = 0; SetupDiEnumDeviceInfo(devInfo, i, ref data); i++)
            {
                var portName = ReadPortName(devInfo, ref data);
                if (string.IsNullOrEmpty(portName)) continue;

                var friendly = ReadProperty(devInfo, ref data, SPDRP_FRIENDLYNAME)
                               ?? ReadProperty(devInfo, ref data, SPDRP_DEVICEDESC);
                var hardwareId = ReadProperty(devInfo, ref data, SPDRP_HARDWAREID);

                // The friendly name already ends in "(COM5)"; the port is shown separately,
                // so trim the duplication rather than print it twice.
                var description = friendly is null ? null
                    : Regex.Replace(friendly, @"\s*\(COM\d+\)\s*$", "").Trim();

                // FTDI's VCP driver enumerates under FTDIBUS rather than USB, so matching
                // "USB\" alone files a perfectly ordinary USB rig interface as non-USB.
                var isUsb = hardwareId is not null &&
                    (hardwareId.StartsWith("USB\\", StringComparison.OrdinalIgnoreCase) ||
                     hardwareId.StartsWith("FTDIBUS\\", StringComparison.OrdinalIgnoreCase));
                results.Add(new SerialPortInfo(portName, string.IsNullOrEmpty(description) ? null : description,
                                               hardwareId, isUsb));
            }
        }
        finally
        {
            SetupDiDestroyDeviceInfoList(devInfo);
        }

        // USB devices first — a radio is on USB, and a machine's built-in COM1 is noise.
        return results
            .OrderByDescending(r => r.IsUsb)
            .ThenBy(r => r.PortName, PortComparer)
            .ToList();
    }

    /// <summary>Vendor name for a hardware id, when we recognise the USB VID.</summary>
    public static string? VendorFor(string? hardwareId)
    {
        if (string.IsNullOrEmpty(hardwareId)) return null;
        var m = Regex.Match(hardwareId, @"VID_([0-9A-Fa-f]{4})");
        return m.Success && KnownUsbVendors.TryGetValue(m.Groups[1].Value, out var vendor) ? vendor : null;
    }

    // --- SetupAPI ---

    [SupportedOSPlatform("windows")]
    private static string? ReadPortName(IntPtr devInfo, ref SP_DEVINFO_DATA data)
    {
        var key = SetupDiOpenDevRegKey(devInfo, ref data, DICS_FLAG_GLOBAL, 0, DIREG_DEV, KEY_READ);
        if (key == IntPtr.Zero || key == INVALID_HANDLE_VALUE) return null;
        try
        {
            var buffer = new byte[256];
            var len = (uint)buffer.Length;
            uint type = 0;
            if (RegQueryValueEx(key, "PortName", IntPtr.Zero, ref type, buffer, ref len) != 0) return null;
            return TrimString(buffer, len);
        }
        finally
        {
            RegCloseKey(key);
        }
    }

    [SupportedOSPlatform("windows")]
    private static string? ReadProperty(IntPtr devInfo, ref SP_DEVINFO_DATA data, uint property)
    {
        var buffer = new byte[1024];
        if (!SetupDiGetDeviceRegistryProperty(devInfo, ref data, property, out _, buffer, (uint)buffer.Length, out var required))
            return null;
        return TrimString(buffer, Math.Min(required, (uint)buffer.Length));
    }

    /// <summary>Unicode bytes → string, stopping at the first NUL (REG_MULTI_SZ gives several).</summary>
    private static string? TrimString(byte[] buffer, uint byteLength)
    {
        if (byteLength == 0) return null;
        var s = System.Text.Encoding.Unicode.GetString(buffer, 0, (int)Math.Min(byteLength, buffer.Length));
        var nul = s.IndexOf('\0');
        if (nul >= 0) s = s[..nul];
        return string.IsNullOrWhiteSpace(s) ? null : s;
    }

    private static readonly IntPtr INVALID_HANDLE_VALUE = new(-1);
    private static Guid GUID_DEVCLASS_PORTS = new("4d36e978-e325-11ce-bfc1-08002be10318");
    private const uint DIGCF_PRESENT = 0x02;
    private const uint SPDRP_DEVICEDESC = 0x00;
    private const uint SPDRP_HARDWAREID = 0x01;
    private const uint SPDRP_FRIENDLYNAME = 0x0C;
    private const uint DICS_FLAG_GLOBAL = 0x01;
    private const uint DIREG_DEV = 0x01;
    private const uint KEY_READ = 0x20019;

    [StructLayout(LayoutKind.Sequential)]
    private struct SP_DEVINFO_DATA
    {
        public uint cbSize;
        public Guid ClassGuid;
        public uint DevInst;
        public IntPtr Reserved;
    }

    [DllImport("setupapi.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern IntPtr SetupDiGetClassDevs(ref Guid classGuid, string? enumerator, IntPtr hwndParent, uint flags);

    [DllImport("setupapi.dll", SetLastError = true)]
    private static extern bool SetupDiEnumDeviceInfo(IntPtr devInfo, uint memberIndex, ref SP_DEVINFO_DATA deviceInfoData);

    [DllImport("setupapi.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern bool SetupDiGetDeviceRegistryProperty(IntPtr devInfo, ref SP_DEVINFO_DATA deviceInfoData,
        uint property, out uint propertyRegDataType, byte[] propertyBuffer, uint propertyBufferSize, out uint requiredSize);

    [DllImport("setupapi.dll", SetLastError = true)]
    private static extern IntPtr SetupDiOpenDevRegKey(IntPtr devInfo, ref SP_DEVINFO_DATA deviceInfoData,
        uint scope, uint hwProfile, uint keyType, uint samDesired);

    [DllImport("setupapi.dll", SetLastError = true)]
    private static extern bool SetupDiDestroyDeviceInfoList(IntPtr devInfo);

    [DllImport("advapi32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern int RegQueryValueEx(IntPtr key, string valueName, IntPtr reserved,
        ref uint type, byte[] data, ref uint dataSize);

    [DllImport("advapi32.dll", SetLastError = true)]
    private static extern int RegCloseKey(IntPtr key);
}
