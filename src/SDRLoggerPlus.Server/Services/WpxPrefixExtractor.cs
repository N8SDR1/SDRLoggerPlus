namespace SDRLoggerPlus.Server.Services;

/// <summary>
/// CQ WPX-style prefix extraction, ported from SDRLogger+ so award counts
/// match: strip short portable suffixes, prefer the shorter side of stroke
/// calls, prefix ends at the last digit of the leading alphanumeric run,
/// and digit-less calls use first letter + "0" (WPX convention).
/// </summary>
public static class WpxPrefixExtractor
{
    private static readonly HashSet<string> PortableSuffixes =
        new(StringComparer.Ordinal) { "P", "M", "QRP", "MM", "AM", "A", "B" };

    public static string? Extract(string callsign)
    {
        var call = callsign.ToUpperInvariant().Trim();
        if (call.Length == 0)
            return null;

        var parts = call.Split('/');
        string basePart;
        if (parts.Length == 1)
        {
            basePart = parts[0];
        }
        else if (parts.Length == 2)
        {
            if (parts[1].Length <= 3 && PortableSuffixes.Contains(parts[1]))
                basePart = parts[0];
            else
                // Stroke call: the shorter part is the prefix source
                // (VK9/N8SDR and N8SDR/VK9 both → VK9)
                basePart = parts[0].Length < parts[1].Length ? parts[0] : parts[1];
        }
        else
        {
            basePart = parts[0];
        }

        // Prefix = everything through the LAST digit (e.g. 3DA0XYZ → 3DA0).
        // SDRLogger+'s loop stopped at the first letter after any digit,
        // returning "3" for 3DA0XYZ against its own docstring — deliberate
        // deviation here in favor of the documented WPX rule.
        var lastDigitPos = -1;
        for (var i = 0; i < basePart.Length; i++)
        {
            if (char.IsDigit(basePart[i]))
                lastDigitPos = i;
        }

        if (lastDigitPos >= 0)
            return basePart[..(lastDigitPos + 1)];

        // No digit: WPX convention is first letter + "0"
        return basePart.Length > 0 ? basePart[0] + "0" : null;
    }
}
