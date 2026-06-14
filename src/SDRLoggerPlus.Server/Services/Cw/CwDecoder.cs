namespace SDRLoggerPlus.Server.Services.Cw;

/// <summary>
/// Experimental offline CW decoder — feasibility spike for the SDRLogger+
/// port (docs/design/cw-decoder-feasibility.md). Goertzel tone detection +
/// adaptive dit/dah timing. Operates on a complete buffer of mono float
/// samples; a future streaming version would carry state between blocks.
/// Not yet wired to any audio input — see the feasibility doc.
/// </summary>
public class CwDecoder
{
    private static readonly Dictionary<string, char> MorseTable = new()
    {
        [".-"] = 'A', ["-..."] = 'B', ["-.-."] = 'C', ["-.."] = 'D', ["."] = 'E',
        ["..-."] = 'F', ["--."] = 'G', ["...."] = 'H', [".."] = 'I', [".---"] = 'J',
        ["-.-"] = 'K', [".-.."] = 'L', ["--"] = 'M', ["-."] = 'N', ["---"] = 'O',
        [".--."] = 'P', ["--.-"] = 'Q', [".-."] = 'R', ["..."] = 'S', ["-"] = 'T',
        ["..-"] = 'U', ["...-"] = 'V', [".--"] = 'W', ["-..-"] = 'X', ["-.--"] = 'Y',
        ["--.."] = 'Z',
        ["-----"] = '0', [".----"] = '1', ["..---"] = '2', ["...--"] = '3', ["....-"] = '4',
        ["....."] = '5', ["-...."] = '6', ["--..."] = '7', ["---.."] = '8', ["----."] = '9',
        [".-.-.-"] = '.', ["--..--"] = ',', ["..--.."] = '?', ["-..-."] = '/',
        ["-...-"] = '=', [".-.-."] = '+', ["-....-"] = '-',
    };

    private readonly double _sampleRate;
    private readonly double _toneHz;
    private readonly int _blockSize;

    public CwDecoder(double sampleRate = 8000, double toneHz = 600)
    {
        _sampleRate = sampleRate;
        _toneHz = toneHz;
        _blockSize = (int)(sampleRate / 200); // 5 ms blocks
    }

    public string Decode(ReadOnlySpan<float> samples)
    {
        var envelope = ComputeEnvelope(samples);
        if (envelope.Count == 0) return "";

        var keyed = Threshold(envelope);
        var runs = ToRuns(keyed);
        return DecodeRuns(runs);
    }

    /// <summary>Goertzel magnitude per 5 ms block at the configured tone frequency.</summary>
    private List<double> ComputeEnvelope(ReadOnlySpan<float> samples)
    {
        var envelope = new List<double>(samples.Length / _blockSize + 1);
        var k = (int)(0.5 + _blockSize * _toneHz / _sampleRate);
        var omega = 2.0 * Math.PI * k / _blockSize;
        var coeff = 2.0 * Math.Cos(omega);

        for (var start = 0; start + _blockSize <= samples.Length; start += _blockSize)
        {
            double q0, q1 = 0, q2 = 0;
            for (var i = 0; i < _blockSize; i++)
            {
                q0 = coeff * q1 - q2 + samples[start + i];
                q2 = q1;
                q1 = q0;
            }
            var magnitude = Math.Sqrt(q1 * q1 + q2 * q2 - q1 * q2 * coeff);
            envelope.Add(magnitude);
        }
        return envelope;
    }

    /// <summary>Adaptive on/off threshold halfway between the noise floor and peak.</summary>
    private static bool[] Threshold(List<double> envelope)
    {
        var sorted = envelope.OrderBy(v => v).ToList();
        var floor = sorted[(int)(sorted.Count * 0.2)];
        var peak = sorted[(int)(sorted.Count * 0.95)];
        var threshold = floor + (peak - floor) * 0.5;
        // Squelch: a keyed tone shows a large peak/floor ratio; broadband
        // noise alone stays within a few dB of its own floor
        if (peak <= floor * 4) return new bool[envelope.Count];

        var keyed = envelope.Select(v => v > threshold).ToArray();

        // De-glitch: flip isolated single blocks
        for (var i = 1; i < keyed.Length - 1; i++)
        {
            if (keyed[i] != keyed[i - 1] && keyed[i] != keyed[i + 1])
                keyed[i] = keyed[i - 1];
        }
        return keyed;
    }

    private static List<(bool On, int Blocks)> ToRuns(bool[] keyed)
    {
        var runs = new List<(bool, int)>();
        var current = keyed.Length > 0 && keyed[0];
        var length = 0;
        foreach (var on in keyed)
        {
            if (on == current) { length++; continue; }
            runs.Add((current, length));
            current = on;
            length = 1;
        }
        if (length > 0) runs.Add((current, length));
        return runs;
    }

    private static string DecodeRuns(List<(bool On, int Blocks)> runs)
    {
        var marks = runs.Where(r => r.On).Select(r => r.Blocks).OrderBy(b => b).ToList();
        if (marks.Count == 0) return "";

        // Dit length estimate: median of the shorter half of mark runs
        // (a normal message has more dits than dahs; dahs are 3×)
        var shortMarks = marks.Take(Math.Max(1, marks.Count / 2)).ToList();
        var unit = shortMarks[shortMarks.Count / 2];
        if (unit <= 0) return "";

        var result = new System.Text.StringBuilder();
        var symbol = new System.Text.StringBuilder();

        void FlushSymbol()
        {
            if (symbol.Length == 0) return;
            result.Append(MorseTable.TryGetValue(symbol.ToString(), out var c) ? c : '~');
            symbol.Clear();
        }

        for (var i = 0; i < runs.Count; i++)
        {
            var (on, blocks) = runs[i];
            var units = (double)blocks / unit;
            if (on)
            {
                symbol.Append(units < 2.0 ? '.' : '-');
            }
            else if (i > 0 && i < runs.Count - 1) // ignore leading/trailing silence
            {
                if (units >= 5.0)
                {
                    FlushSymbol();
                    result.Append(' ');
                }
                else if (units >= 2.0)
                {
                    FlushSymbol();
                }
                // < 2 units: intra-character gap, nothing to do
            }
        }
        FlushSymbol();
        return result.ToString().Trim();
    }
}
