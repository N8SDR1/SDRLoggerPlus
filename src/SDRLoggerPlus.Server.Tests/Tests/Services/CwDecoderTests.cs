using FluentAssertions;
using SDRLoggerPlus.Server.Services.Cw;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

/// <summary>
/// Feasibility-spike tests: synthesize keyed CW audio in memory and verify
/// the decoder copies it. See docs/design/cw-decoder-feasibility.md.
/// </summary>
[Trait("Category", "Unit")]
public class CwDecoderTests
{
    private const double SampleRate = 8000;
    private const double ToneHz = 600;

    private static readonly Dictionary<char, string> Morse = new()
    {
        ['A'] = ".-", ['B'] = "-...", ['C'] = "-.-.", ['D'] = "-..", ['E'] = ".",
        ['F'] = "..-.", ['G'] = "--.", ['H'] = "....", ['I'] = "..", ['J'] = ".---",
        ['K'] = "-.-", ['L'] = ".-..", ['M'] = "--", ['N'] = "-.", ['O'] = "---",
        ['P'] = ".--.", ['Q'] = "--.-", ['R'] = ".-.", ['S'] = "...", ['T'] = "-",
        ['U'] = "..-", ['V'] = "...-", ['W'] = ".--", ['X'] = "-..-", ['Y'] = "-.--",
        ['Z'] = "--..", ['0'] = "-----", ['1'] = ".----", ['2'] = "..---",
        ['3'] = "...--", ['4'] = "....-", ['5'] = ".....", ['6'] = "-....",
        ['7'] = "--...", ['8'] = "---..", ['9'] = "----.",
    };

    /// <summary>Generate keyed-tone CW audio for a message at the given WPM.</summary>
    private static float[] Synthesize(string message, int wpm, double noiseAmplitude = 0)
    {
        var ditSeconds = 1.2 / wpm; // PARIS standard
        var ditSamples = (int)(ditSeconds * SampleRate);
        var samples = new List<float>();
        var rng = new Random(42);
        var phase = 0.0;
        var phaseStep = 2 * Math.PI * ToneHz / SampleRate;

        void Append(int units, bool on)
        {
            for (var i = 0; i < units * ditSamples; i++)
            {
                phase += phaseStep;
                var v = on ? (float)(0.8 * Math.Sin(phase)) : 0f;
                if (noiseAmplitude > 0)
                    v += (float)((rng.NextDouble() * 2 - 1) * noiseAmplitude);
                samples.Add(v);
            }
        }

        Append(4, false); // leading silence
        foreach (var word in message.ToUpperInvariant().Split(' '))
        {
            foreach (var ch in word)
            {
                if (!Morse.TryGetValue(ch, out var code)) continue;
                foreach (var element in code)
                {
                    Append(element == '.' ? 1 : 3, on: true);
                    Append(1, false); // intra-character gap
                }
                Append(2, false);     // +1 already sent = 3 units character gap
            }
            Append(4, false);         // +3 already sent = 7 units word gap
        }
        return samples.ToArray();
    }

    private static double CharAccuracy(string expected, string actual)
    {
        // Levenshtein-free quick measure: fraction of expected chars matched in order
        var matches = 0;
        var ai = 0;
        foreach (var c in expected)
        {
            var idx = actual.IndexOf(c, ai);
            if (idx >= 0) { matches++; ai = idx + 1; }
        }
        return (double)matches / expected.Length;
    }

    [Theory]
    [InlineData("CQ TEST DE W8XYZ", 15)]
    [InlineData("CQ TEST DE W8XYZ", 20)]
    [InlineData("CQ TEST DE W8XYZ", 30)]
    public void Decode_CleanAudio_ExactCopy(string message, int wpm)
    {
        var audio = Synthesize(message, wpm);
        var decoded = new CwDecoder(SampleRate, ToneHz).Decode(audio);
        decoded.Should().Be(message);
    }

    [Fact]
    public void Decode_ModerateNoise_HighAccuracy()
    {
        // ~12 dB SNR (signal 0.8 peak vs noise 0.2 amplitude)
        var audio = Synthesize("CQ TEST DE W8XYZ", 20, noiseAmplitude: 0.2);
        var decoded = new CwDecoder(SampleRate, ToneHz).Decode(audio);
        CharAccuracy("CQ TEST DE W8XYZ", decoded).Should().BeGreaterThanOrEqualTo(0.95);
    }

    [Fact]
    public void Decode_HeavyNoise_DegradesGracefully()
    {
        var audio = Synthesize("CQ CQ CQ", 20, noiseAmplitude: 0.6);
        var decoded = new CwDecoder(SampleRate, ToneHz).Decode(audio);
        // No exactness demanded — just must not throw and not return garbage longer than input
        decoded.Length.Should().BeLessThan(30);
    }

    [Fact]
    public void Decode_Silence_ReturnsEmpty()
    {
        var silence = new float[(int)SampleRate * 2];
        new CwDecoder(SampleRate, ToneHz).Decode(silence).Should().Be("");
    }

    [Fact]
    public void Decode_PureNoiseNoSignal_ReturnsEmptyOrShortGarbage()
    {
        var rng = new Random(7);
        var noise = Enumerable.Range(0, (int)SampleRate * 2)
            .Select(_ => (float)(rng.NextDouble() * 0.4 - 0.2)).ToArray();
        var decoded = new CwDecoder(SampleRate, ToneHz).Decode(noise);
        decoded.Length.Should().BeLessThan(5);
    }
}
