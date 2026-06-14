using FluentAssertions;
using SDRLoggerPlus.Server.Dsp;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Dsp;

/// <summary>
/// End-to-end checks for the IQ -> windowed FFT -> fftshift -> downsample
/// pipeline that produces the panadapter's power spectrum.
/// </summary>
[Trait("Category", "Unit")]
public class IqSpectrumTests
{
    [Fact]
    public void PowerSpectrumDb_DcTone_PeaksAtCenterBin()
    {
        const int fft = 4096, outBins = 512;
        var iq = new float[fft * 2];
        for (int i = 0; i < fft; i++)
        {
            iq[i * 2] = 1f;     // I = 1 (DC)
            iq[i * 2 + 1] = 0f; // Q = 0
        }

        var spec = IqSpectrum.PowerSpectrumDb(iq, fft, Fft.BlackmanHarris(fft), outBins);

        spec.Length.Should().Be(outBins);

        int argmax = 0;
        for (int i = 1; i < spec.Length; i++)
            if (spec[i] > spec[argmax]) argmax = i;

        // After fftshift, DC sits at the center of the span.
        argmax.Should().BeInRange(outBins / 2 - 2, outBins / 2 + 2);
    }

    [Fact]
    public void PowerSpectrumDb_OutputIsNonNegative()
    {
        const int fft = 1024, outBins = 256;
        var iq = new float[fft * 2];
        var rng = new Random(7);
        for (int i = 0; i < iq.Length; i++)
            iq[i] = (float)(rng.NextDouble() * 2 - 1);

        var spec = IqSpectrum.PowerSpectrumDb(iq, fft, Fft.BlackmanHarris(fft), outBins);

        spec.Should().OnlyContain(v => v >= 0);
    }

    [Fact]
    public void PowerSpectrumDb_TooFewSamples_Throws()
    {
        var iq = new float[100];
        var act = () => IqSpectrum.PowerSpectrumDb(iq, 4096, Fft.BlackmanHarris(4096), 512);
        act.Should().Throw<ArgumentException>();
    }
}
