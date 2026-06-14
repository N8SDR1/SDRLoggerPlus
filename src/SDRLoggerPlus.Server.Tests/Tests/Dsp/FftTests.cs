using FluentAssertions;
using SDRLoggerPlus.Server.Dsp;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Dsp;

/// <summary>
/// Correctness tests for the radix-2 FFT and Blackman-Harris window that feed the
/// backend IQ panadapter. Pure math, no hardware.
/// </summary>
[Trait("Category", "Unit")]
public class FftTests
{
    [Fact]
    public void Transform_Impulse_ProducesFlatMagnitudeSpectrum()
    {
        int n = 16;
        var re = new double[n];
        var im = new double[n];
        re[0] = 1.0; // unit impulse at t=0

        Fft.Transform(re, im);

        // FFT of a delta is a constant 1 across every bin.
        for (int i = 0; i < n; i++)
        {
            double mag = Math.Sqrt(re[i] * re[i] + im[i] * im[i]);
            mag.Should().BeApproximately(1.0, 1e-9);
        }
    }

    [Fact]
    public void Transform_SingleComplexSinusoid_ConcentratesEnergyInExpectedBin()
    {
        int n = 16, k = 4;
        var re = new double[n];
        var im = new double[n];
        for (int t = 0; t < n; t++)
        {
            double ang = 2.0 * Math.PI * k * t / n;
            re[t] = Math.Cos(ang);
            im[t] = Math.Sin(ang);
        }

        Fft.Transform(re, im);

        for (int i = 0; i < n; i++)
        {
            double mag = Math.Sqrt(re[i] * re[i] + im[i] * im[i]);
            if (i == k)
                mag.Should().BeApproximately(n, 1e-6);
            else
                mag.Should().BeApproximately(0.0, 1e-6);
        }
    }

    [Fact]
    public void Transform_ForwardThenInverse_RecoversInput()
    {
        int n = 32;
        var rng = new Random(1234);
        var re = new double[n];
        var im = new double[n];
        var re0 = new double[n];
        var im0 = new double[n];
        for (int i = 0; i < n; i++)
        {
            re[i] = re0[i] = rng.NextDouble() * 2 - 1;
            im[i] = im0[i] = rng.NextDouble() * 2 - 1;
        }

        Fft.Transform(re, im);
        Fft.Transform(re, im, inverse: true);

        for (int i = 0; i < n; i++)
        {
            re[i].Should().BeApproximately(re0[i], 1e-9);
            im[i].Should().BeApproximately(im0[i], 1e-9);
        }
    }

    [Fact]
    public void Transform_NonPowerOfTwoLength_Throws()
    {
        var re = new double[6];
        var im = new double[6];
        var act = () => Fft.Transform(re, im);
        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void BlackmanHarris_IsSymmetricWithNearZeroEndpointsAndUnityPeak()
    {
        var w = Fft.BlackmanHarris(64);

        w.Length.Should().Be(64);
        for (int i = 0; i < 32; i++)
            w[i].Should().BeApproximately(w[63 - i], 1e-12);

        w[0].Should().BeLessThan(0.01);
        w[63].Should().BeLessThan(0.01);

        double peak = w.Max();
        peak.Should().BeGreaterThan(0.95);
    }
}
