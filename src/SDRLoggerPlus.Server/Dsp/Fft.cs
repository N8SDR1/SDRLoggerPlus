namespace SDRLoggerPlus.Server.Dsp;

/// <summary>
/// Minimal in-place iterative radix-2 Cooley-Tukey FFT plus window functions.
/// Operates on separate real/imaginary <see cref="double"/> arrays whose length
/// must be a power of two. Pure and allocation-light so it can run once per
/// spectrum frame on the TCI receive path.
/// </summary>
public static class Fft
{
    /// <summary>
    /// In-place complex FFT (inverse transform when <paramref name="inverse"/> is
    /// true, which also scales by 1/N). Length must be a power of two.
    /// </summary>
    public static void Transform(double[] re, double[] im, bool inverse = false)
    {
        int n = re.Length;
        if (n == 0) return;
        if ((n & (n - 1)) != 0)
            throw new ArgumentException("FFT length must be a power of two", nameof(re));
        if (im.Length != n)
            throw new ArgumentException("Real and imaginary arrays must be the same length", nameof(im));

        // Bit-reversal permutation
        for (int i = 1, j = 0; i < n; i++)
        {
            int bit = n >> 1;
            for (; (j & bit) != 0; bit >>= 1)
                j ^= bit;
            j ^= bit;
            if (i < j)
            {
                (re[i], re[j]) = (re[j], re[i]);
                (im[i], im[j]) = (im[j], im[i]);
            }
        }

        // Danielson-Lanczos butterflies
        for (int len = 2; len <= n; len <<= 1)
        {
            double ang = 2.0 * Math.PI / len * (inverse ? 1 : -1);
            double wReal = Math.Cos(ang);
            double wImag = Math.Sin(ang);
            for (int i = 0; i < n; i += len)
            {
                double curReal = 1.0, curImag = 0.0;
                for (int k = 0; k < len / 2; k++)
                {
                    int a = i + k;
                    int b = i + k + len / 2;
                    double tReal = re[b] * curReal - im[b] * curImag;
                    double tImag = re[b] * curImag + im[b] * curReal;
                    re[b] = re[a] - tReal;
                    im[b] = im[a] - tImag;
                    re[a] += tReal;
                    im[a] += tImag;
                    double nextReal = curReal * wReal - curImag * wImag;
                    curImag = curReal * wImag + curImag * wReal;
                    curReal = nextReal;
                }
            }
        }

        if (inverse)
        {
            for (int i = 0; i < n; i++)
            {
                re[i] /= n;
                im[i] /= n;
            }
        }
    }

    /// <summary>
    /// Four-term Blackman-Harris window of the given length. Low side-lobes make
    /// it a good default for a panadapter where dynamic range matters more than
    /// bin resolution.
    /// </summary>
    public static double[] BlackmanHarris(int n)
    {
        var w = new double[n];
        if (n == 1)
        {
            w[0] = 1.0;
            return w;
        }
        const double a0 = 0.35875, a1 = 0.48829, a2 = 0.14128, a3 = 0.01168;
        for (int i = 0; i < n; i++)
        {
            double t = 2.0 * Math.PI * i / (n - 1);
            w[i] = a0 - a1 * Math.Cos(t) + a2 * Math.Cos(2 * t) - a3 * Math.Cos(3 * t);
        }
        return w;
    }
}
