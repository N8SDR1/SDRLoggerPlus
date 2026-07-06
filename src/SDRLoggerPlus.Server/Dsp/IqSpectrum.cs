namespace SDRLoggerPlus.Server.Dsp;

/// <summary>
/// Converts a block of interleaved I/Q float samples into a downsampled,
/// non-negative integer power spectrum for the panadapter: frequency-ascending
/// with DC centered, where a higher value means a stronger signal. The frontend
/// auto-scales by a rolling peak, so the absolute dB offset is arbitrary — only
/// relative levels matter.
/// </summary>
public static class IqSpectrum
{
    // dB-to-int mapping: amp = (dB + Offset) * Gain, clamped to >= 0. Offset lifts
    // the (negative) dB scale into a positive range; Gain gives integer resolution.
    private const double DbOffset = 160.0;
    private const double DbGain = 10.0;

    /// <summary>
    /// Compute an <paramref name="fftSize"/>-point windowed FFT power spectrum from
    /// the first <paramref name="fftSize"/> complex samples of
    /// <paramref name="iqInterleaved"/> (I,Q,I,Q…), fftshifted so index 0 is the
    /// most-negative frequency and the last index the most-positive, then max-hold
    /// downsampled to <paramref name="outBins"/>.
    /// </summary>
    public static int[] PowerSpectrumDb(ReadOnlySpan<float> iqInterleaved, int fftSize, double[] window, int outBins)
    {
        if (window.Length != fftSize)
            throw new ArgumentException("Window length must equal fftSize", nameof(window));
        if (iqInterleaved.Length < fftSize * 2)
            throw new ArgumentException("Not enough IQ samples for the requested FFT size", nameof(iqInterleaved));

        // HL2 baseband → RF-oriented spectrum: complex conjugation.
        //
        // The Hermes Lite 2 (and the openHPSDR family in general) delivers
        // IQ baseband whose spectrum is MIRRORED around DC relative to the
        // RF spectrum — a USB audio tone at +1 kHz above the carrier appears
        // as energy at -1 kHz in the baseband, LSB at -1 kHz shows as +1
        // kHz, and so on. This is a fixed property of the HL2's
        // downconversion path, not an artifact.
        //
        // The TCI protocol's iq_stream carries the RAW baseband so consumer
        // apps can apply their own DSP (this matches Thetis's original
        // behaviour, which Lyra follows). Thetis's and Lyra's OWN
        // panadapters look RF-correct because their WDSP demodulation chain
        // performs the un-mirror as part of sideband selection. A TCI
        // client that wants to show "RF spectrum around the VFO" (which is
        // what operators expect from a panadapter) has to apply the
        // un-mirror at the point of display.
        //
        // Conjugating the input (real part unchanged, imaginary part
        // negated) is mathematically identical to mirroring the spectrum
        // around DC, and that's exactly the correction the HL2 baseband
        // requires.
        var re = new double[fftSize];
        var im = new double[fftSize];
        for (int i = 0; i < fftSize; i++)
        {
            re[i] = iqInterleaved[i * 2] * window[i];
            im[i] = -iqInterleaved[i * 2 + 1] * window[i];
        }

        Fft.Transform(re, im);

        // Magnitude -> dB -> non-negative int, with an fftshift so the negative
        // frequencies (upper FFT half) move to the low output indices and DC sits
        // at the center.
        int half = fftSize / 2;
        var full = new int[fftSize];
        for (int i = 0; i < fftSize; i++)
        {
            double mag = Math.Sqrt(re[i] * re[i] + im[i] * im[i]) / fftSize;
            double db = 20.0 * Math.Log10(mag + 1e-12);
            int amp = (int)Math.Max(0.0, (db + DbOffset) * DbGain);
            int shifted = (i + half) % fftSize;
            full[shifted] = amp;
        }

        return outBins >= fftSize ? full : Downsample(full, outBins);
    }

    /// <summary>Max-hold downsample so transient peaks survive the reduction.</summary>
    private static int[] Downsample(int[] source, int targetCount)
    {
        var result = new int[targetCount];
        double binSize = (double)source.Length / targetCount;
        for (int i = 0; i < targetCount; i++)
        {
            int start = (int)(i * binSize);
            int end = (int)((i + 1) * binSize);
            if (end > source.Length) end = source.Length;
            if (end <= start) end = Math.Min(start + 1, source.Length);

            int max = int.MinValue;
            for (int j = start; j < end; j++)
                if (source[j] > max) max = source[j];
            result[i] = max;
        }
        return result;
    }
}
