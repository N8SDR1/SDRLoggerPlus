namespace SDRLoggerPlus.Contracts.Models;

/// <summary>One RBN reception ("skimmer heard my callsign") for the globe Heard-Me layer.</summary>
public class RbnHeardMeReport
{
    public string Skimmer { get; set; } = string.Empty;
    public double Lat { get; set; }
    public double Lon { get; set; }
    public double FreqKhz { get; set; }
    public string Band { get; set; } = string.Empty;
    public string Mode { get; set; } = string.Empty;
    public int Snr { get; set; }
    public long AgeSeconds { get; set; }
}
