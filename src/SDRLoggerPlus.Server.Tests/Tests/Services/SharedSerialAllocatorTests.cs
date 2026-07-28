using FluentAssertions;
using SDRLoggerPlus.Contracts.Models.Contesting;
using SDRLoggerPlus.Server.Services.Contesting;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

/// <summary>
/// S5 shared serials: one atomic sequence per contest+scope so a fast running op and a slow S&amp;P op
/// never hand out the same number, and the sequence survives a host restart.
/// </summary>
[Trait("Category", "Unit")]
public class SharedSerialAllocatorTests : IDisposable
{
    private readonly string _path = Path.Combine(Path.GetTempPath(), $"sdrl-serials-{Guid.NewGuid():N}.json");
    public void Dispose() { if (File.Exists(_path)) File.Delete(_path); }

    [Fact]
    public void AllBand_isOneSharedSequence_acrossBandsAndOps()
    {
        var a = new SharedSerialAllocator(_path);
        // Two stations on different bands still draw from the ONE all-band sequence.
        a.Next("cq-wpx-cw", SerialMode.AllBand, "20m").Should().Be(1); // op A
        a.Next("cq-wpx-cw", SerialMode.AllBand, "40m").Should().Be(2); // op B, other band
        a.Next("cq-wpx-cw", SerialMode.AllBand, "20m").Should().Be(3); // op A again
    }

    [Fact]
    public void PerBand_countersAreIndependent()
    {
        var a = new SharedSerialAllocator(_path);
        a.Next("arrl-10m", SerialMode.PerBand, "10m").Should().Be(1);
        a.Next("arrl-10m", SerialMode.PerBand, "10m").Should().Be(2);
        a.Next("arrl-10m", SerialMode.PerBand, "15m").Should().Be(1); // separate band counter
    }

    [Fact]
    public void SequenceSurvives_aHostRestart()
    {
        new SharedSerialAllocator(_path).Next("x", SerialMode.AllBand, "20m").Should().Be(1);
        // Fresh instance = a restarted host reading the same file: continues, doesn't reset.
        new SharedSerialAllocator(_path).Next("x", SerialMode.AllBand, "20m").Should().Be(2);
    }

    [Fact]
    public void Peek_doesNotAdvance()
    {
        var a = new SharedSerialAllocator(_path);
        a.Peek("x", SerialMode.AllBand, "20m").Should().Be(1);
        a.Peek("x", SerialMode.AllBand, "20m").Should().Be(1); // still 1 — didn't consume
        a.Next("x", SerialMode.AllBand, "20m").Should().Be(1);
    }

    [Fact]
    public void None_returnsZero()
    {
        new SharedSerialAllocator(_path).Next("x", SerialMode.None, "20m").Should().Be(0);
    }
}
