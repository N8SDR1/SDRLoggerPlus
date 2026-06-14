using FluentAssertions;
using SDRLoggerPlus.Server.Services;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

[Trait("Category", "Unit")]
public class AdifMonitorTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "sdrloggerplus-adifmon-" + Guid.NewGuid());

    public AdifMonitorTests() => Directory.CreateDirectory(_dir);
    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private string WriteFile(string name, string content)
    {
        var path = Path.Combine(_dir, name);
        File.WriteAllText(path, content);
        return path;
    }

    // ── ReadAppended ─────────────────────────────────────────────

    [Fact]
    public void ReadAppended_FirstSight_ReturnsWholeFile()
    {
        var path = WriteFile("a.adi", "<CALL:5>K5XYZ<EOR>");
        var result = AdifMonitorService.ReadAppended(path, 0);
        result.Should().NotBeNull();
        result!.Value.Fragment.Should().Contain("K5XYZ");
        result.Value.NewOffset.Should().Be(new FileInfo(path).Length);
    }

    [Fact]
    public void ReadAppended_NoChange_ReturnsNull()
    {
        var path = WriteFile("a.adi", "<CALL:5>K5XYZ<EOR>");
        var len = new FileInfo(path).Length;
        AdifMonitorService.ReadAppended(path, len).Should().BeNull();
    }

    [Fact]
    public void ReadAppended_Appended_ReturnsOnlyNewBytes()
    {
        var path = WriteFile("a.adi", "<CALL:5>K5XYZ<EOR>\n");
        var offset = new FileInfo(path).Length;
        File.AppendAllText(path, "<CALL:4>W9AB<EOR>\n");

        var result = AdifMonitorService.ReadAppended(path, offset);
        result.Should().NotBeNull();
        result!.Value.Fragment.Should().Contain("W9AB");
        result.Value.Fragment.Should().NotContain("K5XYZ");
    }

    [Fact]
    public void ReadAppended_FileShrank_RestartsFromZero()
    {
        var path = WriteFile("a.adi", "<CALL:5>K5XYZ<EOR> long content here");
        var bigOffset = new FileInfo(path).Length;
        File.WriteAllText(path, "<CALL:4>W9AB<EOR>"); // rotated/replaced, shorter

        var result = AdifMonitorService.ReadAppended(path, bigOffset);
        result.Should().NotBeNull();
        result!.Value.Fragment.Should().Contain("W9AB");
    }

    [Fact]
    public void ReadAppended_MissingFile_ReturnsNull()
    {
        AdifMonitorService.ReadAppended(Path.Combine(_dir, "missing.adi"), 0).Should().BeNull();
    }

    // ── state store ──────────────────────────────────────────────

    [Fact]
    public void StateStore_RoundTrips()
    {
        var store = new AdifMonitorStateStore(Path.Combine(_dir, "state.json"));
        store.SetOffset(@"C:\logs\varac.adi", 1234);
        store.Save();

        var reloaded = new AdifMonitorStateStore(Path.Combine(_dir, "state.json"));
        reloaded.GetOffset(@"C:\logs\varac.adi").Should().Be(1234);
        reloaded.GetOffset(@"C:\logs\other.adi").Should().Be(0);
    }
}
