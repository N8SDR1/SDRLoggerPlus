using FluentAssertions;
using SDRLoggerPlus.Server.Services.Contesting;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

[Trait("Category", "Unit")]
public class ScpParseTests
{
    [Fact]
    public void Parse_SkipsCommentsAndBlanks_UppercasesAndSorts()
    {
        var lines = new[]
        {
            "# master.scp sample",
            "",
            "k1abc",
            "DL1ABC",
            "n8sdr",
        };

        var calls = ScpService.Parse(lines);

        calls.Should().Equal("DL1ABC", "K1ABC", "N8SDR"); // sorted, uppercased
    }

    [Fact]
    public void Parse_DedupesAndRejectsImplausibleTokens()
    {
        var lines = new[] { "K1ABC", "k1abc", "HELLO", "12345", "VK9/N8SDR" };

        var calls = ScpService.Parse(lines);

        calls.Should().Contain("K1ABC");
        calls.Should().Contain("VK9/N8SDR");
        calls.Should().NotContain("HELLO");   // no digit
        calls.Should().NotContain("12345");   // no letter
        calls.Count(c => c == "K1ABC").Should().Be(1); // deduped
    }
}

[Trait("Category", "Unit")]
public class CallHistoryParseTests
{
    [Fact]
    public void Parse_HeaderThenRows_MapsFieldKeys()
    {
        var lines = new[]
        {
            "# my call history",
            "Call,Name,State,CQZ,Grid",
            "K1ABC,John,NH,5,FN43",
            "n8sdr,Rick,OH,4,EN81",
        };

        var map = CallHistoryService.Parse(lines);

        map.Should().ContainKey("K1ABC");
        map["K1ABC"]["name"].Should().Be("John");
        map["K1ABC"]["state"].Should().Be("NH");
        map["K1ABC"]["zone"].Should().Be("5");   // "CQZ" → zone
        map["K1ABC"]["grid"].Should().Be("FN43");
        map.Should().ContainKey("N8SDR");        // case-insensitive key
        map["N8SDR"]["name"].Should().Be("Rick");
    }

    [Fact]
    public void Parse_IgnoresN1mmOrderToken()
    {
        var lines = new[]
        {
            "!!Order!!,Call,Name,State",
            "1,K1ABC,John,NH",
        };

        var map = CallHistoryService.Parse(lines);

        map.Should().ContainKey("K1ABC");
        map["K1ABC"]["name"].Should().Be("John");
        map["K1ABC"]["state"].Should().Be("NH");
        map["K1ABC"].Should().NotContainKey("call"); // call is the key, not a value
    }

    [Fact]
    public void Parse_SkipsBlankValues()
    {
        var map = CallHistoryService.Parse(new[]
        {
            "Call,Name,State",
            "K1ABC,,NH",
        });

        map["K1ABC"].Should().NotContainKey("name"); // blank dropped
        map["K1ABC"]["state"].Should().Be("NH");
    }
}
