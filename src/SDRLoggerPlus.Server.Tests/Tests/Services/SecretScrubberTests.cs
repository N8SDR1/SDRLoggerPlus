using FluentAssertions;
using SDRLoggerPlus.Server.Core.Logging;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

/// <summary>
/// The scrubber is the last thing standing between a credentialed URL and
/// main.log, so these tests are written around the URLs this app actually
/// builds rather than around invented ones.
/// </summary>
[Trait("Category", "Unit")]
public class SecretScrubberTests
{
    [Fact]
    public void MasksThePasswordInALotwReportUrl()
    {
        var url = "https://lotw.arrl.org/lotwuser/lotwreport.adi?qso_query=1&login=N9BC&password=Hunter2Secret&qso_qsl=yes";

        var scrubbed = SecretScrubber.Redact(url);

        scrubbed.Should().NotContain("Hunter2Secret");
        scrubbed.Should().Contain("login=N9BC", "only credentials are masked — the rest must stay debuggable");
        scrubbed.Should().Contain("qso_qsl=yes");
    }

    [Fact]
    public void MasksTheEqslInboxPassword()
    {
        var url = "https://www.eQSL.cc/qslcard/DownloadInBox.cfm?UserName=N9BC&Password=p%40ssw0rd-long&RcvdSince=202607010000";

        SecretScrubber.Redact(url).Should().NotContain("p%40ssw0rd-long");
    }

    [Theory]
    [InlineData("https://clublog.org/getlotwstate.php?api=CLUBLOGKEY123&email=a@b.c&password=pw-secret-1", "CLUBLOGKEY123")]
    [InlineData("https://clublog.org/getlotwstate.php?api=CLUBLOGKEY123&email=a@b.c&password=pw-secret-1", "pw-secret-1")]
    [InlineData("https://xmldata.qrz.com/xml/current/?username=N9BC&password=qrzpassword9", "qrzpassword9")]
    [InlineData("https://xmldata.qrz.com/xml/current/?s=SESSIONKEY99887&callsign=W1AW", "SESSIONKEY99887")]
    [InlineData("https://rt.ambientweather.net/v1/devices?apiKey=AMBIENTKEY1&applicationKey=AMBIENTAPP2", "AMBIENTKEY1")]
    [InlineData("https://rt.ambientweather.net/v1/devices?apiKey=AMBIENTKEY1&applicationKey=AMBIENTAPP2", "AMBIENTAPP2")]
    [InlineData("https://api.ecowitt.net/x?application_key=ECOAPP55&api_key=ECOAPI66&mac=AA:BB", "ECOAPP55")]
    [InlineData("https://api.ecowitt.net/x?application_key=ECOAPP55&api_key=ECOAPI66&mac=AA:BB", "ECOAPI66")]
    [InlineData("POST body: upload_code=HRD12345&Callsign=N9BC", "HRD12345")]
    public void MasksEveryCredentialParameterShapeThisAppSends(string text, string secretValue)
    {
        SecretScrubber.Redact(text).Should().NotContain(secretValue);
    }

    [Fact]
    public void MasksAKnownSecretThatAppearsWithNoParameterNameAroundIt()
    {
        // QRZ's logbook API quotes the rejected key back inside REASON=, which
        // no name=value rule can catch. This is what the value list is for.
        const string apiKey = "1234-ABCD-5678-EFGH";
        var response = $"RESULT=AUTH&REASON=invalid api key {apiKey}&EXTENDED=";

        var scrubbed = SecretScrubber.Redact(response, apiKey);

        scrubbed.Should().NotContain(apiKey);
        scrubbed.Should().Contain("RESULT=AUTH", "the diagnosis has to survive the masking");
    }

    [Fact]
    public void MasksTheTrimmedFormOfAWhitespacePaddedSecret()
    {
        // Services trim credentials before sending (whitespace from copy-paste
        // is why the trims exist), so what a rejection echoes back is the
        // TRIMMED value. Exact-match replacement on the padded stored value
        // would miss it in precisely the configurations the trims are for.
        var scrubbed = SecretScrubber.Redact("rejected key CODE-123456 try again", "  CODE-123456  ");

        scrubbed.Should().NotContain("CODE-123456");
    }

    [Fact]
    public void MasksASecretEvenWhenItReachesTheTextPercentEncoded()
    {
        const string password = "p@ss word/99";

        var scrubbed = SecretScrubber.Redact($"failed for {Uri.EscapeDataString(password)}", password);

        scrubbed.Should().NotContain("p%40ss%20word%2F99");
    }

    [Fact]
    public void LeavesTextWithNoCredentialsUntouched()
    {
        const string line = "Club Log: uploading W1AW 20m CW — HTTP 200";

        SecretScrubber.Redact(line).Should().Be(line);
    }

    [Theory]
    [InlineData("EQSL_PSWD=inbox-pass-77&EQSL_USER=N9BC", "inbox-pass-77")]
    [InlineData("form: user_password=under-scored-1", "under-scored-1")]
    public void MasksFieldNamesThatAreUnderscorePrefixed(string text, string secretValue)
    {
        // eQSL names every field this way. An underscore is a word character,
        // so a \b-anchored pattern would sail straight past EQSL_PSWD.
        SecretScrubber.Redact(text).Should().NotContain(secretValue);
    }

    [Fact]
    public void DoesNotMangleOrdinaryWordsThatMerelyEndInS()
    {
        // A bare "s=" is QRZ's session key, but "items=" must not trip it.
        SecretScrubber.Redact("items=42&totals=7").Should().Be("items=42&totals=7");
    }

    [Fact]
    public void IgnoresSecretsTooShortToMaskSafely()
    {
        // Masking a 2-character "secret" would shred every line it appears in,
        // and nothing that short is worth protecting at that price.
        SecretScrubber.Redact("uploading W1AW on 20m", "1A").Should().Be("uploading W1AW on 20m");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public void PassesNullAndEmptyThroughSoCallersCanWrapUnconditionally(string? input)
    {
        SecretScrubber.Redact(input).Should().Be(input);
    }
}
