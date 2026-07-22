using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using SDRLoggerPlus.Server.Core.Security;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Security;

[Trait("Category", "Unit")]
public class SecretEnvelopeTests
{
    [Theory]
    [InlineData("slp$1$dpapi$AQAAAA==")]
    [InlineData("slp$1$aes$AQAAAA==")]
    public void RecognisesItsOwnFormat(string value)
    {
        SecretEnvelope.IsProtected(value).Should().BeTrue();
    }

    [Theory]
    [InlineData("hunter2")]
    [InlineData("")]
    [InlineData(null)]
    [InlineData("AQAAAA==")]              // plausible-looking base64 password
    [InlineData("slp$2$dpapi$AQAAAA==")]  // a future version, not ours
    public void DoesNotClaimForeignValues(string? value)
    {
        SecretEnvelope.IsProtected(value).Should().BeFalse();
    }

    [Fact]
    public void UnwrapsSchemeAndPayload()
    {
        SecretEnvelope.TryUnwrap("slp$1$aes$Zm9v", out var scheme, out var payload)
            .Should().BeTrue();
        scheme.Should().Be("aes");
        payload.Should().Be("Zm9v");
    }

    [Theory]
    [InlineData("slp$1$")]          // no scheme, no payload
    [InlineData("slp$1$aes")]       // no separator
    [InlineData("slp$1$aes$")]      // empty payload
    [InlineData("slp$1$$Zm9v")]     // empty scheme
    [InlineData("plaintext")]
    public void RejectsMalformedEnvelopes(string value)
    {
        SecretEnvelope.TryUnwrap(value, out _, out _).Should().BeFalse();
    }

    [Fact]
    public void PayloadContainingSeparatorSurvivesRoundTrip()
    {
        // base64 never contains '$', but the unwrap must split on the FIRST
        // separator regardless so a scheme name is never mis-parsed.
        SecretEnvelope.TryUnwrap(SecretEnvelope.Wrap("aes", "ab$cd"), out var scheme, out var payload)
            .Should().BeTrue();
        scheme.Should().Be("aes");
        payload.Should().Be("ab$cd");
    }
}

[Trait("Category", "Unit")]
public class SecretProtectorTests : IDisposable
{
    private readonly string _dir;

    public SecretProtectorTests()
    {
        _dir = Path.Combine(Path.GetTempPath(), $"slp_sec_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_dir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { }
    }

    private SecretProtector Aes(string? dir = null) =>
        new(dir ?? _dir, NullLogger<SecretProtector>.Instance, useDpapi: false);

    private SecretProtector Platform() =>
        new(_dir, NullLogger<SecretProtector>.Instance);

    // --- AES fallback (exercised on every OS, not just Linux CI) ------------

    [Theory]
    [InlineData("hunter2")]
    [InlineData("a")]
    [InlineData("pässwörd with spaces and ünicode ✓")]
    [InlineData("0123456789012345678901234567890123456789012345678901234567890123456789")]
    public void AesRoundTripsAnyValue(string secret)
    {
        var protector = Aes();

        var stored = protector.Protect(secret);

        stored.Should().NotBe(secret);
        stored.Should().StartWith("slp$1$aes$");
        protector.Unprotect(stored).Should().Be(secret);
    }

    [Fact]
    public void CiphertextDoesNotContainThePlaintext()
    {
        // Checked with a distinctive value: a one-character secret would match
        // base64 output by chance and prove nothing.
        var stored = Aes().Protect("SuperSecretPassphrase");

        stored.Should().NotContain("SuperSecretPassphrase");
        stored.Should().NotContain("Secret");
    }

    [Fact]
    public void PlatformProtectorRoundTrips()
    {
        var protector = Platform();
        var stored = protector.Protect("hunter2");

        stored.Should().NotBe("hunter2");
        protector.Unprotect(stored).Should().Be("hunter2");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public void EmptyValuesPassStraightThrough(string? value)
    {
        // An unconfigured credential must stay visibly empty, not become an
        // opaque blob that looks configured.
        var protector = Aes();
        protector.Protect(value).Should().Be(value);
        protector.Unprotect(value).Should().Be(value);
    }

    [Fact]
    public void NeverDoubleWraps()
    {
        var protector = Aes();
        var once = protector.Protect("hunter2");

        var twice = protector.Protect(once);

        twice.Should().Be(once);
        protector.Unprotect(twice).Should().Be("hunter2");
    }

    [Fact]
    public void LegacyPlaintextReadsBackUnchanged()
    {
        // This is what makes an un-migrated database keep working.
        Aes().Unprotect("hunter2").Should().Be("hunter2");
    }

    [Fact]
    public void SameValueEncryptsDifferentlyEachTime()
    {
        // A fresh nonce per call: two accounts sharing a password must not be
        // detectable by comparing stored values.
        var protector = Aes();

        protector.Protect("hunter2").Should().NotBe(protector.Protect("hunter2"));
    }

    [Fact]
    public void ValueFromAnotherInstallReadsBackBlankRatherThanThrowing()
    {
        // The restored-backup case: ciphertext arrives with a key that cannot
        // decrypt it. The app must start and prompt for re-entry.
        var theirs = Aes().Protect("hunter2");

        var otherInstall = Path.Combine(_dir, "other");
        Directory.CreateDirectory(otherInstall);

        Aes(otherInstall).Unprotect(theirs).Should().BeNull();
    }

    [Fact]
    public void TamperedValueIsRejectedRatherThanReturningGarbage()
    {
        var protector = Aes();
        var stored = protector.Protect("hunter2")!;

        // Flip a byte in the payload. GCM authentication must catch it.
        var payload = Convert.FromBase64String(stored.Split('$')[3]);
        payload[^1] ^= 0xFF;
        var tampered = SecretEnvelope.Wrap("aes", Convert.ToBase64String(payload));

        protector.Unprotect(tampered).Should().BeNull();
    }

    [Fact]
    public void UnknownSchemeReadsBackBlank()
    {
        Aes().Unprotect(SecretEnvelope.Wrap("keychain", "Zm9v")).Should().BeNull();
    }

    [Fact]
    public void ReusesTheKeyFileAcrossInstances()
    {
        var stored = Aes().Protect("hunter2");

        // A second protector over the same directory — i.e. the next app run.
        Aes().Unprotect(stored).Should().Be("hunter2");

        Directory.GetFiles(_dir, ".secret.key").Should().ContainSingle();
    }

    [Fact]
    public void RefusesToRunWithAMalformedKeyFile()
    {
        // Silently regenerating the key would make every stored credential
        // permanently unreadable, which is worse than a loud failure.
        File.WriteAllText(Path.Combine(_dir, ".secret.key"), "not-a-valid-key");

        var act = () => Aes().Protect("hunter2");

        act.Should().Throw<Exception>();
    }
}
