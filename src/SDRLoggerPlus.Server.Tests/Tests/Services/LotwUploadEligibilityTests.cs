using FluentAssertions;
using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Services;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

/// <summary>
/// UPLOAD-SAFETY GUARD (GitHub #56). Removing the 100k cap on the LoTW upload selection lets the
/// filter/whole-log path see EVERY QSO. That is only safe because the eligibility predicate still
/// excludes already-sent ("Y") QSOs — otherwise a whole-log upload would re-send contacts and double
/// a user's online LoTW logbook. These tests pin that predicate so the cap removal can't regress it.
/// (A deliberate re-send is a separate path: explicit QsoIds, which bypasses this predicate by design.)
/// </summary>
[Trait("Category", "Unit")]
public class LotwUploadEligibilityTests
{
    private static Qso WithSent(string? sent) => new()
    {
        Callsign = "K1ABC",
        Qsl = sent is null ? new QslStatus() : new QslStatus { Lotw = new LotwStatus { Sent = sent } },
    };

    [Theory]
    [InlineData("Y")]   // already sent — the one that must never re-send from a filter/whole-log selection
    [InlineData("y")]   // case/whitespace-insensitive
    [InlineData(" Y ")]
    public void AlreadySent_isNeverEligible_evenWithOptInFlags(string sent)
    {
        var permissive = new LotwUploadFilter(IncludeIgnored: true, IncludeNotSent: true);
        LotwService.IsEligibleForUpload(WithSent(sent), permissive).Should().BeFalse();
        LotwService.IsEligibleForUpload(WithSent(sent), new LotwUploadFilter()).Should().BeFalse();
    }

    [Theory]
    [InlineData(null)]  // never attempted
    [InlineData("")]
    [InlineData("R")]   // requested
    [InlineData("Q")]   // queued
    public void NotYetSent_isEligible(string? sent) =>
        LotwService.IsEligibleForUpload(WithSent(sent), new LotwUploadFilter()).Should().BeTrue();

    [Fact]
    public void Ignored_and_NotSent_are_opt_in_only()
    {
        LotwService.IsEligibleForUpload(WithSent("I"), new LotwUploadFilter()).Should().BeFalse();
        LotwService.IsEligibleForUpload(WithSent("I"), new LotwUploadFilter(IncludeIgnored: true)).Should().BeTrue();

        LotwService.IsEligibleForUpload(WithSent("N"), new LotwUploadFilter()).Should().BeFalse();
        LotwService.IsEligibleForUpload(WithSent("N"), new LotwUploadFilter(IncludeNotSent: true)).Should().BeTrue();
    }
}
