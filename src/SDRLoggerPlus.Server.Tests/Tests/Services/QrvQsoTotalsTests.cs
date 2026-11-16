using System.Net;
using System.Text.Json;
using FluentAssertions;
using Microsoft.AspNetCore.SignalR;
using MongoDB.Bson;
using Moq;
using SDRLoggerPlus.Contracts.Api;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database;
using SDRLoggerPlus.Server.Hubs;
using SDRLoggerPlus.Server.Services;
using SDRLoggerPlus.Server.Services.Qrv;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

[Trait("Category", "Unit")]
public class QrvQsoTotalsTests
{
    private const string InstallKey = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    private static readonly DateTime Now = new(2026, 11, 16, 12, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void Week_starts_monday_0000_utc()
    {
        QrvQsoTotals.WeekStartUtc(Now).Should().Be(new DateTime(2026, 11, 16, 0, 0, 0, DateTimeKind.Utc));
        QrvQsoTotals.WeekStartUtc(new DateTime(2026, 1, 1, 0, 30, 0, DateTimeKind.Utc))
            .Should().Be(new DateTime(2025, 12, 29, 0, 0, 0, DateTimeKind.Utc));
    }

    [Fact]
    public void Count_uses_utc_week_month_and_year_and_mode_buckets()
    {
        var qsos = new[]
        {
            Qso(Now.Date, "USB", "Lyra"),
            Qso(Now.Date, "CW", "Lyra"),
            Qso(Now.Date, "FT8", "STD"),
            Qso(Now.Date, "CW-R", "Lyra"),
            Qso(Now.Date, "SSB", "Lyra"),
            Qso(Now.Date, "", "STD"),
            Qso(new DateTime(2026, 11, 15, 23, 59, 0, DateTimeKind.Utc), "LSB", "Lyra"),
            Qso(new DateTime(2026, 11, 1, 0, 0, 0, DateTimeKind.Utc), "AM", "Lyra"),
            Qso(new DateTime(2026, 10, 31, 23, 0, 0, DateTimeKind.Utc), "CWL", "STD"),
            Qso(new DateTime(2025, 12, 31, 23, 0, 0, DateTimeKind.Utc), "USB", "Lyra"),
            Qso(Now.AddSeconds(1), "USB", "Lyra"),
            Qso(new DateTime(2026, 11, 16, 0, 0, 0, DateTimeKind.Unspecified), "FM", "Lyra"),
        };

        var totals = QrvQsoTotals.Count(qsos, Now);

        totals.Week.Should().Be(new QrvQsoTotals.ModeCounts(Voice: 3, Cw: 2, Digital: 2));
        totals.Month.Should().Be(new QrvQsoTotals.ModeCounts(Voice: 5, Cw: 2, Digital: 2));
        totals.Year.Should().Be(new QrvQsoTotals.ModeCounts(Voice: 5, Cw: 3, Digital: 2));
    }

    [Fact]
    public void January_iso_week_includes_the_previous_december_only_in_the_week()
    {
        var now = new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);
        var qsos = new[]
        {
            Qso(new DateTime(2025, 12, 29, 0, 0, 0, DateTimeKind.Utc), "USB", "Lyra"),
            Qso(new DateTime(2025, 12, 28, 23, 59, 0, DateTimeKind.Utc), "USB", "Lyra"),
            Qso(new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc), "CW", "STD"),
        };

        var totals = QrvQsoTotals.Count(qsos, now);

        totals.Week.Should().Be(new QrvQsoTotals.ModeCounts(Voice: 1, Cw: 1, Digital: 0));
        totals.Month.Should().Be(new QrvQsoTotals.ModeCounts(Voice: 0, Cw: 1, Digital: 0));
        totals.Year.Should().Be(new QrvQsoTotals.ModeCounts(Voice: 0, Cw: 1, Digital: 0));
    }

    [Fact]
    public void Pota_counts_and_contest_qsos_do_not()
    {
        var pota = Qso(Now, "USB", "Lyra");
        pota.AdifExtra = new BsonDocument { ["pota_ref"] = "K-1234", ["my_pota_ref"] = "K-0001" };

        var byId = Qso(Now, "CW", "Lyra");
        byId.Contest = new ContestInfo { ContestId = "CQ-WW-CW" };

        var bySession = Qso(Now, "FT8", "STD");
        bySession.SessionOnly();

        var totals = QrvQsoTotals.Count(new[] { pota, byId, bySession }, Now);

        totals.Week.Should().Be(new QrvQsoTotals.ModeCounts(Voice: 1, Cw: 0, Digital: 0));
        totals.Year.Total.Should().Be(1);
    }

    [Fact]
    public void Every_non_contest_contact_counts_and_a_stamp_is_not_required()
    {
        var unstamped = Qso(Now, "USB", null);
        var blank = Qso(Now, "CW", "");
        var otherRadio = Qso(Now, "FT8", "Thetis");
        var logger = Qso(Now, "FT8", "Logger");
        var lyra = Qso(Now, "USB", "Lyra");
        var std = Qso(Now, "CW", "STD");
        var pota = Qso(Now, "FT8", null);
        pota.AdifExtra = new BsonDocument { ["pota_ref"] = "K-1234" };
        var contest = Qso(Now, "FT8", null);
        contest.Contest = new ContestInfo { ContestId = "CQ-WW-SSB" };
        var contestStamped = Qso(Now, "USB", "Logger");
        contestStamped.SessionOnly();

        QrvQsoTotals.IsCountedContact(unstamped).Should().BeTrue();
        QrvQsoTotals.IsCountedContact(blank).Should().BeTrue();
        QrvQsoTotals.IsCountedContact(otherRadio).Should().BeTrue();
        QrvQsoTotals.IsCountedContact(logger).Should().BeTrue();
        QrvQsoTotals.IsCountedContact(pota).Should().BeTrue();
        QrvQsoTotals.IsCountedContact(contest).Should().BeFalse();
        QrvQsoTotals.IsCountedContact(contestStamped).Should().BeFalse();

        var totals = QrvQsoTotals.Count(
            new[] { unstamped, blank, otherRadio, logger, lyra, std, pota, contest, contestStamped }, Now);

        totals.Week.Should().Be(new QrvQsoTotals.ModeCounts(Voice: 2, Cw: 2, Digital: 3));
        totals.Month.Should().Be(totals.Week);
        totals.Year.Should().Be(totals.Week);
    }

    [Fact]
    public void Contacts_already_in_2026_count_on_month_and_year_and_the_week_starts_monday()
    {
        var qsos = new[]
        {
            Qso(new DateTime(2026, 1, 15, 12, 0, 0, DateTimeKind.Utc), "FT8", null),
            Qso(new DateTime(2026, 11, 1, 0, 0, 0, DateTimeKind.Utc), "USB", ""),
            Qso(new DateTime(2026, 11, 15, 23, 59, 0, DateTimeKind.Utc), "CW", null),
            Qso(new DateTime(2026, 11, 16, 0, 0, 0, DateTimeKind.Utc), "FT8", null),
            Qso(new DateTime(2025, 12, 31, 23, 0, 0, DateTimeKind.Utc), "USB", null),
        };

        var totals = QrvQsoTotals.Count(qsos, Now);

        totals.Week.Should().Be(new QrvQsoTotals.ModeCounts(Voice: 0, Cw: 0, Digital: 1));
        totals.Month.Should().Be(new QrvQsoTotals.ModeCounts(Voice: 1, Cw: 1, Digital: 1));
        totals.Year.Should().Be(new QrvQsoTotals.ModeCounts(Voice: 1, Cw: 1, Digital: 2));
    }

    [Fact]
    public async Task A_contact_from_another_radio_counts_lyra_and_std_still_count_and_contest_does_not()
    {
        var tracker = new TciRadioSourceTracker();
        tracker.Connected("rig-1");
        tracker.NoteProtocol("rig-1", "Thetis");
        var presence = new QrvPresenceClient(new HttpClient(new FakeHandler()));
        var signal = new QrvQsoTotalsSignal();
        var (service, captured) = Service(tracker, presence, signal);
        var saved = new List<Qso>();

        await service.CreateAsync(Request("FT8"));
        captured().RadioSource.Should().Be("Logger");
        signal.Consume().Should().BeTrue();
        saved.Add(Copy(captured()));

        tracker.NoteProtocol("rig-1", "Lyra");
        await service.CreateAsync(Request("USB"));
        captured().RadioSource.Should().Be("Lyra");
        signal.Consume().Should().BeTrue();
        saved.Add(Copy(captured()));

        tracker.NoteStd("rig-1");
        using (QsoLoggingOrigin.Use("STD"))
            await service.CreateAsync(Request("CW"));
        captured().RadioSource.Should().Be("STD");
        signal.Consume().Should().BeTrue();
        saved.Add(Copy(captured()));

        await service.CreateAsync(Request("USB", contest: "CQ-WW-SSB"));
        captured().RadioSource.Should().Be("Lyra");
        captured().Contest!.ContestId.Should().Be("CQ-WW-SSB");
        signal.Consume().Should().BeFalse();
        saved.Add(Copy(captured()));
        saved.Add(Qso(Now, "USB", null));

        QrvQsoTotals.Count(saved, Now).Week.Should().Be(new QrvQsoTotals.ModeCounts(Voice: 2, Cw: 1, Digital: 1));
    }

    [Fact]
    public void Counts_clamp_at_100000()
    {
        var qsos = Enumerable.Range(0, QrvQsoTotals.MaxCount + 1)
            .Select(_ => Qso(Now, "DIGU", "Lyra"));

        QrvQsoTotals.Count(qsos, Now).Week.Digital.Should().Be(QrvQsoTotals.MaxCount);
    }

    [Theory]
    [InlineData("LSB", "voice")]
    [InlineData("usb", "voice")]
    [InlineData("AM", "voice")]
    [InlineData("SAM", "voice")]
    [InlineData("FM", "voice")]
    [InlineData("NFM", "voice")]
    [InlineData("DSB", "voice")]
    [InlineData("SSB", "voice")]
    [InlineData("CW", "cw")]
    [InlineData("CWU", "cw")]
    [InlineData("CWL", "cw")]
    [InlineData("CW-R", "cw")]
    [InlineData("FT8", "digital")]
    [InlineData("RTTY-R", "digital")]
    [InlineData("", "digital")]
    [InlineData(null, "digital")]
    public void Mode_buckets_match_the_spec(string? mode, string bucket)
    {
        QrvQsoTotals.ModeBucket(mode).Should().Be(bucket);
    }

    [Fact]
    public void Log_macro_is_not_a_source()
    {
        QrvQsoTotals.IsStdIdentity("lyra_log", new[] { "N8SDR", "599" }).Should().BeFalse();
        QrvQsoTotals.Classify(new QrvQsoTotals.TciSession("ExpertSDR3", ComboLinked: false, StdIdentified: false), null)
            .Should().Be("Logger");
        QrvQsoTotals.Classify(new QrvQsoTotals.TciSession("ExpertSDR3", ComboLinked: true, StdIdentified: false), null)
            .Should().Be("Lyra");
    }

    [Fact]
    public void Source_follows_the_tci_hello_and_std_identity()
    {
        QrvQsoTotals.Classify(new QrvQsoTotals.TciSession("Lyra", false, false), null).Should().Be("Lyra");
        QrvQsoTotals.Classify(new QrvQsoTotals.TciSession("lyra", false, false), "WSJT-X").Should().Be("Lyra");
        QrvQsoTotals.Classify(new QrvQsoTotals.TciSession("ExpertSDR3", false, false), null).Should().Be("Logger");
        QrvQsoTotals.Classify(new QrvQsoTotals.TciSession("Thetis", false, false), null).Should().Be("Logger");
        QrvQsoTotals.Classify(new QrvQsoTotals.TciSession("ExpertSDR3", true, false), null).Should().Be("Lyra");
        QrvQsoTotals.Classify(new QrvQsoTotals.TciSession("Lyra", false, false), "STD").Should().Be("Logger");
        QrvQsoTotals.Classify(new QrvQsoTotals.TciSession("ExpertSDR3", true, true), "STD").Should().Be("STD");
        QrvQsoTotals.Classify(new QrvQsoTotals.TciSession("STD", false, false), null).Should().Be("STD");
        QrvQsoTotals.Classify(QrvQsoTotals.TciSession.None, null).Should().Be("Logger");

        QrvQsoTotals.IsStdIdentity("protocol", new[] { "Lyra", "2.0" }).Should().BeFalse();
        QrvQsoTotals.IsStdIdentity("protocol", new[] { "ExpertSDR3", "1.9" }).Should().BeFalse();
        QrvQsoTotals.IsStdIdentity("protocol", new[] { "STD", "1.0" }).Should().BeTrue();
        QrvQsoTotals.IsStdIdentity("std", Array.Empty<string>()).Should().BeTrue();
        QrvQsoTotals.IsStdIdentity("app", new[] { "STD" }).Should().BeTrue();
        QrvQsoTotals.IsStdIdentity("client", new[] { "std" }).Should().BeTrue();
        QrvQsoTotals.IsStdIdentity("app", new[] { "Logger" }).Should().BeFalse();
    }

    [Fact]
    public void Encode_is_a_replacement_snapshot_without_the_log_or_frequency()
    {
        var json = QrvQsoTotals.Encode("N8SDR", InstallKey, new QrvQsoTotals.Snapshot(
            new QrvQsoTotals.ModeCounts(0, 1, 12),
            new QrvQsoTotals.ModeCounts(2, 4, 40),
            new QrvQsoTotals.ModeCounts(10, 20, 100)));

        using var doc = JsonDocument.Parse(json);
        var names = doc.RootElement.EnumerateObject().Select(p => p.Name).ToArray();
        names.Should().Equal("call", "app", "install_key", "week", "month", "year");
        doc.RootElement.GetProperty("call").GetString().Should().Be("N8SDR");
        doc.RootElement.GetProperty("app").GetString().Should().Be("Logger");
        doc.RootElement.GetProperty("install_key").GetString().Should().Be(InstallKey);
        Bucket(doc.RootElement.GetProperty("week")).Should().Be((0, 1, 12));
        Bucket(doc.RootElement.GetProperty("month")).Should().Be((2, 4, 40));
        Bucket(doc.RootElement.GetProperty("year")).Should().Be((10, 20, 100));
        json.Should().NotContain("freq");
        json.Should().NotContain("adif");
        json.Should().NotContain("hz");
    }

    [Fact]
    public async Task Disabled_sends_nothing()
    {
        var (client, handler, _) = CreateClient();
        client.SetCounts(SampleSnapshot());
        client.ApplySettings(false, "https://qrvsdr.cc/v1", "N8SDR", InstallKey);
        await client.TickAsync(CancellationToken.None);
        await client.TickAsync(CancellationToken.None);
        await client.ShutdownAsync(CancellationToken.None);

        handler.Requests.Should().BeEmpty();
        client.Status().Phase.Should().Be("off");
        client.Status().Detail.Should().Contain("Nothing is sent");
    }

    [Fact]
    public async Task Enabled_without_an_install_key_sends_nothing()
    {
        var (client, handler, _) = CreateClient();
        client.ApplySettings(true, "https://qrvsdr.cc/v1", "N8SDR", null);
        client.SetCounts(SampleSnapshot());
        await client.TickAsync(CancellationToken.None);

        handler.Requests.Should().BeEmpty();
        client.Status().Phase.Should().Be("needs-key");
    }

    [Fact]
    public async Task Enabled_posts_the_snapshot_then_respects_the_ten_second_floor_and_the_few_minute_refresh()
    {
        var (client, handler, advance) = CreateClient();
        client.ApplySettings(true, "https://qrvsdr.cc/v1", "n8sdr", InstallKey);
        client.SetCounts(SampleSnapshot());
        await client.TickAsync(CancellationToken.None);

        handler.Requests.Should().ContainSingle();
        handler.Requests[0].Url.Should().Be("https://qrvsdr.cc/v1/qso.php");
        using (var doc = JsonDocument.Parse(handler.Requests[0].Body))
        {
            doc.RootElement.GetProperty("app").GetString().Should().Be("Logger");
            doc.RootElement.GetProperty("call").GetString().Should().Be("N8SDR");
            Bucket(doc.RootElement.GetProperty("week")).Should().Be((0, 1, 12));
        }

        await client.TickAsync(CancellationToken.None);
        advance(TimeSpan.FromSeconds(9));
        await client.TickAsync(CancellationToken.None);
        handler.Requests.Should().ContainSingle();

        advance(TimeSpan.FromSeconds(1));
        client.SetCounts(new QrvQsoTotals.Snapshot(
            new QrvQsoTotals.ModeCounts(1, 1, 12),
            new QrvQsoTotals.ModeCounts(2, 4, 40),
            new QrvQsoTotals.ModeCounts(10, 20, 100)));
        await client.TickAsync(CancellationToken.None);
        handler.Requests.Should().HaveCount(2);

        advance(TimeSpan.FromMinutes(3));
        await client.TickAsync(CancellationToken.None);
        handler.Requests.Should().HaveCount(3);
    }

    [Fact]
    public async Task Turning_totals_off_posts_one_snapshot_of_zeros()
    {
        var (client, handler, advance) = CreateClient();
        await GoOn(client);
        client.ApplySettings(false, "https://qrvsdr.cc/v1", "N8SDR", InstallKey);
        await client.TickAsync(CancellationToken.None);
        advance(TimeSpan.FromSeconds(10));
        await client.TickAsync(CancellationToken.None);
        await client.TickAsync(CancellationToken.None);

        var zero = handler.Requests.Last();
        zero.Url.Should().EndWith("/v1/qso.php");
        using var doc = JsonDocument.Parse(zero.Body);
        Bucket(doc.RootElement.GetProperty("week")).Should().Be((0, 0, 0));
        Bucket(doc.RootElement.GetProperty("month")).Should().Be((0, 0, 0));
        Bucket(doc.RootElement.GetProperty("year")).Should().Be((0, 0, 0));
        handler.Requests.Should().HaveCount(2);
    }

    [Fact]
    public async Task Rate_limit_and_store_failure_wait_instead_of_looping()
    {
        var (client, handler, advance) = CreateClient(HttpStatusCode.TooManyRequests, """{"ok":false,"error":"rate_limited"}""");
        client.ApplySettings(true, "https://qrvsdr.cc/v1", "N8SDR", InstallKey);
        client.SetCounts(SampleSnapshot());
        await client.TickAsync(CancellationToken.None);
        advance(TimeSpan.FromSeconds(1));
        await client.TickAsync(CancellationToken.None);
        handler.Requests.Should().ContainSingle();

        advance(TimeSpan.FromSeconds(10));
        await client.TickAsync(CancellationToken.None);
        handler.Requests.Should().HaveCount(2);
    }

    [Fact]
    public async Task Store_failed_retries_the_same_snapshot_later()
    {
        var (client, handler, advance) = CreateClient(HttpStatusCode.InternalServerError, """{"ok":false,"error":"store_failed"}""");
        client.ApplySettings(true, "https://qrvsdr.cc/v1", "N8SDR", InstallKey);
        client.SetCounts(SampleSnapshot());
        await client.TickAsync(CancellationToken.None);
        var first = handler.Requests[0].Body;

        advance(TimeSpan.FromSeconds(10));
        await client.TickAsync(CancellationToken.None);
        handler.Requests.Should().ContainSingle();

        advance(TimeSpan.FromSeconds(20));
        await client.TickAsync(CancellationToken.None);
        handler.Requests.Should().HaveCount(2);
        handler.Requests[1].Body.Should().Be(first);
    }

    [Fact]
    public async Task Invalid_fields_is_not_retried_unchanged()
    {
        var (client, handler, advance) = CreateClient(HttpStatusCode.BadRequest, """{"ok":false,"error":"invalid_fields"}""");
        client.ApplySettings(true, "https://qrvsdr.cc/v1", "N8SDR", InstallKey);
        client.SetCounts(SampleSnapshot());
        await client.TickAsync(CancellationToken.None);

        advance(TimeSpan.FromMinutes(3));
        await client.TickAsync(CancellationToken.None);
        handler.Requests.Should().ContainSingle();

        advance(TimeSpan.FromSeconds(10));
        client.SetCounts(new QrvQsoTotals.Snapshot(
            new QrvQsoTotals.ModeCounts(1, 0, 0),
            QrvQsoTotals.ModeCounts.Zero,
            QrvQsoTotals.ModeCounts.Zero));
        await client.TickAsync(CancellationToken.None);
        handler.Requests.Should().HaveCount(2);
    }

    [Fact]
    public async Task Bad_key_does_not_invent_a_replacement()
    {
        var (client, handler, advance) = CreateClient(HttpStatusCode.Unauthorized, """{"ok":false,"error":"bad_key"}""");
        client.ApplySettings(true, "https://qrvsdr.cc/v1", "N8SDR", InstallKey);
        client.SetCounts(SampleSnapshot());
        await client.TickAsync(CancellationToken.None);

        advance(TimeSpan.FromMinutes(5));
        await client.TickAsync(CancellationToken.None);
        handler.Requests.Should().ContainSingle();
        handler.Requests[0].Body.Should().Contain(InstallKey);
        client.Status().Detail.Should().Contain("not invented");

        var other = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
        client.ApplySettings(true, "https://qrvsdr.cc/v1", "N8SDR", other);
        advance(TimeSpan.FromSeconds(10));
        await client.TickAsync(CancellationToken.None);
        handler.Requests.Should().HaveCount(2);
        handler.Requests[1].Body.Should().Contain(other);
    }

    [Fact]
    public async Task Forbidden_stops()
    {
        var (client, handler, advance) = CreateClient(HttpStatusCode.Forbidden, """{"ok":false,"error":"forbidden"}""");
        client.ApplySettings(true, "https://qrvsdr.cc/v1", "N8SDR", InstallKey);
        client.SetCounts(SampleSnapshot());
        await client.TickAsync(CancellationToken.None);

        advance(TimeSpan.FromMinutes(5));
        await client.TickAsync(CancellationToken.None);
        client.ApplySettings(false, "https://qrvsdr.cc/v1", "N8SDR", InstallKey);
        await client.TickAsync(CancellationToken.None);
        client.ApplySettings(true, "https://qrvsdr.cc/v1", "N8SDR", InstallKey);
        advance(TimeSpan.FromSeconds(10));
        await client.TickAsync(CancellationToken.None);

        handler.Requests.Should().ContainSingle();
        client.Status().Phase.Should().Be("stopped");
    }

    [Fact]
    public async Task Call_mismatch_uses_the_presence_call_and_does_not_retry_unchanged()
    {
        var (client, handler, advance) = CreateClient(HttpStatusCode.Forbidden, """{"ok":false,"error":"call_mismatch"}""");
        client.ApplySettings(true, "https://qrvsdr.cc/v1", "N8SDR", InstallKey);
        client.SetCounts(SampleSnapshot());
        await client.TickAsync(CancellationToken.None);

        advance(TimeSpan.FromMinutes(3));
        await client.TickAsync(CancellationToken.None);
        handler.Requests.Should().ContainSingle();

        client.ApplySettings(true, "https://qrvsdr.cc/v1", "W1AW", InstallKey);
        advance(TimeSpan.FromSeconds(10));
        await client.TickAsync(CancellationToken.None);
        handler.Requests.Should().HaveCount(2);
        using var doc = JsonDocument.Parse(handler.Requests[1].Body);
        doc.RootElement.GetProperty("call").GetString().Should().Be("W1AW");
    }

    [Fact]
    public async Task Save_stamps_the_live_source_and_does_not_backfill_an_edit()
    {
        var tracker = new TciRadioSourceTracker();
        tracker.Connected("lyra-1");
        tracker.NoteProtocol("lyra-1", "Lyra");
        var presence = new QrvPresenceClient(new HttpClient(new FakeHandler()));
        var signal = new QrvQsoTotalsSignal();
        var (service, captured) = Service(tracker, presence, signal);

        await service.CreateAsync(Request("USB"));
        captured().RadioSource.Should().Be("Lyra");
        signal.Consume().Should().BeTrue();

        tracker.NoteProtocol("lyra-1", "ExpertSDR3");
        tracker.NoteCombo("lyra-1", false);
        await service.CreateAsync(Request("USB"));
        captured().RadioSource.Should().Be("Logger");
        signal.Consume().Should().BeTrue();
        QrvQsoTotals.Count(new[] { captured() }, Now).Week.Voice.Should().Be(1);

        tracker.NoteCombo("lyra-1", true);
        await service.CreateAsync(Request("CW", contest: "CQ-WW-CW"));
        captured().RadioSource.Should().Be("Lyra");
        captured().Contest!.ContestId.Should().Be("CQ-WW-CW");
        signal.Consume().Should().BeFalse();
        QrvQsoTotals.Count(new[] { captured() }, Now).Week.Total.Should().Be(0);

        tracker.NoteStd("lyra-1");
        using (QsoLoggingOrigin.Use("STD"))
            await service.CreateAsync(Request("FT8"));
        captured().RadioSource.Should().Be("STD");

        tracker.Disconnected("lyra-1");
        await service.CreateAsync(Request("USB"));
        captured().RadioSource.Should().Be("Logger");
        signal.Consume().Should().BeTrue();

        var updated = await UpdateWithoutSource(service, tracker);
        updated.RadioSource.Should().BeNull();
    }

    [Fact]
    public void Two_different_live_sessions_are_not_guessed()
    {
        var tracker = new TciRadioSourceTracker();
        tracker.Connected("a");
        tracker.NoteProtocol("a", "Lyra");
        tracker.Connected("b");
        tracker.NoteProtocol("b", "ExpertSDR3");

        tracker.View(null).Protocol.Should().BeNull();
        tracker.View("a").Protocol.Should().Be("Lyra");
    }

    private static (QrvQsoTotalsClient Client, FakeHandler Handler, Action<TimeSpan> Advance) CreateClient(
        HttpStatusCode status = HttpStatusCode.OK,
        string? body = null)
    {
        body ??= """{"ok":true,"periods":{"week":"2026-W47","month":"2026-11","year":"2026"}}""";
        var now = new DateTimeOffset(2026, 11, 16, 12, 0, 0, TimeSpan.Zero);
        var handler = new FakeHandler();
        handler.Responder = (_, _) => new HttpResponseMessage(status) { Content = new StringContent(body) };
        var client = new QrvQsoTotalsClient(new HttpClient(handler), clock: () => now);
        return (client, handler, by => now = now.Add(by));
    }

    private static async Task GoOn(QrvQsoTotalsClient client)
    {
        client.ApplySettings(true, "https://qrvsdr.cc/v1", "N8SDR", InstallKey);
        client.SetCounts(SampleSnapshot());
        await client.TickAsync(CancellationToken.None);
    }

    private static QrvQsoTotals.Snapshot SampleSnapshot() => new(
        new QrvQsoTotals.ModeCounts(0, 1, 12),
        new QrvQsoTotals.ModeCounts(2, 4, 40),
        new QrvQsoTotals.ModeCounts(10, 20, 100));

    private static (int Voice, int Cw, int Digital) Bucket(JsonElement counts) =>
        (counts.GetProperty("voice").GetInt32(), counts.GetProperty("cw").GetInt32(), counts.GetProperty("digital").GetInt32());

    private static Qso Copy(Qso qso) => new()
    {
        Id = qso.Id,
        Callsign = qso.Callsign,
        QsoDate = qso.QsoDate,
        TimeOn = qso.TimeOn,
        Band = qso.Band,
        Mode = qso.Mode,
        RadioSource = qso.RadioSource,
        Contest = qso.Contest == null ? null : new ContestInfo { ContestId = qso.Contest.ContestId, SessionId = qso.Contest.SessionId },
    };

    private static Qso Qso(DateTime when, string mode, string? source) => new()
    {
        Id = Guid.NewGuid().ToString("N"),
        Callsign = "W1AW",
        QsoDate = when,
        TimeOn = "120000",
        Band = "20m",
        Mode = mode,
        RadioSource = source,
    };

    private static CreateQsoRequest Request(string mode, string? contest = null) =>
        new("W1AW", Now, "120000", "20m", mode, Contest: contest);

    private static (QsoService Service, Func<Qso> Captured) Service(
        TciRadioSourceTracker tracker, QrvPresenceClient presence, QrvQsoTotalsSignal signal)
    {
        Qso? saved = null;
        var repo = new Mock<IQsoRepository>();
        repo.Setup(r => r.CreateAsync(It.IsAny<Qso>()))
            .Callback<Qso>(q => saved = q)
            .ReturnsAsync((Qso q) => { q.Id ??= "id"; return q; });
        var hub = new Mock<IHubContext<LogHub, ILogHubClient>>();
        var clients = new Mock<IHubClients<ILogHubClient>>();
        clients.Setup(c => c.All).Returns(new Mock<ILogHubClient>().Object);
        hub.Setup(h => h.Clients).Returns(clients.Object);
        var service = new QsoService(repo.Object, hub.Object, radioSources: tracker, qrvPresence: presence, qrvTotals: signal);
        return (service, () => saved!);
    }

    private static async Task<Qso> UpdateWithoutSource(QsoService service, TciRadioSourceTracker tracker)
    {
        tracker.Connected("lyra-1");
        tracker.NoteProtocol("lyra-1", "Lyra");
        var existing = new Qso
        {
            Id = "old",
            Callsign = "W1AW",
            QsoDate = Now,
            TimeOn = "120000",
            Band = "20m",
            Mode = "USB",
            RadioSource = null,
        };
        Qso? updated = null;
        var repo = new Mock<IQsoRepository>();
        repo.Setup(r => r.GetByIdAsync("old")).ReturnsAsync(existing);
        repo.Setup(r => r.UpdateAsync("old", It.IsAny<Qso>(), It.IsAny<bool>()))
            .Callback<string, Qso, bool>((_, q, _) => updated = q)
            .ReturnsAsync(true);
        var hub = new Mock<IHubContext<LogHub, ILogHubClient>>();
        var clients = new Mock<IHubClients<ILogHubClient>>();
        clients.Setup(c => c.All).Returns(new Mock<ILogHubClient>().Object);
        hub.Setup(h => h.Clients).Returns(clients.Object);
        var editing = new QsoService(repo.Object, hub.Object, radioSources: tracker);
        await editing.UpdateAsync("old", new UpdateQsoRequest(Mode: "CW"));
        return updated!;
    }

    private sealed class FakeHandler : HttpMessageHandler
    {
        public List<(string Method, string Url, string Body)> Requests { get; } = new();
        public Func<HttpRequestMessage, string, HttpResponseMessage>? Responder;

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var body = request.Content == null ? "" : await request.Content.ReadAsStringAsync(cancellationToken);
            Requests.Add((request.Method.Method, request.RequestUri!.ToString(), body));
            return Responder?.Invoke(request, body)
                ?? new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new StringContent("""{"ok":true}"""),
                };
        }
    }
}

file static class QrvQsoTotalsTestExtensions
{
    public static void SessionOnly(this Qso qso) =>
        qso.Contest = new ContestInfo { SessionId = "session-1" };
}
