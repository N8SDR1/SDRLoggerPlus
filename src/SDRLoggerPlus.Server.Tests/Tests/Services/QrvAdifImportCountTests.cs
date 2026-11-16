using System.Net;
using System.Net.Sockets;
using System.Text;
using FluentAssertions;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Moq;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database;
using SDRLoggerPlus.Server.Hubs;
using SDRLoggerPlus.Server.Services;
using SDRLoggerPlus.Server.Services.Qrv;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

/// <summary>
/// STD, WSJT-X, JTDX, MSHV, and VARAC append ADIF. The monitor and
/// ADIF-over-UDP stamp those new imports Logger. A manual import does not.
/// The count does not depend on that stamp. Contest contacts stay out.
/// </summary>
[Trait("Category", "Unit")]
public class QrvAdifImportCountTests : IDisposable
{
    private static readonly DateTime Now = new(2026, 11, 16, 12, 0, 0, DateTimeKind.Utc);
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "qrv-adif-" + Guid.NewGuid().ToString("N"));
    private readonly List<Qso> _stored = new();
    private readonly AdifService _adif;

    public QrvAdifImportCountTests()
    {
        Directory.CreateDirectory(_dir);
        var repo = new Mock<IQsoRepository>();
        repo.Setup(r => r.GetAllAsync()).ReturnsAsync(() => _stored.ToList());
        repo.Setup(r => r.CreateBulkAsync(It.IsAny<IEnumerable<Qso>>()))
            .Callback<IEnumerable<Qso>>(q => _stored.AddRange(q))
            .ReturnsAsync((IEnumerable<Qso> q) => q);

        _adif = new AdifService(
            repo.Object,
            new Mock<ISettingsRepository>().Object,
            Hub(),
            new Mock<ILogger<AdifService>>().Object);
    }

    [Fact]
    public async Task Monitor_and_udp_stamp_logger_manual_import_does_not_and_the_count_ignores_the_stamp()
    {
        var std = Path.Combine(_dir, "std.adi");
        var wsjtx = Path.Combine(_dir, "wsjtx_log.adi");
        await File.WriteAllTextAsync(std, Record("K1STD", "100000", "FT8"));
        await File.WriteAllTextAsync(wsjtx, Record("K1JT", "100100", "FT8"));

        var settings = new Mock<ISettingsService>();
        settings.Setup(s => s.GetSettingsAsync(It.IsAny<string>())).ReturnsAsync(new UserSettings
        {
            AdifMonitor = new AdifMonitorSettings { Enabled = true, File1 = std, File2 = wsjtx },
        });

        var monitor = new AdifMonitorService(
            new Mock<ILogger<AdifMonitorService>>().Object,
            Scope(settings.Object, _adif),
            Hub(),
            _dir);
        await monitor.TickAsync(CancellationToken.None);

        _stored.Should().HaveCount(2);
        _stored.Should().OnlyContain(q => q.RadioSource == QrvQsoTotals.SourceLogger);
        _stored.Select(q => q.Callsign).Should().BeEquivalentTo("K1STD", "K1JT");

        var udp = new AdifUdpListenerService(RootScope(_adif), new Mock<ILogger<AdifUdpListenerService>>().Object);
        var datagram = Encoding.UTF8.GetBytes(Record("N0UDP", "100200", "USB").Replace("<EOH>", ""));
        await udp.HandleDatagramAsync(new UdpReceiveResult(datagram, new IPEndPoint(IPAddress.Loopback, 9)), CancellationToken.None);

        _stored.Should().HaveCount(3);
        _stored.Single(q => q.Callsign == "N0UDP").RadioSource.Should().Be(QrvQsoTotals.SourceLogger);

        var manual = await _adif.ImportAdifAsync(StreamOf(Record("N0MAN", "100300", "CW")));
        manual.ImportedCount.Should().Be(1);
        _stored.Single(q => q.Callsign == "N0MAN").RadioSource.Should().BeNull();

        var contest = await _adif.ImportAdifAsync(
            StreamOf(Record("N0CON", "100400", "FT8", "<CONTEST_ID:9>CQ-WW-SSB ")),
            stampLogger: true);
        contest.ImportedCount.Should().Be(1);
        var contestRow = _stored.Single(q => q.Callsign == "N0CON");
        contestRow.RadioSource.Should().Be(QrvQsoTotals.SourceLogger);
        QrvQsoTotals.IsCountedContact(contestRow).Should().BeFalse();

        var pota = await _adif.ImportAdifAsync(StreamOf(Record("N0POT", "100500", "USB", "<MY_POTA_REF:6>K-0001 ")));
        pota.ImportedCount.Should().Be(1);
        var potaRow = _stored.Single(q => q.Callsign == "N0POT");
        potaRow.RadioSource.Should().BeNull();
        QrvQsoTotals.IsCountedContact(potaRow).Should().BeTrue();
        QrvQsoTotals.ModeBucket(potaRow.Mode).Should().Be("voice");

        var totals = QrvQsoTotals.Count(_stored, Now);
        totals.Week.Should().Be(new QrvQsoTotals.ModeCounts(Voice: 2, Cw: 1, Digital: 2));
        totals.Month.Should().Be(totals.Week);
        totals.Year.Should().Be(totals.Week);
    }

    [Fact]
    public void Watched_import_stamp_does_not_replace_a_source_already_on_the_row()
    {
        var qso = new Qso { RadioSource = QrvQsoTotals.SourceStd };
        QsoRadioSourceStamp.ApplyWatchedImport(qso);
        qso.RadioSource.Should().Be(QrvQsoTotals.SourceStd);

        var blank = new Qso();
        QsoRadioSourceStamp.ApplyWatchedImport(blank);
        blank.RadioSource.Should().Be(QrvQsoTotals.SourceLogger);
    }

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); }
        catch (IOException) { }
    }

    private static string Record(string call, string time, string mode, string extra = "") =>
        $"<EOH><CALL:{call.Length}>{call} <QSO_DATE:8>20261116 <TIME_ON:{time.Length}>{time} " +
        $"<BAND:3>20M <MODE:{mode.Length}>{mode} {extra}<EOR>";

    private static Stream StreamOf(string adif) => new MemoryStream(Encoding.UTF8.GetBytes(adif));

    private static IServiceScopeFactory Scope(ISettingsService settings, IAdifService adif)
    {
        var provider = new Mock<IServiceProvider>();
        provider.Setup(p => p.GetService(typeof(ISettingsService))).Returns(settings);
        provider.Setup(p => p.GetService(typeof(IAdifService))).Returns(adif);
        var scope = new Mock<IServiceScope>();
        scope.Setup(s => s.ServiceProvider).Returns(provider.Object);
        var factory = new Mock<IServiceScopeFactory>();
        factory.Setup(f => f.CreateScope()).Returns(scope.Object);
        return factory.Object;
    }

    private static IServiceProvider RootScope(IAdifService adif)
    {
        var factory = Scope(new Mock<ISettingsService>().Object, adif);
        var root = new Mock<IServiceProvider>();
        root.Setup(p => p.GetService(typeof(IServiceScopeFactory))).Returns(factory);
        return root.Object;
    }

    private static IHubContext<LogHub, ILogHubClient> Hub()
    {
        var client = new Mock<ILogHubClient>();
        client.Setup(c => c.OnAdifMonitorImport(It.IsAny<SDRLoggerPlus.Contracts.Events.AdifMonitorImportEvent>()))
            .Returns(Task.CompletedTask);
        client.Setup(c => c.OnAdifImportProgress(It.IsAny<SDRLoggerPlus.Contracts.Events.AdifImportProgressEvent>()))
            .Returns(Task.CompletedTask);
        var clients = new Mock<IHubClients<ILogHubClient>>();
        clients.Setup(c => c.All).Returns(client.Object);
        var hub = new Mock<IHubContext<LogHub, ILogHubClient>>();
        hub.Setup(h => h.Clients).Returns(clients.Object);
        return hub.Object;
    }
}
