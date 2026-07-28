using System.Text;
using FluentAssertions;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging;
using Moq;
using SDRLoggerPlus.Server.Core.Database;
using SDRLoggerPlus.Server.Core.Database.LiteDb;
using SDRLoggerPlus.Server.Hubs;
using SDRLoggerPlus.Server.Services;
using SDRLoggerPlus.Server.Tests.Fixtures;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Database.LiteDb;

/// <summary>
/// The K3UK regression, end-to-end through the REAL LiteDB repository.
///
/// The existing AdifImportDuplicateTests use an in-memory List&lt;Qso&gt; mock repo, so rows keep the
/// Kind=Utc they were parsed with and the LiteDB local-read projection is never exercised — which is
/// exactly why those tests stayed green while re-imports still duplicated in production. This test
/// imports through a live LiteDbContext so the existing-keys set is built from LiteDB-read rows,
/// reproducing (and now guarding) the real path.
///
/// Scenario: an evening QSO at 02:30 UTC. West of UTC that stored instant reads back on the PREVIOUS
/// local calendar day; before the canonical-UTC projection the import dedupe key computed a local
/// date for the existing row and a UTC date for the freshly parsed row, so they disagreed and the
/// contact re-imported. With the projection both sides are UTC and the duplicate is caught.
///
/// NOTE: run the suite in a NON-UTC time zone (e.g. TZ=America/New_York) to actually exercise the
/// day-boundary gap — on a UTC runner local==UTC and the assertion passes trivially. The CI job
/// should run at least once off-UTC; this is the class of bug a UTC-only runner hides.
/// </summary>
[Trait("Category", "Integration")]
public class AdifReimportUtcRegressionTests : IDisposable
{
    private readonly LiteDbTestFixture _fixture;
    private readonly AdifService _service;

    public AdifReimportUtcRegressionTests()
    {
        _fixture = new LiteDbTestFixture();
        var repo = new LiteQsoRepository(_fixture.Context);

        var hub = new Mock<IHubContext<LogHub, ILogHubClient>>();
        var clients = new Mock<IHubClients<ILogHubClient>>();
        clients.Setup(c => c.All).Returns(new Mock<ILogHubClient>().Object);
        hub.Setup(h => h.Clients).Returns(clients.Object);

        _service = new AdifService(
            repo,
            new Mock<ISettingsRepository>().Object,
            hub.Object,
            new Mock<ILogger<AdifService>>().Object);
    }

    public void Dispose() => _fixture.Dispose();

    // Evening QSO: 02:30 UTC — the previous local day for any negative UTC offset.
    private static Stream EveningQso() => new MemoryStream(Encoding.UTF8.GetBytes(
        "<CALL:5>G0AMO <QSO_DATE:8>20260310 <TIME_ON:4>0230 <BAND:3>20m " +
        "<MODE:3>SSB <FREQ:6>14.084 <EOR>"));

    [Fact]
    public async Task Reimporting_the_same_evening_qso_skips_it_through_real_litedb()
    {
        var first = await _service.ImportAdifAsync(EveningQso());
        first.ImportedCount.Should().Be(1);

        // Re-import the identical file — the existing row now comes from LiteDB (the real read path).
        var second = await _service.ImportAdifAsync(EveningQso());

        second.SkippedDuplicates.Should().Be(1,
            "the same evening contact must be recognized as a duplicate on re-import");
        second.ImportedCount.Should().Be(0);

        (await new LiteQsoRepository(_fixture.Context).GetAllAsync())
            .Should().ContainSingle("re-importing the same file must not grow the log");
    }
}
