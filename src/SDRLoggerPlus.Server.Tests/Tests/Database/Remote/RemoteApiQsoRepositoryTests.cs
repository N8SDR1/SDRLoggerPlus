using FluentAssertions;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database;
using SDRLoggerPlus.Server.Core.Database.LiteDb;
using SDRLoggerPlus.Server.Core.Database.Remote;
using SDRLoggerPlus.Server.Tests.Fixtures;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Database.Remote;

/// <summary>
/// S1 end-to-end: a field station's <see cref="RemoteApiQsoRepository"/> logging into and dupe-checking
/// against a HOST's shared log over real HTTP. Boots a minimal TestServer hosting only the host-side
/// <c>DataSyncController</c> over a temp LiteDB, then drives it through the client repo — proving the full
/// loop (client repo → HTTP → host controller → host DB) round-trips the QSO entity with fidelity.
/// </summary>
[Trait("Category", "Integration")]
public class RemoteApiQsoRepositoryTests : IDisposable
{
    private readonly LiteDbTestFixture _hostDb;
    private readonly TestServer _host;
    private readonly RemoteApiQsoRepository _client; // the field station's repo

    public RemoteApiQsoRepositoryTests()
    {
        _hostDb = new LiteDbTestFixture();
        var hostRepo = new LiteQsoRepository(_hostDb.Context);

        var builder = new WebHostBuilder()
            .ConfigureServices(services =>
            {
                services.AddControllers(o =>
                        o.SuppressImplicitRequiredAttributeForNonNullableReferenceTypes = true)
                    .AddApplicationPart(typeof(SDRLoggerPlus.Server.Controllers.DataSyncController).Assembly)
                    .AddJsonOptions(o => o.JsonSerializerOptions.Converters.Add(
                        new SDRLoggerPlus.Server.Core.Serialization.BsonDocumentJsonConverter()));
                services.AddScoped<IQsoRepository>(_ => hostRepo); // the host's local shared log
                services.AddSignalR(); // DataSyncController broadcasts new QSOs to the hub
            })
            .Configure(app =>
            {
                app.UseRouting();
                app.UseEndpoints(e =>
                {
                    e.MapControllers();
                    e.MapHub<SDRLoggerPlus.Server.Hubs.LogHub>("/hubs/log");
                });
            });

        _host = new TestServer(builder);
        _client = new RemoteApiQsoRepository(_host.CreateClient(), NullLogger<RemoteApiQsoRepository>.Instance);
    }

    public void Dispose()
    {
        _host.Dispose();
        _hostDb.Dispose();
    }

    private static Qso Qso(string call = "DL1ABC") => new()
    {
        Callsign = call, Band = "20m", Mode = "SSB",
        QsoDate = DateTime.UtcNow.Date, TimeOn = "1200",
    };

    [Fact]
    public async Task Create_onClient_landsInHostLog_andReadsBack()
    {
        var created = await _client.CreateAsync(Qso("W1AW"));

        created.Id.Should().NotBeNullOrEmpty();
        // It's really in the HOST's database, not the client:
        (await _client.GetByIdAsync(created.Id))!.Callsign.Should().Be("W1AW");
        (await _client.GetCountAsync()).Should().Be(1);
        (await _client.GetRecentAsync()).Should().ContainSingle(q => q.Callsign == "W1AW");
    }

    [Fact]
    public async Task OriginMintedId_isPreserved_throughTheWire()
    {
        var qso = Qso();
        qso.Id = "5f9a1b2c3d4e5f6a7b8c9d0e"; // client mints it before sending (idempotency key)
        var created = await _client.CreateAsync(qso);
        created.Id.Should().Be("5f9a1b2c3d4e5f6a7b8c9d0e");
    }

    [Fact]
    public async Task DupeCheck_seesAContactAnotherStationJustLogged()
    {
        // Simulate station A logging directly into the host's shared log...
        var hostRepo = new LiteQsoRepository(_hostDb.Context);
        await hostRepo.CreateAsync(Qso("VK3AMP"));

        // ...station B (the client) dupe-checks against the SAME shared log and sees it.
        var dupe = await _client.FindRecentDuplicateAsync("VK3AMP", "20m", "SSB", DateTime.UtcNow.AddMinutes(-5));
        dupe.Should().NotBeNull();
        dupe!.Callsign.Should().Be("VK3AMP");
    }

    [Fact]
    public async Task GetById_missing_returnsNull_notThrow()
    {
        (await _client.GetByIdAsync("5f9a1b2c3d4e5f6a7b8c9d0e")).Should().BeNull();
    }

    [Fact]
    public async Task Create_preservesAdifExtra_throughTheWire()
    {
        // Data-fidelity check: a QSO carrying custom ADIF fields must survive the remote round-trip,
        // or the shared log silently loses them. AdifExtra is a LiteDB BsonDocument.
        var qso = Qso();
        qso.AdifExtra = new MongoDB.Bson.BsonDocument { { "my_custom_field", "hello" }, { "iota", "EU-005" } };

        var created = await _client.CreateAsync(qso);
        var loaded = await _client.GetByIdAsync(created.Id);

        loaded!.AdifExtra.Should().NotBeNull();
        loaded!.AdifExtra!["my_custom_field"].AsString.Should().Be("hello");
        loaded.AdifExtra["iota"].AsString.Should().Be("EU-005");
    }

    [Fact]
    public async Task Create_sameOriginId_twice_landsOnceOnTheHost()
    {
        // The outbox will re-send a QSO whose ack was lost. With the origin-minted Id + the host's
        // idempotent create, that retry must NOT double-log — the whole client→host→DB chain.
        var qso = Qso();
        qso.Id = "5f9a1b2c3d4e5f6a7b8c9d0e";
        await _client.CreateAsync(qso);

        var retry = Qso();
        retry.Id = "5f9a1b2c3d4e5f6a7b8c9d0e";
        await _client.CreateAsync(retry);

        (await _client.GetCountAsync()).Should().Be(1);
    }

    [Fact]
    public async Task HostOnlySyncMethods_areNoOpsOnAClient()
    {
        // A client must never independently upload the shared log — these delegate to the host.
        (await _client.GetPendingSyncCountAsync()).Should().Be(0);
        (await _client.MarkAllQrzSyncedAsync()).Should().Be(0);
        (await _client.GetUnsyncedToQrzAsync()).Should().BeEmpty();
    }
}
