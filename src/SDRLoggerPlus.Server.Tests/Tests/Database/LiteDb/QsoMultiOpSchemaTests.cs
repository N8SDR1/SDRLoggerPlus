using FluentAssertions;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database.LiteDb;
using SDRLoggerPlus.Server.Tests.Fixtures;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Database.LiteDb;

/// <summary>
/// Multi-op schema-freeze invariants (S0.5). These pin the record shape the networked layer will
/// transport: the store must accept an <b>origin-minted QSO Id</b> (the idempotency/dedup key the
/// outbox retry + multi-USB merge depend on — a host must never overwrite it), and the per-QSO
/// <c>Operator</c> + <c>LoggedByStation</c> fields must round-trip.
/// </summary>
[Trait("Category", "Integration")]
public class QsoMultiOpSchemaTests : IDisposable
{
    private readonly LiteDbTestFixture _fixture;
    private readonly LiteQsoRepository _repo;

    public QsoMultiOpSchemaTests()
    {
        _fixture = new LiteDbTestFixture();
        _repo = new LiteQsoRepository(_fixture.Context);
    }

    public void Dispose() => _fixture.Dispose();

    private static Qso Basic() => new()
    {
        Callsign = "W1AW",
        Band = "20m",
        Mode = "SSB",
        QsoDate = DateTime.UtcNow.Date,
        TimeOn = "1200",
    };

    [Fact]
    public async Task CreateAsync_respectsAClientMintedId()
    {
        // A field laptop mints the Id before sending it to the host; the host must NOT overwrite it,
        // or an outbox retry / a merge of two USB copies would double-log the same contact.
        var qso = Basic();
        qso.Id = "5f9a1b2c3d4e5f6a7b8c9d0e"; // origin-minted ObjectId-format id

        var created = await _repo.CreateAsync(qso);

        created.Id.Should().Be("5f9a1b2c3d4e5f6a7b8c9d0e");
        (await _repo.GetByIdAsync(created.Id)).Should().NotBeNull();
    }

    [Fact]
    public async Task CreateAsync_generatesAnId_whenNoneProvided()
    {
        var created = await _repo.CreateAsync(Basic());
        created.Id.Should().NotBeNullOrEmpty();
    }

    [Fact]
    public async Task Operator_andLoggedByStation_roundTrip()
    {
        var qso = Basic();
        qso.Contest = new ContestInfo { Operator = "N8SDR", LoggedByStation = "station-A" };

        var created = await _repo.CreateAsync(qso);
        var loaded = await _repo.GetByIdAsync(created.Id);

        loaded!.Contest!.Operator.Should().Be("N8SDR");
        loaded.Contest.LoggedByStation.Should().Be("station-A");
    }

    // -- S2: idempotent create on the origin-minted Id -----------------------------------------------

    [Fact]
    public async Task CreateAsync_withAnExistingId_isIdempotent_noDoubleLog()
    {
        var qso = Basic();
        qso.Id = "5f9a1b2c3d4e5f6a7b8c9d0e";
        var first = await _repo.CreateAsync(qso);

        // A retry (first write landed, ack lost) or a re-merge sends the same Id again.
        var retry = Basic();
        retry.Id = "5f9a1b2c3d4e5f6a7b8c9d0e";
        var second = await _repo.CreateAsync(retry); // must NOT throw or double-log

        second.Id.Should().Be(first.Id);
        (await _repo.GetCountAsync()).Should().Be(1); // one row, not two
    }

    [Fact]
    public async Task CreateBulkAsync_skipsAlreadyPresentIds()
    {
        var a = Basic();
        a.Id = "5f9a1b2c3d4e5f6a7b8c9d01";
        await _repo.CreateAsync(a);

        var again = Basic();
        again.Id = "5f9a1b2c3d4e5f6a7b8c9d01"; // already present → skip
        var fresh = Basic();
        fresh.Id = "5f9a1b2c3d4e5f6a7b8c9d02"; // new → insert

        await _repo.CreateBulkAsync(new[] { again, fresh });

        (await _repo.GetCountAsync()).Should().Be(2); // a + fresh, not 3
    }
}
