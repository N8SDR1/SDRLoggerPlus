using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging.Abstractions;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Controllers;
using SDRLoggerPlus.Server.Services;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Controllers;

[Trait("Category", "Unit")]
public class RbnHeardMeControllerTests
{
    private sealed class FakeRbn : IRbnService
    {
        public IReadOnlyList<RbnSpot> Spots = Array.Empty<RbnSpot>();
        public IReadOnlyList<RbnSpot> GetRecentSpots(int minutes = 5) => Spots;
        public Task<(string? Grid, double? Lat, double? Lon, string? Country)> LookupSkimmerLocationAsync(string callsign)
            => Task.FromResult(callsign == "W3LPL"
                ? ((string?)null, (double?)39.0, (double?)-77.0, (string?)null)
                : ((string?)null, (double?)null, (double?)null, (string?)null));

        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }

    [Fact]
    public async Task HeardMe_returns_only_located_spots_for_my_call()
    {
        var fake = new FakeRbn
        {
            Spots = new[]
            {
                new RbnSpot { Callsign = "W3LPL", Dx = "K1ABC", Frequency = 14025, Band = "20m", Mode = "CW", Snr = 25, Timestamp = DateTime.UtcNow },
                new RbnSpot { Callsign = "NOLOC", Dx = "K1ABC", Frequency = 14025, Band = "20m", Mode = "CW", Snr = 10, Timestamp = DateTime.UtcNow },
                new RbnSpot { Callsign = "N4ZR", Dx = "OTHER", Frequency = 14025, Band = "20m", Mode = "CW", Snr = 30, Timestamp = DateTime.UtcNow },
            }
        };
        var controller = new RbnController(NullLogger<RbnController>.Instance, fake);

        var result = await controller.GetHeardMe("k1abc", null, 30);

        var ok = Assert.IsType<OkObjectResult>(result);
        var reports = Assert.IsAssignableFrom<IEnumerable<RbnHeardMeReport>>(ok.Value);
        var list = reports.ToList();
        Assert.Single(list);                       // OTHER filtered out; NOLOC dropped (no coords)
        Assert.Equal("W3LPL", list[0].Skimmer);
        Assert.Equal(39.0, list[0].Lat);
    }

    [Fact]
    public async Task Location_returns_ok_when_latlon_resolved_even_though_grid_is_null()
    {
        // The real RbnService populates only Lat/Lon (Grid stays null by design), so presence
        // must be keyed off lat/lon — not grid — or a resolved skimmer 404s.
        var controller = new RbnController(NullLogger<RbnController>.Instance, new FakeRbn());

        var result = await controller.GetSkimmerLocation("W3LPL");

        var ok = Assert.IsType<OkObjectResult>(result);
        var lat = ok.Value!.GetType().GetProperty("lat")!.GetValue(ok.Value);
        Assert.Equal(39.0, lat);
    }

    [Fact]
    public async Task Location_returns_notfound_when_coords_unresolved()
    {
        var controller = new RbnController(NullLogger<RbnController>.Instance, new FakeRbn());

        var result = await controller.GetSkimmerLocation("NOLOC");

        Assert.IsType<NotFoundObjectResult>(result);
    }
}
