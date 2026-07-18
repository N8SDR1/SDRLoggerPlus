using System.Text.Json;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Moq;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database;
using SDRLoggerPlus.Server.Core.Database.LiteDb;
using SDRLoggerPlus.Server.Services;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

/// <summary>
/// Tests for database startup behaviour. SDRLoggerPlus is LiteDB-only:
///   1. No config.json → defaults to Local (LiteDB), start immediately.
///   2. AddDatabase always registers the LiteDB services.
///   3. Settings round-trip + LiteDB persistence work without errors.
/// </summary>
[Trait("Category", "Unit")]
public class StartupProviderTests
{
    // ──────────────────────────────────────────────────────────
    // 1. UserConfigService defaults
    // ──────────────────────────────────────────────────────────

    [Fact]
    public void UserConfig_Defaults_To_Local_Provider()
    {
        var config = new UserConfig();
        config.Provider.Should().Be(DatabaseProvider.Local);
    }

    [Fact]
    public async Task UserConfigService_Returns_Local_When_No_ConfigFile_Exists()
    {
        var logger = new Mock<ILogger<UserConfigService>>();
        // Use a path that definitely doesn't exist
        var svc = new UserConfigService(logger.Object, Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString(), "config.json"));

        // GetConfigAsync should return defaults when no file exists
        var config = await svc.GetConfigAsync();
        config.Provider.Should().Be(DatabaseProvider.Local);
    }

    [Fact]
    public async Task GetConfigAsync_Legacy_MongoDb_Provider_File_Resolves_To_Local_And_Preserves_Config()
    {
        // A config.json written by an older (Mongo-era) build literally contains
        // the PascalCase string "Provider":"MongoDb" — a value the enum no longer
        // has. Loading it must NOT throw: it resolves to Local, ignores the dead
        // MongoDb fields, and preserves the rest (ConfiguredAt) so the upgrading
        // user is never re-prompted through the setup wizard.
        var dir = Path.Combine(Path.GetTempPath(), $"sdrloggerplus_cfg_{Guid.NewGuid():N}");
        Directory.CreateDirectory(dir);
        var path = Path.Combine(dir, "config.json");
        await File.WriteAllTextAsync(path,
            "{\"Provider\":\"MongoDb\",\"MongoDbConnectionString\":\"mongodb+srv://old/db\",\"ConfiguredAt\":\"2024-01-01T00:00:00Z\"}");
        try
        {
            var svc = new UserConfigService(Mock.Of<ILogger<UserConfigService>>(), path);

            var config = await svc.GetConfigAsync();
            config.Provider.Should().Be(DatabaseProvider.Local);
            config.ConfiguredAt.Should().NotBeNull("the upgrading user's config must be preserved");

            svc.IsConfigured().Should().BeTrue();
        }
        finally
        {
            try { Directory.Delete(dir, true); } catch { }
        }
    }

    // ──────────────────────────────────────────────────────────
    // 2. DbServiceRegistration
    // ──────────────────────────────────────────────────────────

    [Fact]
    public void AddDatabase_Local_Registers_LiteDb_Services()
    {
        var services = new Microsoft.Extensions.DependencyInjection.ServiceCollection();
        var config = new UserConfig { Provider = DatabaseProvider.Local };

        services.AddDatabase(config);

        // Should register LiteDbContext as IDbContext
        var dbContextDescriptor = services.FirstOrDefault(d => d.ServiceType == typeof(IDbContext));
        dbContextDescriptor.Should().NotBeNull();
        // LiteDbContext should be registered
        services.Any(d => d.ServiceType == typeof(LiteDbContext) || d.ImplementationType == typeof(LiteDbContext))
            .Should().BeTrue("Local provider must register LiteDbContext");

        // Should NOT register MongoDbContext
        services.Any(d => d.ImplementationType?.Name == "MongoDbContext")
            .Should().BeFalse("Local provider must not register MongoDbContext");
    }

    // ──────────────────────────────────────────────────────────
    // 3. Settings model round-trip (JSON serialization)
    // ──────────────────────────────────────────────────────────

    [Fact]
    public void Settings_Model_Deserializes_All_Frontend_Properties()
    {
        // This JSON represents what the frontend sends to POST /api/settings.
        // Every property must deserialize without error.
        var frontendJson = """
        {
            "station": {
                "callsign": "EI2KK",
                "operatorName": "Test",
                "gridSquare": "IO63",
                "latitude": 53.0,
                "longitude": -7.0,
                "city": "Dublin",
                "country": "Ireland"
            },
            "qrz": {
                "username": "test",
                "password": "test",
                "apiKey": "",
                "enabled": false
            },
            "appearance": {
                "theme": "dark"
            },
            "rotator": {
                "enabled": false,
                "connectionType": "network",
                "ipAddress": "127.0.0.1",
                "port": 4533,
                "serialPort": "",
                "baudRate": 9600,
                "hamlibModelId": null,
                "hamlibModelName": "",
                "pollingIntervalMs": 500,
                "rotatorId": "default",
                "presets": [
                    { "name": "N", "azimuth": 0 },
                    { "name": "E", "azimuth": 90 }
                ]
            },
            "radio": {
                "followRadio": true,
                "activeRigType": null,
                "autoReconnect": false,
                "autoConnectRigId": null,
                "tci": {
                    "host": "localhost",
                    "port": 50001,
                    "name": "",
                    "autoConnect": false
                }
            },
            "map": {
                "tileLayer": "dark",
                "showSatellites": false,
                "selectedSatellites": ["ISS", "AO-91"],
                "rbn": {
                    "enabled": false,
                    "opacity": 0.7,
                    "showPaths": true,
                    "timeWindowMinutes": 5,
                    "minSnr": -10,
                    "bands": ["all"],
                    "modes": ["CW", "RTTY"]
                },
                "showPotaOverlay": false,
                "showDayNightOverlay": true,
                "showGrayLine": true,
                "showSunMarker": true,
                "showMoonMarker": true,
                "dayNightOpacity": 0.5,
                "grayLineOpacity": 0.6,
                "showCallsignImages": true,
                "maxCallsignImages": 50
            },
            "cluster": {
                "connections": []
            },
            "header": {
                "timeFormat": "24h",
                "showWeather": true,
                "weatherLocation": "Dublin"
            },
            "ai": {
                "provider": "anthropic",
                "apiKey": "",
                "model": "claude-sonnet-4-5-20250929",
                "autoGenerateTalkPoints": true,
                "includeQrzProfile": true,
                "includeQsoHistory": true,
                "includeSpotComments": false
            }
        }
        """;

        var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
        var settings = JsonSerializer.Deserialize<UserSettings>(frontendJson, options);

        settings.Should().NotBeNull();
        settings!.Station.Callsign.Should().Be("EI2KK");
        settings.Header.Should().NotBeNull();
        settings.Header.TimeFormat.Should().Be("24h");
        settings.Header.ShowWeather.Should().BeTrue();
        settings.Header.WeatherLocation.Should().Be("Dublin");
        settings.Rotator.ConnectionType.Should().Be("network");
        settings.Rotator.SerialPort.Should().Be("");
        settings.Rotator.BaudRate.Should().Be(9600);
        settings.Rotator.HamlibModelId.Should().BeNull();
        settings.Rotator.HamlibModelName.Should().Be("");
        settings.Radio.Tci.AutoConnect.Should().BeFalse();
        settings.Map.ShowSatellites.Should().BeFalse();
        settings.Map.SelectedSatellites.Should().Contain("ISS");
        settings.Map.ShowPotaOverlay.Should().BeFalse();
        settings.Map.ShowDayNightOverlay.Should().BeTrue();
        settings.Map.ShowGrayLine.Should().BeTrue();
        settings.Map.ShowSunMarker.Should().BeTrue();
        settings.Map.ShowMoonMarker.Should().BeTrue();
        settings.Map.DayNightOpacity.Should().Be(0.5);
        settings.Map.GrayLineOpacity.Should().Be(0.6);
        settings.Map.ShowCallsignImages.Should().BeTrue();
        settings.Map.MaxCallsignImages.Should().Be(50);
    }

    [Fact]
    public void Settings_Model_Handles_Unknown_Future_Properties_Gracefully()
    {
        // If the frontend adds new properties in the future,
        // the backend should NOT reject the JSON.
        var jsonWithExtraProps = """
        {
            "station": { "callsign": "EI2KK" },
            "someNewSection": { "foo": "bar" },
            "header": { "timeFormat": "12h", "newField": true }
        }
        """;

        var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
        var act = () => JsonSerializer.Deserialize<UserSettings>(jsonWithExtraProps, options);

        // Should NOT throw — unknown properties must be silently ignored
        act.Should().NotThrow();
        var settings = act();
        settings!.Station.Callsign.Should().Be("EI2KK");
        settings.Header.TimeFormat.Should().Be("12h");
    }

    [Fact]
    public void Settings_Model_Serializes_And_Deserializes_Roundtrip()
    {
        var original = new UserSettings
        {
            Station = new StationSettings { Callsign = "EI2KK", GridSquare = "IO63" },
            Header = new HeaderSettings { TimeFormat = "12h", ShowWeather = false, WeatherLocation = "Cork" },
            Rotator = new RotatorSettings
            {
                Enabled = true,
                ConnectionType = "serial",
                SerialPort = "/dev/ttyUSB0",
                BaudRate = 19200,
                HamlibModelId = 603,
                HamlibModelName = "Yaesu GS-232B"
            },
            Map = new MapSettings
            {
                ShowSatellites = true,
                SelectedSatellites = new List<string> { "ISS" },
                ShowDayNightOverlay = true,
                ShowGrayLine = true,
                DayNightOpacity = 0.3,
                GrayLineOpacity = 0.4,
                ShowCallsignImages = false,
                MaxCallsignImages = 25
            },
            Radio = new RadioSettings
            {
                Tci = new TciSettings { AutoConnect = true, Host = "192.168.1.100" }
            }
        };

        var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
        var json = JsonSerializer.Serialize(original, options);
        var deserialized = JsonSerializer.Deserialize<UserSettings>(json, options);

        deserialized.Should().NotBeNull();
        deserialized!.Station.Callsign.Should().Be("EI2KK");
        deserialized.Header.TimeFormat.Should().Be("12h");
        deserialized.Header.ShowWeather.Should().BeFalse();
        deserialized.Header.WeatherLocation.Should().Be("Cork");
        deserialized.Rotator.ConnectionType.Should().Be("serial");
        deserialized.Rotator.SerialPort.Should().Be("/dev/ttyUSB0");
        deserialized.Rotator.BaudRate.Should().Be(19200);
        deserialized.Rotator.HamlibModelId.Should().Be(603);
        deserialized.Rotator.HamlibModelName.Should().Be("Yaesu GS-232B");
        deserialized.Radio.Tci.AutoConnect.Should().BeTrue();
        deserialized.Map.ShowSatellites.Should().BeTrue();
        deserialized.Map.ShowDayNightOverlay.Should().BeTrue();
        deserialized.Map.DayNightOpacity.Should().Be(0.3);
        deserialized.Map.ShowCallsignImages.Should().BeFalse();
        deserialized.Map.MaxCallsignImages.Should().Be(25);
    }

    // ──────────────────────────────────────────────────────────
    // 4. LiteDB settings persistence
    // ──────────────────────────────────────────────────────────

    [Fact]
    public void LiteDb_Settings_SaveAndLoad_Roundtrip()
    {
        // Use a temp file for LiteDB
        var tempDir = Path.Combine(Path.GetTempPath(), $"sdrloggerplus_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempDir);
        var dbPath = Path.Combine(tempDir, "test.db");

        try
        {
            using var db = new LiteDB.LiteDatabase($"Filename={dbPath};Connection=shared");
            var collection = db.GetCollection<UserSettings>("settings");

            var settings = new UserSettings
            {
                Id = "default",
                Station = new StationSettings { Callsign = "EI2KK", GridSquare = "IO63" },
                Header = new HeaderSettings { TimeFormat = "12h", WeatherLocation = "Dublin" },
                Rotator = new RotatorSettings
                {
                    ConnectionType = "serial",
                    SerialPort = "/dev/ttyUSB0",
                    BaudRate = 19200,
                    HamlibModelId = 603
                },
                Map = new MapSettings
                {
                    ShowDayNightOverlay = true,
                    ShowGrayLine = true,
                    DayNightOpacity = 0.3,
                    ShowCallsignImages = false,
                    MaxCallsignImages = 25
                },
                Radio = new RadioSettings
                {
                    Tci = new TciSettings { AutoConnect = true }
                }
            };

            // Save
            collection.Upsert(settings);
            db.Checkpoint();

            // Load
            var loaded = collection.FindById("default");

            loaded.Should().NotBeNull();
            loaded!.Station.Callsign.Should().Be("EI2KK");
            loaded.Header.TimeFormat.Should().Be("12h");
            loaded.Header.WeatherLocation.Should().Be("Dublin");
            loaded.Rotator.ConnectionType.Should().Be("serial");
            loaded.Rotator.SerialPort.Should().Be("/dev/ttyUSB0");
            loaded.Rotator.BaudRate.Should().Be(19200);
            loaded.Rotator.HamlibModelId.Should().Be(603);
            loaded.Map.ShowDayNightOverlay.Should().BeTrue();
            loaded.Map.ShowGrayLine.Should().BeTrue();
            loaded.Map.DayNightOpacity.Should().Be(0.3);
            loaded.Map.ShowCallsignImages.Should().BeFalse();
            loaded.Map.MaxCallsignImages.Should().Be(25);
            loaded.Radio.Tci.AutoConnect.Should().BeTrue();
        }
        finally
        {
            // Cleanup
            try { Directory.Delete(tempDir, true); } catch { }
        }
    }

    [Fact]
    public void LiteDb_Settings_Handles_Extra_Elements_On_Read()
    {
        // Simulate reading a document that has extra fields (e.g. saved by a newer version).
        // BsonIgnoreExtraElements should prevent errors.
        var tempDir = Path.Combine(Path.GetTempPath(), $"sdrloggerplus_test_{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempDir);
        var dbPath = Path.Combine(tempDir, "test.db");

        try
        {
            using var db = new LiteDB.LiteDatabase($"Filename={dbPath};Connection=shared");
            var rawCollection = db.GetCollection("settings");

            // Insert a document with extra fields that don't exist on the model
            var doc = new LiteDB.BsonDocument
            {
                ["_id"] = "default",
                ["station"] = new LiteDB.BsonDocument { ["callsign"] = "EI2KK" },
                ["header"] = new LiteDB.BsonDocument { ["timeFormat"] = "24h", ["futureField"] = "value" },
                ["futureSection"] = new LiteDB.BsonDocument { ["key"] = "val" }
            };
            rawCollection.Upsert(doc);
            db.Checkpoint();

            // Reading via the typed collection should NOT throw
            var typedCollection = db.GetCollection<UserSettings>("settings");
            var loaded = typedCollection.FindById("default");

            loaded.Should().NotBeNull();
            loaded!.Station.Callsign.Should().Be("EI2KK");
            loaded.Header.TimeFormat.Should().Be("24h");
        }
        finally
        {
            try { Directory.Delete(tempDir, true); } catch { }
        }
    }

    // ──────────────────────────────────────────────────────────
    // 5. Default settings values
    // ──────────────────────────────────────────────────────────

    [Fact]
    public void HeaderSettings_Has_Correct_Defaults()
    {
        var header = new HeaderSettings();
        header.TimeFormat.Should().Be("24h");
        header.ShowWeather.Should().BeTrue();
        header.WeatherLocation.Should().BeEmpty();
    }

    [Fact]
    public void RotatorSettings_Has_Correct_Defaults_For_New_Properties()
    {
        var rotator = new RotatorSettings();
        rotator.ConnectionType.Should().Be("network");
        rotator.SerialPort.Should().BeEmpty();
        rotator.BaudRate.Should().Be(9600);
        rotator.HamlibModelId.Should().BeNull();
        rotator.HamlibModelName.Should().BeEmpty();
    }

    [Fact]
    public void MapSettings_Has_Correct_Defaults_For_New_Properties()
    {
        var map = new MapSettings();
        map.ShowSatellites.Should().BeFalse();
        map.SelectedSatellites.Should().BeEquivalentTo(new[] { "ISS", "AO-91", "SO-50" });
        map.ShowPotaOverlay.Should().BeFalse();
        map.ShowDayNightOverlay.Should().BeFalse();
        map.ShowGrayLine.Should().BeFalse();
        map.ShowSunMarker.Should().BeTrue();
        map.ShowMoonMarker.Should().BeTrue();
        map.DayNightOpacity.Should().Be(0.5);
        map.GrayLineOpacity.Should().Be(0.6);
        map.ShowCallsignImages.Should().BeTrue();
        map.MaxCallsignImages.Should().Be(50);
    }

    [Fact]
    public void TciSettings_Has_AutoConnect_Default()
    {
        var tci = new TciSettings();
        tci.AutoConnect.Should().BeFalse();
    }

    [Fact]
    public void UserSettings_Has_Header_Section()
    {
        var settings = new UserSettings();
        settings.Header.Should().NotBeNull();
        settings.Header.TimeFormat.Should().Be("24h");
    }
}
