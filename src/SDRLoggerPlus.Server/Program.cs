using Serilog;
using Microsoft.AspNetCore.SignalR;
using SDRLoggerPlus.Server.Core.Database;
using SDRLoggerPlus.Server.Core.Database.LiteDb;
using SDRLoggerPlus.Server.Core.Events;
using SDRLoggerPlus.Server.Services;
using SDRLoggerPlus.Server.Services.Backup;
using SDRLoggerPlus.Server.Services.Wsjtx;
using SDRLoggerPlus.Server.Services.Weather;
using SDRLoggerPlus.Server.Services.Sat;
using SDRLoggerPlus.Server.Hubs;

var builder = WebApplication.CreateBuilder(args);

// Note: URLs are controlled via ASPNETCORE_URLS environment variable
// When running from Electron, main.js sets this to http://localhost:{port}
// For standalone development, run with: ASPNETCORE_URLS=http://localhost:5050 dotnet run
// We don't use UseUrls() here as it would override the environment variable

// Configure Serilog
Log.Logger = new LoggerConfiguration()
    .ReadFrom.Configuration(builder.Configuration)
    .Enrich.FromLogContext()
    .WriteTo.Console()
    .CreateLogger();

builder.Host.UseSerilog();

// Add controllers
builder.Services.AddControllers();
builder.Services.AddMemoryCache();

// Add API Explorer and Swagger
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(c =>
{
    c.SwaggerDoc("v1", new() { Title = "SDRLoggerPlus API", Version = "v1" });
});

// Add SignalR with JSON enum string serialization
builder.Services.AddSignalR()
    .AddJsonProtocol(options =>
    {
        options.PayloadSerializerOptions.Converters.Add(new System.Text.Json.Serialization.JsonStringEnumConverter());
    });

// Add CORS
builder.Services.AddCors(options =>
{
    options.AddDefaultPolicy(policy =>
    {
        policy.SetIsOriginAllowed(_ => true)
            .AllowAnyHeader()
            .AllowAnyMethod()
            .AllowCredentials();
    });
});

// SDRLoggerPlus is LiteDB-only. Load the existing config (for ConfiguredAt / paths)
// if present; otherwise use defaults. The provider is always Local.
var userConfigService = new UserConfigService(
    LoggerFactory.Create(b => b.AddConsole()).CreateLogger<UserConfigService>());
var userConfig = File.Exists(userConfigService.GetConfigPath())
    ? await userConfigService.GetConfigAsync()
    : new UserConfig();
userConfig.Provider = DatabaseProvider.Local;

// Register the same instance used at startup so DI callers see the resolved config
builder.Services.AddSingleton<IUserConfigService>(userConfigService);

// Register database provider and repositories
builder.Services.AddDatabase(userConfig);

// Register services
builder.Services.AddScoped<IQsoService, QsoService>();
builder.Services.AddScoped<IAwardsService, AwardsService>();
builder.Services.AddScoped<ISettingsService, SettingsService>();
builder.Services.AddScoped<IQrzService, QrzService>();
builder.Services.AddSingleton<ITqslRunner, TqslRunner>();
builder.Services.AddScoped<ILotwService, LotwService>();
builder.Services.AddScoped<IAdifService, AdifService>();
builder.Services.AddScoped<IAiService, AiService>();

// Register HTTP client for external APIs
builder.Services.AddHttpClient("QRZ", client =>
{
    client.Timeout = TimeSpan.FromSeconds(30);
    client.DefaultRequestHeaders.Add("User-Agent", "SDRLoggerPlus/1.0");
});

builder.Services.AddHttpClient("AI", client =>
{
    client.Timeout = TimeSpan.FromSeconds(60);
    client.DefaultRequestHeaders.Add("User-Agent", "SDRLoggerPlus/1.0");
});

// Register default HTTP client factory for space weather and other APIs
builder.Services.AddHttpClient();

// Register Space Weather service (shared data source for controllers and propagation)
builder.Services.AddSingleton<ISpaceWeatherService, SpaceWeatherService>();

// Register Propagation service
builder.Services.AddSingleton<IPropagationService, PropagationService>();

// Register Contests service
builder.Services.AddSingleton<ContestsService>();

// Register DX News service
builder.Services.AddScoped<IDXNewsService, DXNewsService>();

// Register event bus
builder.Services.AddSingleton<IEventBus, EventBus>();

// Register Antenna Genius service
builder.Services.AddSingleton<AntennaGeniusService>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<AntennaGeniusService>());

// Register Tuner Genius service
builder.Services.AddSingleton<TunerGeniusService>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<TunerGeniusService>());

// Register PGXL Amplifier service
builder.Services.AddSingleton<PgxlService>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<PgxlService>());


// Register TCI Radio CAT service
builder.Services.AddSingleton<TciRadioService>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<TciRadioService>());

// Register Hamlib rigctld CAT service
builder.Services.AddSingleton<HamlibService>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<HamlibService>());


// Club Log realtime QSO upload (singleton so the one-strike auth block persists)
builder.Services.AddSingleton<ClubLogService>(sp =>
    new ClubLogService(
        sp.GetRequiredService<ISettingsService>(),
        sp.GetRequiredService<IHttpClientFactory>().CreateClient("ClubLog"),
        sp.GetRequiredService<ILogger<ClubLogService>>()));

// HRDLog.net realtime QSO upload
builder.Services.AddSingleton<HrdLogService>(sp =>
    new HrdLogService(
        sp.GetRequiredService<ISettingsService>(),
        sp.GetRequiredService<IHttpClientFactory>().CreateClient("HrdLog"),
        sp.GetRequiredService<ILogger<HrdLogService>>()));

// Register Rotator service (hamlib rotctld)
builder.Services.AddSingleton<RotatorService>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<RotatorService>());

// Register Hot List service (must be registered before DX cluster / RBN which depend on it)
builder.Services.AddSingleton<HotListService>();
builder.Services.AddSingleton<IHotListService>(sp => sp.GetRequiredService<HotListService>());
builder.Services.AddHostedService(sp => sp.GetRequiredService<HotListService>());

// Register Spot Status service (must be registered before DxClusterService which depends on it)
builder.Services.AddSingleton<SpotStatusService>();
builder.Services.AddSingleton<ISpotStatusService>(sp => sp.GetRequiredService<SpotStatusService>());
builder.Services.AddHostedService(sp => sp.GetRequiredService<SpotStatusService>());

// Register DX Cluster service
builder.Services.AddSingleton<DxClusterService>();
builder.Services.AddSingleton<IDxClusterService>(sp => sp.GetRequiredService<DxClusterService>());
builder.Services.AddHostedService(sp => sp.GetRequiredService<DxClusterService>());

// Spothole.app REST spot aggregator (default spot source; feeds the cluster pipeline)
builder.Services.AddSingleton<SpotholeService>(sp =>
    new SpotholeService(
        sp.GetRequiredService<ILogger<SpotholeService>>(),
        sp.GetRequiredService<IServiceScopeFactory>(),
        sp.GetRequiredService<DxClusterService>(),
        sp.GetRequiredService<IHttpClientFactory>().CreateClient("Spothole")));
builder.Services.AddHostedService(sp => sp.GetRequiredService<SpotholeService>());

// Register RBN service
builder.Services.AddSingleton<RbnService>();
builder.Services.AddSingleton<IRbnService>(sp => sp.GetRequiredService<RbnService>());
builder.Services.AddHostedService(sp => sp.GetRequiredService<RbnService>());

// Register CW Keyer service
builder.Services.AddSingleton<CwKeyerService>();

// Register WSJT-X auto-logging service (UDP listener)
builder.Services.AddSingleton<WsjtxService>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<WsjtxService>());

// Register weather alert service (lightning + high wind pollers)
builder.Services.AddSingleton<INwsClient, NwsClient>();
builder.Services.AddSingleton<IAmbientWeatherClient, AmbientWeatherClient>();
builder.Services.AddSingleton<IEcowittClient, EcowittClient>();
builder.Services.AddSingleton<IBlitzortungClient, BlitzortungClient>();
builder.Services.AddSingleton<WeatherAlertService>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<WeatherAlertService>());

// Register CSN S.A.T. controller integration
builder.Services.AddSingleton<SatControllerService>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<SatControllerService>());

// Register scheduled backup service
builder.Services.AddSingleton<BackupService>(sp =>
{
    var userConfig = sp.GetRequiredService<IUserConfigService>();
    var configDir = Path.GetDirectoryName(userConfig.GetConfigPath())!;
    // LiteDB → raw DB file copy + ADIF export.
    var liteDb = sp.GetRequiredService<IDbContext>() as LiteDbContext;
    var liteDbPath = liteDb != null
        ? (liteDb.DatabaseFilePath ?? Path.Combine(configDir, "sdrloggerplus.db"))
        : null;
    return new BackupService(
        sp.GetRequiredService<ISettingsService>(),
        sp.GetRequiredService<IAdifService>(),
        configDir,
        liteDbPath,
        sp.GetRequiredService<ILoggerFactory>());
});
builder.Services.AddHostedService(sp => sp.GetRequiredService<BackupService>());

// RBN band-opening alerts (own telnet session, VHF/UHF only, voice on frontend)
builder.Services.AddSingleton<SDRLoggerPlus.Server.Services.BandOpening.BandOpeningService>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<SDRLoggerPlus.Server.Services.BandOpening.BandOpeningService>());

// ADIF File Monitor — auto-import QSOs appended to watched external .adi files
builder.Services.AddSingleton<AdifMonitorService>(sp =>
{
    var userConfig = sp.GetRequiredService<IUserConfigService>();
    var configDir = Path.GetDirectoryName(userConfig.GetConfigPath())!;
    return new AdifMonitorService(
        sp.GetRequiredService<ILogger<AdifMonitorService>>(),
        sp.GetRequiredService<IServiceScopeFactory>(),
        sp.GetRequiredService<IHubContext<LogHub, ILogHubClient>>(),
        configDir);
});
builder.Services.AddHostedService(sp => sp.GetRequiredService<AdifMonitorService>());

var app = builder.Build();

// Configure middleware
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseCors();

// Serve static files (React build)
app.UseDefaultFiles();
app.UseStaticFiles();

// Map controllers
app.MapControllers();

// Map SignalR hub
app.MapHub<LogHub>("/hubs/log");

// Fallback to index.html for SPA routing
app.MapFallbackToFile("index.html");

Log.Information("SDRLoggerPlus Server starting on {Urls}", string.Join(", ", app.Urls));

app.Run();
