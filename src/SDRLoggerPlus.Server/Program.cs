using Serilog;
using Microsoft.AspNetCore.SignalR;
using SDRLoggerPlus.Server.Core.Database;
using SDRLoggerPlus.Server.Core.Database.LiteDb;
using SDRLoggerPlus.Server.Core.Events;
using SDRLoggerPlus.Server.Core.Security;
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

// Configure Serilog. The console sink is what Electron captures into main.log,
// so it formats through ScrubbingTextFormatter: any credential that reaches a
// log line — from our code or the framework's — is masked before it is written.
// Any sink added later (here or via a WriteTo section in appsettings.json)
// bypasses that formatter unless it is wrapped the same way — wrap it, or
// credentials get a fresh unscrubbed channel.
Log.Logger = new LoggerConfiguration()
    .ReadFrom.Configuration(builder.Configuration)
    .Enrich.FromLogContext()
    .WriteTo.Console(new SDRLoggerPlus.Server.Core.Logging.ScrubbingTextFormatter())
    .CreateLogger();

builder.Host.UseSerilog();

// Add controllers
builder.Services.AddControllers(options =>
{
    // The settings DTO (and other bulk models) are round-tripped whole by the
    // frontend, so a non-nullable string that is legitimately empty/absent —
    // e.g. VoiceSettings.VoiceUri "" meaning "system default voice" — must not
    // fail model validation. Without this, .NET's implicit [Required] for
    // non-nullable reference types 400s the ENTIRE POST the moment any such
    // field is null in storage, blocking all settings saves.
    options.SuppressImplicitRequiredAttributeForNonNullableReferenceTypes = true;
});
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

// Add CORS. The packaged app serves its own SPA same-origin (no CORS needed); this
// policy is only for cross-origin callers — the Vite dev server, or a browser pointed
// at a remote backend. An explicit allow-list replaces the old reflect-any policy.
// Bearer tokens travel in the Authorization header (not cookies), so AllowCredentials
// is deliberately NOT enabled — reflect-any + credentials was the footgun removed here.
// Add a separately-hosted frontend's origin via SDRLOGGERPLUS_ALLOWED_ORIGINS (CSV).
var allowedOrigins = new[] { "http://localhost:5173", "http://127.0.0.1:5173" }
    .Concat((builder.Configuration["SDRLOGGERPLUS_ALLOWED_ORIGINS"] ?? "")
        .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
    .Distinct()
    .ToArray();
builder.Services.AddCors(options =>
{
    options.AddDefaultPolicy(policy =>
    {
        policy.WithOrigins(allowedOrigins)
            .AllowAnyHeader()
            .AllowAnyMethod();
    });
});

// Load the saved config (provider / host connection / paths) if present, else defaults. Provider is
// Local (own LiteDB — the default and the log HOST) unless this machine is configured as a field
// CLIENT (RemoteHost + a host URL); a RemoteHost with no URL falls back to Local so the app still starts.
var userConfigService = new UserConfigService(
    LoggerFactory.Create(b => b.AddConsole()).CreateLogger<UserConfigService>());
var userConfig = File.Exists(userConfigService.GetConfigPath())
    ? await userConfigService.GetConfigAsync()
    : new UserConfig();
if (userConfig.Provider == DatabaseProvider.RemoteHost && string.IsNullOrWhiteSpace(userConfig.HostUrl))
    userConfig.Provider = DatabaseProvider.Local;
if (userConfig.Provider == DatabaseProvider.RemoteHost)
    Log.Information("Data provider: RemoteHost — shared log on {HostUrl}", userConfig.HostUrl);

// ── Remote-access auth (v1.0 — networked/shared backend) ───────────────────
// Per-device bearer tokens, ENFORCED only when the backend is bound off-localhost
// (the loopback desktop stays zero-auth and unchanged). Token hashes live in a JSON
// file beside the config, like QslBlockStateStore.
// See docs/design/networked-shared-database-execution-plan.md.
var authStore = new AuthTokenStore(
    Path.Combine(Path.GetDirectoryName(userConfigService.GetConfigPath())!, "auth-tokens.json"));

// Headless token management (mint/list/revoke) runs instead of the web host.
if (AuthCli.TryHandle(args, authStore))
    return;

var bindUrls = (builder.Configuration["ASPNETCORE_URLS"] ?? "")
    .Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
var forceAuth = string.Equals(builder.Configuration["SDRLOGGERPLUS_REQUIRE_AUTH"], "true",
    StringComparison.OrdinalIgnoreCase);

// Default-deny: never expose the API + radio control to the network without auth.
var unsafeBind = RemoteAccessGuard.UnsafeBindReason(bindUrls, forceAuth, authStore.AnyDevices);
if (unsafeBind is not null)
{
    Log.Fatal("{Reason}", unsafeBind);
    Log.CloseAndFlush();
    Environment.Exit(1);
}
var enforceAuth = RemoteAccessGuard.ShouldEnforce(bindUrls, forceAuth);
if (enforceAuth)
    Log.Information("Remote access: token auth ENFORCED ({Count} device(s) registered).",
        authStore.List().Count);

builder.Services.AddSingleton(authStore);
builder.Services.AddSingleton(new AuthOptions(enforceAuth));

// Register the same instance used at startup so DI callers see the resolved config
builder.Services.AddSingleton<IUserConfigService>(userConfigService);

// Register database provider and repositories
builder.Services.AddDatabase(userConfig);

// Field-CLIENT only: bridge the host's live QSO events onto our own hub so the local UI sees
// contacts other stations just logged. A host/normal install never starts this.
if (userConfig.Provider == DatabaseProvider.RemoteHost)
    builder.Services.AddHostedService<HostBridgeService>();

// Register services
builder.Services.AddScoped<IQsoService, QsoService>();
builder.Services.AddScoped<IAwardsService, AwardsService>();
builder.Services.AddScoped<ISettingsService, SettingsService>();
builder.Services.AddScoped<IQrzService, QrzService>();
// HamQTH is a Singleton so the session_id cache survives across requests
// (LogHub is Scoped, would re-login every callsign click otherwise).
builder.Services.AddSingleton<IHamQthService, HamQthService>();
builder.Services.AddSingleton<ITqslRunner, TqslRunner>();
builder.Services.AddScoped<ILotwService, LotwService>();
builder.Services.AddScoped<IConfirmationSyncService, ConfirmationSyncService>();
builder.Services.AddHostedService<ConfirmationSyncBackgroundService>();
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

// Register Contest suite (definitions = singleton; sessions = scoped for the repo)
builder.Services.AddSingleton<SDRLoggerPlus.Server.Services.Contesting.ContestDefinitionService>();
builder.Services.AddSingleton<SDRLoggerPlus.Server.Services.Contesting.ScpService>();
builder.Services.AddSingleton<SDRLoggerPlus.Server.Services.Contesting.CallHistoryService>();
builder.Services.AddSingleton<SDRLoggerPlus.Server.Services.Contesting.ContestBroadcastService>();
builder.Services.AddScoped<SDRLoggerPlus.Server.Services.Contesting.ContestSessionService>();
builder.Services.AddScoped<SDRLoggerPlus.Server.Services.Contesting.ContestService>();

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

// Register flrig XML-RPC rig-control service (ported from v1.x)
builder.Services.AddSingleton<FlrigService>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<FlrigService>());

// Register native FlexRadio 6000 (SmartSDR) backend
builder.Services.AddSingleton<SDRLoggerPlus.Server.Services.Flex.FlexRadioService>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<SDRLoggerPlus.Server.Services.Flex.FlexRadioService>());

// Rig backend abstraction (v2.8.0). The registry replaces LogHub's per-action
// TCI→Hamlib→flrig ladders. The three services are registered as IRigBackend in
// PRECEDENCE ORDER (TCI → Hamlib → flrig) — the injection order IS the precedence.
// Each resolves to the same singleton instance already registered above.
// See docs/design/rig-backend-abstraction.md.
builder.Services.AddSingleton<SDRLoggerPlus.Server.Services.Rig.IRigBackend>(
    sp => sp.GetRequiredService<TciRadioService>());
builder.Services.AddSingleton<SDRLoggerPlus.Server.Services.Rig.IRigBackend>(
    sp => sp.GetRequiredService<HamlibService>());
builder.Services.AddSingleton<SDRLoggerPlus.Server.Services.Rig.IRigBackend>(
    sp => sp.GetRequiredService<FlrigService>());
builder.Services.AddSingleton<SDRLoggerPlus.Server.Services.Rig.IRigBackend>(
    sp => sp.GetRequiredService<SDRLoggerPlus.Server.Services.Flex.FlexRadioService>());
builder.Services.AddSingleton<SDRLoggerPlus.Server.Services.Rig.IRigRegistry,
    SDRLoggerPlus.Server.Services.Rig.RigRegistry>();


// Club Log realtime QSO upload (singleton so the one-strike auth block persists)
// Club Log's one-strike auth block, persisted beside the database so a
// restart cannot re-arm uploads against credentials already rejected.
builder.Services.AddSingleton(sp => new SDRLoggerPlus.Server.Services.Qsl.QslBlockStateStore(
    Path.Combine(
        Path.GetDirectoryName(sp.GetRequiredService<IUserConfigService>().GetConfigPath())!,
        "qsl-block-state.json")));

builder.Services.AddSingleton<ClubLogService>(sp =>
    new ClubLogService(
        sp.GetRequiredService<ISettingsService>(),
        sp.GetRequiredService<IHttpClientFactory>().CreateClient("ClubLog"),
        sp.GetRequiredService<ILogger<ClubLogService>>(),
        sp.GetRequiredService<SDRLoggerPlus.Server.Services.Qsl.QslBlockStateStore>()));

// HRDLog.net realtime QSO upload
builder.Services.AddSingleton<HrdLogService>(sp =>
    new HrdLogService(
        sp.GetRequiredService<ISettingsService>(),
        sp.GetRequiredService<IHttpClientFactory>().CreateClient("HrdLog"),
        sp.GetRequiredService<ILogger<HrdLogService>>()));

// eQSL.cc realtime QSO upload
builder.Services.AddSingleton<EqslService>(sp =>
    new EqslService(
        sp.GetRequiredService<ISettingsService>(),
        sp.GetRequiredService<IHttpClientFactory>().CreateClient("Eqsl"),
        sp.GetRequiredService<ILogger<EqslService>>()));

// Records every QSL upload attempt on the QSO it belongs to. Scoped because
// it uses the scoped QSO repository; the background upload tasks resolve it
// through their own scope.
builder.Services.AddScoped<SDRLoggerPlus.Server.Services.Qsl.QslSyncRecorder>();

// Generic ADIF-over-UDP auto-import (VarAC / N1MM / Logger32 / …)
builder.Services.AddHostedService<AdifUdpListenerService>();

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

// Globe lightning strikes (own poller; only polls while the map toggle is enabled)
builder.Services.AddSingleton<LightningStrikeService>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<LightningStrikeService>());

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

// Gate /api and /hubs behind a device token when reachable remotely. A no-op on a
// loopback-only desktop (enforce=false). Sits after CORS, before static files/endpoints;
// static assets + /api/health are exempt so the app shell can load and supply a token.
app.UseMiddleware<TokenAuthMiddleware>();

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
