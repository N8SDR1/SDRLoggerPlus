using System.Net;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Moq;
using Moq.Protected;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database;
using SDRLoggerPlus.Server.Core.Logging;
using SDRLoggerPlus.Server.Services;
using Serilog.Events;
using Serilog.Parsing;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

/// <summary>
/// T-10: no credential may reach main.log. The scrubber's own behaviour is
/// covered in <see cref="SecretScrubberTests"/>; these tests check that it is
/// actually wired in at the two places a credential gets there — the Serilog
/// formatter every line passes through, and the services that log a
/// third-party response verbatim.
/// </summary>
[Trait("Category", "Unit")]
public class CredentialRedactionTests
{
    /// <summary>Records rendered log lines the way a sink would see them.</summary>
    private sealed class CapturingLogger<T> : ILogger<T>
    {
        public readonly List<string> Lines = new();
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter) => Lines.Add(formatter(state, exception));
        public string All => string.Join("\n", Lines);
    }

    private static LogEvent EventWith(string template, params (string Name, object Value)[] properties) =>
        new(DateTimeOffset.UtcNow, LogEventLevel.Information, null,
            new MessageTemplateParser().Parse(template),
            properties.Select(p => new LogEventProperty(p.Name, new ScalarValue(p.Value))).ToArray());

    [Fact]
    public void TheSerilogFormatterMasksACredentialedUrlInAnyLogLine()
    {
        // Nothing logs this URL today. The formatter exists so that the day
        // someone adds a line that does, the credential still does not ship.
        var evt = EventWith("Downloading LoTW report from {Url}",
            ("Url", "https://lotw.arrl.org/lotwuser/lotwreport.adi?login=N9BC&password=SuperSecret42"));

        var output = new StringWriter();
        new ScrubbingTextFormatter().Format(evt, output);

        output.ToString().Should().NotContain("SuperSecret42");
        output.ToString().Should().Contain("login=N9BC");
    }

    [Fact]
    public void TheSerilogFormatterMasksCredentialsCarriedInExceptionText()
    {
        var ex = new InvalidOperationException(
            "Could not reach https://www.eQSL.cc/qslcard/DownloadInBox.cfm?UserName=N9BC&Password=InboxPass77");
        var evt = new LogEvent(DateTimeOffset.UtcNow, LogEventLevel.Error, ex,
            new MessageTemplateParser().Parse("Sync failed"), Array.Empty<LogEventProperty>());

        var output = new StringWriter();
        new ScrubbingTextFormatter().Format(evt, output);

        output.ToString().Should().NotContain("InboxPass77");
    }

    [Fact]
    public async Task HrdLogDoesNotLogAnUploadCodeTheServiceEchoesBack()
    {
        const string uploadCode = "HRD-UPLOAD-CODE-9911";
        var logger = new CapturingLogger<HrdLogService>();
        var settings = new Mock<ISettingsService>();
        settings.Setup(s => s.GetSettingsAsync()).ReturnsAsync(new UserSettings
        {
            HrdLog = new HrdLogSettings { Enabled = true, UploadCode = uploadCode, Callsign = "N9BC" },
        });

        var handler = new StubHandler(HttpStatusCode.OK, $"<result>invalid code {uploadCode}</result>");
        var service = new HrdLogService(settings.Object, new HttpClient(handler), logger);

        await service.UploadQsoAsync(NewQso());

        logger.All.Should().NotContain(uploadCode);
        logger.All.Should().Contain("HTTP 200", "the response still has to be diagnosable");
    }

    [Fact]
    public async Task RedactionDoesNotChangeHowAResponseIsClassified()
    {
        // A password containing a marker word would, if masking ran before
        // classification, erase the very word the classifier looks for — a
        // rejected upload would be recorded as accepted.
        const string password = "invalid-cat-9";
        var logger = new CapturingLogger<HrdLogService>();
        var settings = new Mock<ISettingsService>();
        settings.Setup(s => s.GetSettingsAsync()).ReturnsAsync(new UserSettings
        {
            HrdLog = new HrdLogSettings { Enabled = true, UploadCode = password, Callsign = "N9BC" },
        });

        var handler = new StubHandler(HttpStatusCode.OK, $"<result>{password}</result>");
        var service = new HrdLogService(settings.Object, new HttpClient(handler), logger);

        var result = await service.UploadQsoAsync(NewQso());

        result.Ok.Should().BeFalse("the body says \"invalid\" — masking must not hide that");
        result.Error.Should().NotContain(password, "the stored ledger error is user-visible too");
    }

    [Fact]
    public async Task EqslDoesNotLogAPasswordTheServiceEchoesBack()
    {
        const string password = "eqsl-password-4242";
        var logger = new CapturingLogger<EqslService>();
        var settings = new Mock<ISettingsService>();
        settings.Setup(s => s.GetSettingsAsync()).ReturnsAsync(new UserSettings
        {
            Eqsl = new EqslSettings { Enabled = true, Username = "N9BC", Password = password },
        });

        var handler = new StubHandler(HttpStatusCode.OK, $"ERROR: Bad password {password}");
        var service = new EqslService(settings.Object, new HttpClient(handler), logger);

        await service.UploadQsoAsync(NewQso());

        logger.All.Should().NotContain(password);
    }

    [Fact]
    public async Task ClubLogDoesNotLogAPasswordTheServiceEchoesBack()
    {
        const string password = "clublog-password-7777";
        var logger = new CapturingLogger<ClubLogService>();
        var settings = new Mock<ISettingsService>();
        settings.Setup(s => s.GetSettingsAsync()).ReturnsAsync(new UserSettings
        {
            ClubLog = new ClubLogSettings
            {
                Enabled = true, ApiKey = "CLUBLOG-KEY-1", Email = "n9bc@example.com",
                Password = password, Callsign = "N9BC",
            },
        });

        var handler = new StubHandler(HttpStatusCode.OK, $"Login rejected for password {password}");
        var service = new ClubLogService(settings.Object, new HttpClient(handler), logger);

        await service.UploadQsoAsync(NewQso());

        logger.All.Should().NotContain(password);
    }

    [Fact]
    public async Task QrzDoesNotLogTheLogbookApiKeyQuotedBackInARejection()
    {
        // QRZ answers a bad key with RESULT=AUTH&REASON=<message containing the
        // key>, and that whole response was logged at Information level.
        const string apiKey = "QRZ-1234-ABCD-5678";
        var logger = new CapturingLogger<QrzService>();
        var repo = new Mock<ISettingsRepository>();
        repo.Setup(r => r.GetAsync()).ReturnsAsync(new UserSettings
        {
            Qrz = new QrzSettings { Enabled = true, ApiKey = apiKey },
        });

        var handler = new Mock<HttpMessageHandler>();
        handler.Protected()
            .Setup<Task<HttpResponseMessage>>("SendAsync", ItExpr.IsAny<HttpRequestMessage>(), ItExpr.IsAny<CancellationToken>())
            .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent($"RESULT=AUTH&REASON=invalid api key {apiKey}"),
            });
        var factory = new Mock<IHttpClientFactory>();
        factory.Setup(f => f.CreateClient("QRZ")).Returns(new HttpClient(handler.Object));

        var service = new QrzService(repo.Object, factory.Object, logger);

        await service.UploadQsoAsync(NewQso());

        logger.All.Should().NotContain(apiKey);
        logger.All.Should().Contain("RESULT=AUTH", "the failure reason must still be readable");
    }

    private static Qso NewQso() => new()
    {
        Id = Guid.NewGuid().ToString(),
        Callsign = "W1AW",
        QsoDate = new DateTime(2026, 7, 22),
        TimeOn = "1234",
        Band = "20m",
        Mode = "CW",
        Frequency = 14025.0,
    };

    private sealed class StubHandler : HttpMessageHandler
    {
        private readonly HttpStatusCode _status;
        private readonly string _body;
        public StubHandler(HttpStatusCode status, string body) { _status = status; _body = body; }
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct) =>
            Task.FromResult(new HttpResponseMessage(_status) { Content = new StringContent(_body) });
    }
}
