using System.Net;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Services;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

[Trait("Category", "Unit")]
public class ClubLogServiceTests
{
    private sealed class FakeHandler : HttpMessageHandler
    {
        public HttpStatusCode Status = HttpStatusCode.OK;
        public string Body = "OK";
        public int Calls;
        public Dictionary<string, string>? LastForm;

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            Calls++;
            if (request.Content != null)
            {
                var text = await request.Content.ReadAsStringAsync(ct);
                LastForm = text.Split('&')
                    .Select(p => p.Split('=', 2))
                    .ToDictionary(p => Uri.UnescapeDataString(p[0]), p => Uri.UnescapeDataString(p.Length > 1 ? p[1] : ""));
            }
            return new HttpResponseMessage(Status) { Content = new StringContent(Body) };
        }
    }

    private readonly FakeHandler _handler = new();
    private readonly Mock<ISettingsService> _settings = new();
    private readonly UserSettings _userSettings = new();

    private ClubLogService CreateService()
    {
        _settings.Setup(s => s.GetSettingsAsync(It.IsAny<string>())).ReturnsAsync(_userSettings);
        return new ClubLogService(_settings.Object, new HttpClient(_handler), NullLogger<ClubLogService>.Instance);
    }

    private void ConfigureEnabled()
    {
        _userSettings.ClubLog.Enabled = true;
        _userSettings.ClubLog.Email = "op@example.com";
        _userSettings.ClubLog.Password = "secret";
        _userSettings.ClubLog.ApiKey = "appkey123";
        _userSettings.ClubLog.Callsign = "N9BC";
    }

    private static Qso MakeQso() => new()
    {
        Id = "1",
        Callsign = "K5XYZ",
        QsoDate = new DateTime(2026, 6, 11, 0, 0, 0, DateTimeKind.Utc),
        TimeOn = "18:30",
        Band = "20m",
        Mode = "SSB",
        Frequency = 14.250,
        RstSent = "59",
        RstRcvd = "57",
    };

    [Fact]
    public void BuildAdif_ProducesSingleRecordWithRequiredFields()
    {
        var adif = ClubLogService.BuildAdif(MakeQso(), "N9BC");
        adif.Should().Contain("<CALL:5>K5XYZ");
        adif.Should().Contain("<STATION_CALLSIGN:4>N9BC");
        adif.Should().Contain("<QSO_DATE:8>20260611");
        adif.Should().Contain("<TIME_ON:4>1830");
        adif.Should().Contain("<BAND:3>20m");
        adif.Should().Contain("<MODE:3>SSB");
        adif.Should().Contain("<RST_SENT:2>59");
        adif.Should().EndWith("<EOR>");
    }

    [Fact]
    public async Task Upload_Disabled_DoesNotCallHttp()
    {
        var result = await CreateService().UploadQsoAsync(MakeQso());
        var ok = result.Ok;
        ok.Should().BeFalse();
        _handler.Calls.Should().Be(0);
    }

    [Fact]
    public async Task Upload_Success_PostsFormToRealtimeEndpoint()
    {
        ConfigureEnabled();
        _handler.Body = "OK";

        var result = await CreateService().UploadQsoAsync(MakeQso());
        var (ok, error) = (result.Ok, result.Error);

        ok.Should().BeTrue();
        error.Should().BeNull();
        _handler.LastForm.Should().NotBeNull();
        _handler.LastForm!["api"].Should().Be("appkey123");
        _handler.LastForm["email"].Should().Be("op@example.com");
        _handler.LastForm["callsign"].Should().Be("N9BC");
        _handler.LastForm["adif"].Should().Contain("<CALL:5>K5XYZ");
    }

    [Theory]
    [InlineData("Dupe")]
    [InlineData("Updated QSO")]
    public async Task Upload_DupeOrUpdated_CountsAsSuccess(string body)
    {
        ConfigureEnabled();
        _handler.Body = body;
        var result = await CreateService().UploadQsoAsync(MakeQso());
        var ok = result.Ok;
        ok.Should().BeTrue();
    }

    [Fact]
    public async Task Upload_403_BlocksAllFurtherUploads()
    {
        ConfigureEnabled();
        _handler.Status = HttpStatusCode.Forbidden;

        var service = CreateService();
        var result = await service.UploadQsoAsync(MakeQso());
        var (ok, error) = (result.Ok, result.Error);
        ok.Should().BeFalse();
        error.Should().Contain("disabled");

        // One-strike rule: the next upload must NOT hit HTTP at all
        _handler.Status = HttpStatusCode.OK;
        var before = _handler.Calls;
        var result2 = await service.UploadQsoAsync(MakeQso());
        var ok2 = result2.Ok;
        ok2.Should().BeFalse();
        _handler.Calls.Should().Be(before);
    }

    [Fact]
    public async Task ResetBlock_AllowsUploadsAgain()
    {
        ConfigureEnabled();
        _handler.Status = HttpStatusCode.Forbidden;
        var service = CreateService();
        await service.UploadQsoAsync(MakeQso());

        service.ResetBlock();
        _handler.Status = HttpStatusCode.OK;
        _handler.Body = "OK";
        var result = await service.UploadQsoAsync(MakeQso());
        var ok = result.Ok;
        ok.Should().BeTrue();
    }

    [Fact]
    public async Task Upload_400_ReturnsRejectionWithoutBlocking()
    {
        ConfigureEnabled();
        _handler.Status = HttpStatusCode.BadRequest;
        _handler.Body = "Bad ADIF";

        var service = CreateService();
        var result = await service.UploadQsoAsync(MakeQso());
        var (ok, error) = (result.Ok, result.Error);
        ok.Should().BeFalse();
        error.Should().Contain("rejected");

        _handler.Status = HttpStatusCode.OK;
        _handler.Body = "OK";
        var result2 = await service.UploadQsoAsync(MakeQso());
        var ok2 = result2.Ok;
        ok2.Should().BeTrue(); // not blocked
    }
}
