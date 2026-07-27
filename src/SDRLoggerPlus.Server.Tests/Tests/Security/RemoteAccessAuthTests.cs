using FluentAssertions;
using Microsoft.AspNetCore.Http;
using SDRLoggerPlus.Server.Core.Security;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Security;

/// <summary>
/// v1.0 remote-access auth: the backend stays zero-auth on a loopback desktop, refuses to
/// expose the API/radio-control to the network without a token, and gates /api + /hubs behind
/// a per-device bearer token when reachable remotely.
/// </summary>
[Trait("Category", "Unit")]
public class RemoteAccessGuardTests
{
    [Theory]
    [InlineData("http://localhost:5050", false)]
    [InlineData("http://127.0.0.1:5050", false)]
    [InlineData("http://[::1]:5050", false)]
    [InlineData("https://localhost:5050;http://127.0.0.1:5051", false)]
    [InlineData("http://0.0.0.0:5050", true)]
    [InlineData("http://192.168.1.50:5050", true)]
    [InlineData("https://myserver.example.com", true)]
    [InlineData("http://localhost:5050;http://0.0.0.0:5051", true)] // any remote leg ⇒ remote
    public void IsRemotelyBound_classifies(string urls, bool expected) =>
        RemoteAccessGuard.IsRemotelyBound(new[] { urls }).Should().Be(expected);

    [Fact]
    public void NoUrls_isNotRemote() =>
        RemoteAccessGuard.IsRemotelyBound(new string?[] { null, "" }).Should().BeFalse();

    [Fact]
    public void RemoteBind_withNoTokens_isRefused() =>
        RemoteAccessGuard.UnsafeBindReason(new[] { "http://0.0.0.0:5050" }, false, anyDevices: false)
            .Should().NotBeNull();

    [Fact]
    public void RemoteBind_withTokens_isAllowed() =>
        RemoteAccessGuard.UnsafeBindReason(new[] { "http://0.0.0.0:5050" }, false, anyDevices: true)
            .Should().BeNull();

    [Fact]
    public void LoopbackBind_isNeverRefused_andNotEnforced()
    {
        RemoteAccessGuard.UnsafeBindReason(new[] { "http://localhost:5050" }, false, anyDevices: false)
            .Should().BeNull();
        RemoteAccessGuard.ShouldEnforce(new[] { "http://localhost:5050" }, forceRequire: false)
            .Should().BeFalse();
    }

    [Fact]
    public void ForceRequire_enforcesAndRefusesEvenOnLoopback()
    {
        RemoteAccessGuard.ShouldEnforce(new[] { "http://localhost:5050" }, forceRequire: true).Should().BeTrue();
        RemoteAccessGuard.UnsafeBindReason(new[] { "http://localhost:5050" }, true, anyDevices: false)
            .Should().NotBeNull();
    }
}

[Trait("Category", "Unit")]
public class AuthTokenStoreTests : IDisposable
{
    private readonly string _path = Path.Combine(Path.GetTempPath(), $"sdrl-auth-{Guid.NewGuid():N}.json");
    public void Dispose() { if (File.Exists(_path)) File.Delete(_path); }

    [Fact]
    public void Issue_thenValidate_succeeds_andWrongTokenFails()
    {
        var store = new AuthTokenStore(_path);
        store.AnyDevices.Should().BeFalse();

        var (token, dev) = store.Issue("my phone");

        token.Should().StartWith("sdrl_");
        dev.Name.Should().Be("my phone");
        store.AnyDevices.Should().BeTrue();
        store.Validate(token).Should().BeTrue();
        store.Validate("sdrl_not-the-token").Should().BeFalse();
        store.Validate(null).Should().BeFalse();
        store.Validate("").Should().BeFalse();
    }

    [Fact]
    public void Token_isNeverStoredInReadableForm()
    {
        var store = new AuthTokenStore(_path);
        var (token, _) = store.Issue("laptop");

        var fileText = File.ReadAllText(_path);
        fileText.Should().NotContain(token);
        fileText.Should().NotContain("sdrl_"); // only the hash is persisted, no token prefix
    }

    [Fact]
    public void Revoke_invalidatesOnlyThatDevice()
    {
        var store = new AuthTokenStore(_path);
        var (t1, d1) = store.Issue("phone");
        var (t2, _) = store.Issue("laptop");

        store.Revoke(d1.Id).Should().BeTrue();
        store.Validate(t1).Should().BeFalse();
        store.Validate(t2).Should().BeTrue();
        store.Revoke("nonexistent").Should().BeFalse();
    }

    [Fact]
    public void Tokens_persistAcrossReload()
    {
        var (token, _) = new AuthTokenStore(_path).Issue("desktop");
        new AuthTokenStore(_path).Validate(token).Should().BeTrue(); // fresh instance, same file
    }
}

[Trait("Category", "Unit")]
public class TokenAuthMiddlewareTests : IDisposable
{
    private readonly string _path = Path.Combine(Path.GetTempPath(), $"sdrl-auth-{Guid.NewGuid():N}.json");
    public void Dispose() { if (File.Exists(_path)) File.Delete(_path); }

    [Theory]
    [InlineData("/api/qsos", true)]
    [InlineData("/api/settings", true)]
    [InlineData("/hubs/log", true)]
    [InlineData("/api/health", false)]
    [InlineData("/", false)]
    [InlineData("/index.html", false)]
    [InlineData("/assets/app.js", false)]
    public void RequiresAuth_gatesApiAndHubsOnly(string path, bool gated) =>
        TokenAuthMiddleware.RequiresAuth(new PathString(path)).Should().Be(gated);

    private static DefaultHttpContext Ctx(string path, string? bearer = null, string? accessToken = null)
    {
        var ctx = new DefaultHttpContext();
        ctx.Request.Path = path;
        if (bearer != null) ctx.Request.Headers.Authorization = $"Bearer {bearer}";
        if (accessToken != null) ctx.Request.QueryString = new QueryString($"?access_token={accessToken}");
        ctx.Response.Body = new MemoryStream();
        return ctx;
    }

    private static TokenAuthMiddleware Mw(AuthTokenStore store, bool enforce, Action onNext) =>
        new(_ => { onNext(); return Task.CompletedTask; }, store, new AuthOptions(enforce));

    [Fact]
    public async Task Enforced_apiWithoutToken_is401_andBlocks()
    {
        var store = new AuthTokenStore(_path);
        store.Issue("d");
        var called = false;
        var ctx = Ctx("/api/qsos");
        await Mw(store, enforce: true, () => called = true).Invoke(ctx);

        ctx.Response.StatusCode.Should().Be(StatusCodes.Status401Unauthorized);
        called.Should().BeFalse();
    }

    [Fact]
    public async Task Enforced_apiWithValidBearer_passes()
    {
        var store = new AuthTokenStore(_path);
        var (token, _) = store.Issue("d");
        var called = false;
        await Mw(store, enforce: true, () => called = true).Invoke(Ctx("/api/qsos", bearer: token));
        called.Should().BeTrue();
    }

    [Fact]
    public async Task Enforced_hubWithAccessTokenQuery_passes()
    {
        var store = new AuthTokenStore(_path);
        var (token, _) = store.Issue("d");
        var called = false;
        await Mw(store, enforce: true, () => called = true).Invoke(Ctx("/hubs/log", accessToken: token));
        called.Should().BeTrue();
    }

    [Fact]
    public async Task Enforced_healthAndStaticAreExempt()
    {
        var store = new AuthTokenStore(_path);
        store.Issue("d");
        var health = false; var root = false;
        await Mw(store, enforce: true, () => health = true).Invoke(Ctx("/api/health"));
        await Mw(store, enforce: true, () => root = true).Invoke(Ctx("/index.html"));
        health.Should().BeTrue();
        root.Should().BeTrue();
    }

    [Fact]
    public async Task NotEnforced_passesEverythingThrough()
    {
        var store = new AuthTokenStore(_path); // loopback desktop: no tokens, not enforced
        var called = false;
        await Mw(store, enforce: false, () => called = true).Invoke(Ctx("/api/qsos"));
        called.Should().BeTrue();
    }
}
