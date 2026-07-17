using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using SDRLoggerPlus.Contracts.Models.Contesting;
using SDRLoggerPlus.Server.Services;
using SDRLoggerPlus.Server.Services.Contesting;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

[Trait("Category", "Unit")]
public class ContestDefinitionServiceTests : IDisposable
{
    private readonly string _tempDir;
    private readonly ContestDefinitionService _service;

    public ContestDefinitionServiceTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "sdrlp-contest-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);

        var cfg = new Mock<IUserConfigService>();
        cfg.Setup(c => c.GetConfigPath()).Returns(Path.Combine(_tempDir, "config.json"));
        _service = new ContestDefinitionService(cfg.Object, NullLogger<ContestDefinitionService>.Instance);
    }

    public void Dispose()
    {
        try { Directory.Delete(_tempDir, recursive: true); } catch { /* best effort */ }
    }

    [Fact]
    public void GetAll_IncludesBuiltinSeeds()
    {
        var all = _service.GetAll();
        all.Should().Contain(d => d.Id == "cq-ww-cw" && d.Builtin);
        all.Should().Contain(d => d.Id == "generic-serial" && d.Builtin);
    }

    [Fact]
    public void AllSeedDefinitions_AreValid()
    {
        foreach (var def in SeedContests.All)
            ContestDefinitionService.Validate(def).Should().BeEmpty($"seed '{def.Id}' should be valid");
    }

    [Fact]
    public void Delete_Builtin_Throws()
    {
        var act = () => _service.Delete("cq-ww-cw");
        act.Should().Throw<ContestDefinitionException>().WithMessage("*cannot be deleted*");
    }

    [Fact]
    public void Save_ThenDelete_RoundTripsUserDefinition()
    {
        var def = new ContestDefinition
        {
            Name = "My Sprint",
            Bands = { "20M", "40M" },
            Modes = { "CW" },
            RcvdExchange = { new ContestField { Key = "serial", Label = "Nr", Type = ContestFieldType.Serial } },
        };

        var saved = _service.Save(def);
        saved.Builtin.Should().BeFalse();
        saved.Id.Should().Be("my-sprint");
        File.Exists(Path.Combine(_tempDir, "contests", "my-sprint.json")).Should().BeTrue();
        _service.Get("my-sprint").Should().NotBeNull();

        _service.Delete("my-sprint");
        _service.Get("my-sprint").Should().BeNull();
        File.Exists(Path.Combine(_tempDir, "contests", "my-sprint.json")).Should().BeFalse();
    }

    [Fact]
    public void Save_IdConflictingWithBuiltin_Throws()
    {
        var def = new ContestDefinition
        {
            Name = "CQ WW CW", // slugs to cq-ww-cw, a built-in id
            Bands = { "20M" },
            Modes = { "CW" },
            RcvdExchange = { new ContestField { Key = "zone", Type = ContestFieldType.Zone } },
        };
        var act = () => _service.Save(def);
        act.Should().Throw<ContestDefinitionException>().WithMessage("*conflicts with a built-in*");
    }

    [Fact]
    public void Validate_EmptyReceivedExchange_ReportsError()
    {
        var def = new ContestDefinition { Name = "X", Bands = { "20M" }, Modes = { "CW" } };
        ContestDefinitionService.Validate(def).Should().Contain(e => e.Contains("received-exchange"));
    }

    [Fact]
    public void CloneAsDraft_ProducesEditableUserCopy()
    {
        var draft = _service.CloneAsDraft("cq-ww-cw", "My WW");
        draft.Builtin.Should().BeFalse();
        draft.Id.Should().Be("my-ww");
        draft.Name.Should().Be("My WW");
        draft.RcvdExchange.Should().NotBeEmpty(); // carried over from the source
    }
}
