using System.Text;
using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging;
using Moq;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Database;
using SDRLoggerPlus.Server.Hubs;
using SDRLoggerPlus.Server.Services;
using SDRLoggerPlus.Server.Services.Adif;
using Xunit;
using Xunit.Abstractions;

namespace SDRLoggerPlus.Server.Tests.Manual;

/// <summary>
/// Points the importer at a real ADIF file and reports what it finds, without writing
/// anything anywhere. Set ADIF_AUDIT_FILE to a path to run it:
///
///   ADIF_AUDIT_FILE=C:\path\to\log.adi dotnet test --filter "FullyQualifiedName~AdifRealFileAudit"
///
/// Skips silently when the variable is unset, so it costs nothing in CI. Exists because
/// synthetic fixtures cannot show you how badly a decade-old log is actually mangled.
/// </summary>
[Trait("Category", "Manual")]
public class AdifRealFileAuditTest
{
    private readonly ITestOutputHelper _out;

    public AdifRealFileAuditTest(ITestOutputHelper output) => _out = output;

    [Fact]
    public void AuditRealAdifFile()
    {
        var path = Environment.GetEnvironmentVariable("ADIF_AUDIT_FILE");
        if (string.IsNullOrWhiteSpace(path) || !File.Exists(path))
        {
            _out.WriteLine("ADIF_AUDIT_FILE not set or not found — skipping.");
            return;
        }

        var service = new AdifService(
            new Mock<IQsoRepository>().Object,
            new Mock<ISettingsRepository>().Object,
            new Mock<IHubContext<LogHub, ILogHubClient>>().Object,
            new Mock<ILogger<AdifService>>().Object);

        var issues = new List<AdifFieldIssue>();
        var qsos = service.ParseAdif(File.ReadAllText(path), issues).ToList();

        _out.WriteLine($"file    : {path}");
        _out.WriteLine($"parsed  : {qsos.Count:N0} records");
        _out.WriteLine("");

        _out.WriteLine("=== FIELD ISSUES (what the old importer accepted silently) ===");
        foreach (var g in issues
                     .GroupBy(i => (i.Field, i.Original, i.Result, i.Action))
                     .OrderByDescending(g => g.Count()))
        {
            _out.WriteLine($"{g.Count(),7:N0}  {g.Key.Field,-5} '{g.Key.Original}' -> '{g.Key.Result}'  [{g.Key.Action}]");
        }
        _out.WriteLine("");

        // The whole point: how many records the two key designs consider distinct.
        // Old key used the raw band and mode; new key normalises band and drops mode.
        string OldKey(Qso q) => $"{q.Callsign}|{q.QsoDate.Date:yyyyMMdd}|{q.TimeOn}|{q.Band}|{q.Mode}";
        string NewKey(Qso q) => $"{q.Callsign}|{q.QsoDate.Date:yyyyMMdd}|{q.TimeOn}|{AdifFieldNormalizer.CanonicalBandKey(q.Band)}";

        var oldDistinct = qsos.Select(OldKey).Distinct().Count();
        var newDistinct = qsos.Select(NewKey).Distinct().Count();

        _out.WriteLine("=== DUPLICATE DETECTION ===");
        _out.WriteLine($"records in file            : {qsos.Count,7:N0}");
        _out.WriteLine($"distinct under OLD key     : {oldDistinct,7:N0}  (call+date+time+band+mode, raw)");
        _out.WriteLine($"distinct under NEW key     : {newDistinct,7:N0}  (call+date+time+normalised band)");
        _out.WriteLine($"duplicates the OLD key MISSED: {oldDistinct - newDistinct,5:N0}");
        _out.WriteLine("");

        _out.WriteLine("=== WORST OFFENDERS (same contact, disagreeing modes) ===");
        var collisions = qsos
            .GroupBy(NewKey)
            .Where(g => g.Select(q => q.Mode).Distinct(StringComparer.OrdinalIgnoreCase).Count() > 1)
            .GroupBy(g => string.Join(" + ", g.Select(q => q.Mode).Distinct(StringComparer.OrdinalIgnoreCase).OrderBy(m => m)))
            .OrderByDescending(g => g.Count());

        foreach (var c in collisions.Take(15))
        {
            _out.WriteLine($"{c.Count(),7:N0}  {c.Key}");
        }
    }
}
