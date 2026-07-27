using FluentAssertions;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Contracts.Models.Contesting;
using SDRLoggerPlus.Server.Services.Contesting;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Services;

/// <summary>
/// The class-eliminating guard for contest exchange wiring. Every REQUIRED sent-exchange field
/// declared by any seeded contest (in every role) must resolve to a NON-BLANK Cabrillo token when
/// MyExchange is populated exactly the way the entry UI can populate it. This fails the instant a
/// field is declared that the operator can't set / that CabrilloExporter can't emit — the exact
/// bug that shipped as Field Day's blank class and Sweepstakes' blank precedence/check.
///
/// Critical detail: this must NOT populate MyExchange by blindly iterating each definition's own
/// keys, or it would mask a missing-plumbing gap. It fills only the concrete members the UI exposes
/// (typed properties) plus the generic SentFields store for Text fields (which the UI's generic
/// input covers) — mirroring real operator capability.
/// </summary>
public class CabrilloExchangeWiringTests
{
    [Fact]
    [Trait("Category", "Unit")]
    public void EveryRequiredSentField_EmitsNonBlankToken()
    {
        var qso = new Qso
        {
            Callsign = "W1AW",
            Mode = "SSB",
            Band = "20m",
            RstSent = "59",
            Contest = new ContestInfo { SerialSent = "001" },
        };

        var failures = new List<string>();

        foreach (var def in SeedContests.All)
        {
            foreach (var (roleLabel, sent) in SentExchangesOf(def))
            {
                var me = UiPopulatedMyExchange(sent);
                foreach (var f in sent.Where(f => f.Required))
                {
                    var token = CabrilloExporter.SentValue(f, me, qso);
                    if (string.IsNullOrWhiteSpace(token))
                        failures.Add($"{def.Id} [{roleLabel}] sent field '{f.Key}' ({f.Type}) -> BLANK");
                }
            }
        }

        failures.Should().BeEmpty(
            "every required sent exchange field must be enterable + emitted; blanks below are unwired fields:\n" +
            string.Join("\n", failures));
    }

    /// <summary>Top-level sent exchange plus each role's sent exchange (falling back to top-level).</summary>
    private static IEnumerable<(string RoleLabel, List<ContestField> Sent)> SentExchangesOf(ContestDefinition def)
    {
        yield return ("base", def.SentExchange);
        if (def.Roles != null)
            foreach (var (role, rules) in def.Roles)
                yield return (role.ToString(), rules.SentExchange ?? def.SentExchange);
    }

    /// <summary>
    /// A MyExchange filled the way the entry UI can fill it: every typed property, plus the generic
    /// SentFields store for each Text field the UI renders a generic input for (all Text keys except
    /// class/county, which have dedicated inputs). A field with no such path stays blank -> fails.
    /// </summary>
    private static MyExchange UiPopulatedMyExchange(IEnumerable<ContestField> sent)
    {
        var me = new MyExchange
        {
            CqZone = 5,
            ItuZone = 8,
            State = "OH",
            Section = "OH",
            Grid = "EM79",
            Name = "RICK",
            Power = "100",
            Class = "1E",
            County = "FRANKLIN",
            Continent = "NA",
            SentFields = new Dictionary<string, string>(),
        };

        foreach (var f in sent)
        {
            var key = f.Key.ToLowerInvariant();
            if (f.Type == ContestFieldType.Text && key != "class" && key != "county")
                me.SentFields[f.Key] = "X";
        }

        return me;
    }
}
