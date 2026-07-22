using System.Reflection;
using System.Text.RegularExpressions;
using FluentAssertions;
using SDRLoggerPlus.Contracts.Models;
using SDRLoggerPlus.Server.Core.Security;
using Xunit;

namespace SDRLoggerPlus.Server.Tests.Tests.Security;

/// <summary>
/// Guards the credential list against the failure mode that matters: someone
/// adds a password to Settings.cs, does not add it to SettingsSecrets, and it
/// is stored in plaintext forever with nothing failing.
///
/// The scan walks the whole UserSettings object graph looking for
/// secret-shaped string properties, then checks each one is actually visited
/// by SettingsSecrets.Transform. It found four fields the first time it ran
/// (the Ambient/Ecowitt *AppKey* pairs, which a hand-written "ApiKey" list
/// had missed).
/// </summary>
[Trait("Category", "Unit")]
public class SettingsSecretsCoverageTests
{
    // Names that mean "this is a credential". Deliberately broad — a false
    // positive costs one line in the allow-list below and a moment's thought;
    // a false negative is a plaintext password shipped to users.
    private static readonly Regex SecretName =
        new(@"pass|secret|token|credential|(api|app|upload|access|private)_?(key|code)",
            RegexOptions.IgnoreCase);

    /// <summary>
    /// Properties whose names trip the scanner but are not credentials.
    /// Every entry needs a reason.
    /// </summary>
    private static readonly HashSet<string> NotSecrets = new()
    {
        // (empty — add "Type.Property  // why" entries here as they arise)
    };

    private static IEnumerable<string> FindSecretProperties(Type type, string path, HashSet<Type> seen)
    {
        if (!seen.Add(type)) yield break;

        foreach (var property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            var propertyType = property.PropertyType;
            var here = $"{path}.{property.Name}";

            if (propertyType == typeof(string))
            {
                if (SecretName.IsMatch(property.Name) &&
                    !NotSecrets.Contains($"{type.Name}.{property.Name}"))
                {
                    yield return here;
                }
                continue;
            }

            // Walk nested settings objects and collections of them.
            var elementType = propertyType.IsGenericType &&
                              typeof(System.Collections.IEnumerable).IsAssignableFrom(propertyType)
                ? propertyType.GetGenericArguments().FirstOrDefault()
                : propertyType;

            if (elementType is { IsClass: true } &&
                elementType.Namespace?.StartsWith("SDRLoggerPlus.Contracts") == true)
            {
                foreach (var nested in FindSecretProperties(elementType, here, seen))
                    yield return nested;
            }
        }
    }

    [Fact]
    public void EverySecretShapedPropertyIsCoveredBySettingsSecrets()
    {
        var discovered = FindSecretProperties(typeof(UserSettings), "UserSettings", new HashSet<Type>())
            .ToList();

        discovered.Should().NotBeEmpty("the scanner itself must be working");

        // Count what Transform actually visits, including cluster passwords.
        var settings = new UserSettings();
        settings.Cluster.Connections.Add(new ClusterConnection());
        var visited = 0;
        SettingsSecrets.Transform(settings, value => { visited++; return value; });

        visited.Should().Be(discovered.Count,
            "SettingsSecrets must cover exactly the credential fields the settings model " +
            "declares. Discovered: " + string.Join(", ", discovered));
    }

    [Fact]
    public void TransformReachesEveryDeclaredFieldAndWritesBack()
    {
        var settings = new UserSettings();
        foreach (var field in SettingsSecrets.Fields) field.Set(settings, "plain");
        settings.Cluster.Connections.Add(new ClusterConnection { Password = "plain" });

        SettingsSecrets.Transform(settings, v => v == null ? null : v + "!");

        foreach (var field in SettingsSecrets.Fields)
        {
            field.Get(settings).Should().Be("plain!", $"{field.Name} must be transformed");
        }
        settings.Cluster.Connections[0].Password.Should().Be("plain!");
    }

    [Fact]
    public void FieldNamesAreUnique()
    {
        SettingsSecrets.Fields.Select(f => f.Name).Should().OnlyHaveUniqueItems();
    }

    [Fact]
    public void TransformHandlesAnEmptyConnectionList()
    {
        var settings = new UserSettings();

        var act = () => SettingsSecrets.Transform(settings, v => v);

        act.Should().NotThrow();
    }
}
