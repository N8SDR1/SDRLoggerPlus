using System.Text.Json;
using SDRLoggerPlus.Contracts.Models.Contesting;

namespace SDRLoggerPlus.Server.Services.Contesting;

/// <summary>
/// Loads contest definitions (built-in seeds + user JSON files under
/// %APPDATA%\SDRLoggerPlus\contests\), validates them, and provides create /
/// clone / update / delete. Built-ins are read-only and never deletable.
/// </summary>
public class ContestDefinitionService
{
    private readonly ILogger<ContestDefinitionService> _logger;
    private readonly string _contestsDir;
    private readonly Dictionary<string, ContestDefinition> _userDefs = new(StringComparer.OrdinalIgnoreCase);
    private readonly object _lock = new();

    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true,
    };

    public ContestDefinitionService(IUserConfigService userConfig, ILogger<ContestDefinitionService> logger)
    {
        _logger = logger;
        var configDir = Path.GetDirectoryName(userConfig.GetConfigPath()) ?? ".";
        _contestsDir = Path.Combine(configDir, "contests");
        LoadUserDefs();
    }

    /// <summary>All definitions: built-in seeds followed by user definitions.</summary>
    public IReadOnlyList<ContestDefinition> GetAll()
    {
        lock (_lock)
        {
            return SeedContests.All.Concat(_userDefs.Values)
                .OrderByDescending(d => d.Builtin)
                .ThenBy(d => d.Name, StringComparer.OrdinalIgnoreCase)
                .ToList();
        }
    }

    public ContestDefinition? Get(string id)
    {
        var seed = SeedContests.All.FirstOrDefault(d => string.Equals(d.Id, id, StringComparison.OrdinalIgnoreCase));
        if (seed != null) return seed;
        lock (_lock)
        {
            return _userDefs.TryGetValue(id, out var d) ? d : null;
        }
    }

    /// <summary>Create or update a user definition. Always stored as builtin: false.</summary>
    public ContestDefinition Save(ContestDefinition def)
    {
        def.Builtin = false;
        if (string.IsNullOrWhiteSpace(def.Id))
            def.Id = Slug(def.Name);

        var errors = Validate(def);
        if (errors.Count > 0)
            throw new ContestDefinitionException(string.Join("; ", errors));

        if (SeedContests.All.Any(d => string.Equals(d.Id, def.Id, StringComparison.OrdinalIgnoreCase)))
            throw new ContestDefinitionException($"Id '{def.Id}' conflicts with a built-in contest; choose another name.");

        Directory.CreateDirectory(_contestsDir);
        var path = Path.Combine(_contestsDir, def.Id + ".json");
        File.WriteAllText(path, JsonSerializer.Serialize(def, JsonOpts));

        lock (_lock) { _userDefs[def.Id] = def; }
        _logger.LogInformation("Saved user contest definition {Id}", def.Id);
        return def;
    }

    /// <summary>Delete a user definition. Rejects built-ins.</summary>
    public void Delete(string id)
    {
        if (SeedContests.All.Any(d => string.Equals(d.Id, id, StringComparison.OrdinalIgnoreCase)))
            throw new ContestDefinitionException("Built-in contests cannot be deleted (clone it instead).");

        lock (_lock)
        {
            if (!_userDefs.Remove(id))
                throw new ContestDefinitionException($"No user contest with id '{id}'.");
        }

        var path = Path.Combine(_contestsDir, id + ".json");
        if (File.Exists(path)) File.Delete(path);
        _logger.LogInformation("Deleted user contest definition {Id}", id);
    }

    /// <summary>Return a deep-ish clone of a definition as an editable user draft (builtin: false, new id).</summary>
    public ContestDefinition CloneAsDraft(string sourceId, string newName)
    {
        var src = Get(sourceId) ?? throw new ContestDefinitionException($"Unknown contest '{sourceId}'.");
        var json = JsonSerializer.Serialize(src, JsonOpts);
        var copy = JsonSerializer.Deserialize<ContestDefinition>(json, JsonOpts)!;
        copy.Builtin = false;
        copy.Name = newName;
        copy.Id = Slug(newName);
        return copy;
    }

    /// <summary>Validate a definition; returns human-readable errors (empty = valid).</summary>
    public static List<string> Validate(ContestDefinition def)
    {
        var errors = new List<string>();
        if (string.IsNullOrWhiteSpace(def.Name))
            errors.Add("Name is required.");
        if (def.Bands.Count == 0)
            errors.Add("At least one band is required.");
        if (def.Modes.Count == 0)
            errors.Add("At least one mode is required.");
        if (def.RcvdExchange.Count == 0)
            errors.Add("At least one received-exchange field is required.");
        foreach (var f in def.RcvdExchange)
            if (string.IsNullOrWhiteSpace(f.Key))
                errors.Add("Every exchange field needs a key.");
        // Points rule is always resolvable (Default guarantees a value); mult
        // sources are enum-constrained so cannot be unknown. Cabrillo map is
        // validated separately when exporting.
        return errors;
    }

    private void LoadUserDefs()
    {
        try
        {
            if (!Directory.Exists(_contestsDir)) return;
            foreach (var file in Directory.EnumerateFiles(_contestsDir, "*.json"))
            {
                try
                {
                    var def = JsonSerializer.Deserialize<ContestDefinition>(File.ReadAllText(file), JsonOpts);
                    if (def == null || string.IsNullOrWhiteSpace(def.Id)) continue;
                    def.Builtin = false;
                    _userDefs[def.Id] = def;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Skipping malformed contest definition {File}", file);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to load user contest definitions from {Dir}", _contestsDir);
        }
    }

    private static string Slug(string name)
    {
        var s = new string((name ?? string.Empty).ToLowerInvariant()
            .Select(c => char.IsLetterOrDigit(c) ? c : '-').ToArray());
        while (s.Contains("--")) s = s.Replace("--", "-");
        s = s.Trim('-');
        return string.IsNullOrEmpty(s) ? "contest-" + Guid.NewGuid().ToString("N")[..8] : s;
    }
}

/// <summary>Thrown for invalid or disallowed contest-definition operations (maps to HTTP 400).</summary>
public class ContestDefinitionException : Exception
{
    public ContestDefinitionException(string message) : base(message) { }
}
