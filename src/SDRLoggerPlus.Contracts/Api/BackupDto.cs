namespace SDRLoggerPlus.Contracts.Api;

public record BackupStatusDto(
    bool Enabled,
    string Interval,
    int Retention,
    string Destination,
    DateTime? LastRunUtc,
    bool? Ok,
    string? Message,
    string? Path,
    DateTime? NextDueUtc);

public record BackupRunResult(bool Ok, string Message, string? Path);
