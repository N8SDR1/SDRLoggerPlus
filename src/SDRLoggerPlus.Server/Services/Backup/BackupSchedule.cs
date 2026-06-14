namespace SDRLoggerPlus.Server.Services.Backup;

/// <summary>
/// Due-time math for scheduled backups. Schedule anchors to the last
/// successful run (persisted across restarts), so a daily backup that
/// already fired today won't re-fire on app relaunch. No prior run →
/// due immediately. "on_exit" never fires from the timer.
/// </summary>
public static class BackupSchedule
{
    public static TimeSpan? IntervalOf(string interval) => interval switch
    {
        "on_exit" => null,
        "weekly" => TimeSpan.FromDays(7),
        _ => TimeSpan.FromDays(1),
    };

    public static DateTime? NextDue(string interval, DateTime? lastRunUtc)
    {
        var td = IntervalOf(interval);
        if (td is null || lastRunUtc is null) return null;
        return lastRunUtc.Value + td.Value;
    }

    public static bool IsDue(string interval, DateTime? lastRunUtc, DateTime nowUtc)
    {
        var td = IntervalOf(interval);
        if (td is null) return false;
        if (lastRunUtc is null) return true;
        return nowUtc - lastRunUtc.Value >= td.Value;
    }
}
