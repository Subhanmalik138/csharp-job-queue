namespace JobQueue.Scheduling;

/// <summary>
/// A job template that fires on a fixed interval, e.g. "every 60 seconds".
/// Each firing enqueues a fresh <see cref="Jobs.Job"/> stamped with
/// <see cref="Jobs.Job.RecurringJobId"/>. The interval is fixed (not cron) on
/// purpose: it keeps scheduling a one-line concept with no expression language
/// to parse — a cron parser is a separate project.
/// </summary>
public sealed class RecurringJob
{
    public string Id { get; set; } = "";

    /// <summary>Human name, e.g. <c>nightly-report</c>. Must be unique.</summary>
    public string Name { get; set; } = "";

    /// <summary>Handler name the generated jobs carry.</summary>
    public string Type { get; set; } = "";

    public string Payload { get; set; } = "{}";

    public int Priority { get; set; }

    public int MaxAttempts { get; set; } = 3;

    /// <summary>Seconds between firings. Minimum 1.</summary>
    public int IntervalSeconds { get; set; }

    /// <summary>Next scheduled firing.</summary>
    public DateTimeOffset NextRunAt { get; set; }

    public DateTimeOffset CreatedAt { get; set; }

    public bool Enabled { get; set; } = true;
}
