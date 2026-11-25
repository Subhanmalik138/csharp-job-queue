using System.Text.Json.Serialization;

namespace JobQueue.Jobs;

/// <summary>
/// A unit of work. A job is just a typed envelope: <see cref="Type"/> names the
/// handler that will run it and <see cref="Payload"/> is an opaque JSON document
/// the handler interprets. Everything else is scheduling metadata.
/// </summary>
public sealed class Job
{
    /// <summary>Stable id, e.g. <c>job_01J...</c>. Assigned by the engine if empty.</summary>
    public string Id { get; set; } = "";

    /// <summary>Handler name, e.g. <c>send-email</c> or <c>shell</c>.</summary>
    public string Type { get; set; } = "";

    /// <summary>Raw JSON payload handed to the handler. Defaults to <c>{}</c>.</summary>
    public string Payload { get; set; } = "{}";

    /// <summary>Higher runs first among jobs that are due at the same time.</summary>
    public int Priority { get; set; }

    public JobStatus Status { get; set; } = JobStatus.Pending;

    /// <summary>Attempts already spent (including the one in flight, if any).</summary>
    public int Attempts { get; set; }

    /// <summary>Maximum attempts before the job is dead-lettered. Default 3.</summary>
    public int MaxAttempts { get; set; } = 3;

    /// <summary>Earliest time the job may run. In the past (or null) means "now".</summary>
    public DateTimeOffset? RunAt { get; set; }

    /// <summary>When the next retry becomes due (set by the engine on failure).</summary>
    public DateTimeOffset? NextAttemptAt { get; set; }

    public DateTimeOffset CreatedAt { get; set; }

    public DateTimeOffset UpdatedAt { get; set; }

    /// <summary>Error message from the most recent failed attempt, if any.</summary>
    public string? LastError { get; set; }

    /// <summary>Id of the recurring job that produced this instance, if any.</summary>
    public string? RecurringJobId { get; set; }

    [JsonIgnore]
    public DateTimeOffset DueAt => NextAttemptAt ?? RunAt ?? CreatedAt;

    /// <summary>True when the job is due to run at <paramref name="now"/>.</summary>
    public bool IsDue(DateTimeOffset now) =>
        Status is JobStatus.Pending or JobStatus.Failed && DueAt <= now;
}
