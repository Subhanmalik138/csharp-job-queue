namespace JobQueue.Jobs;

/// <summary>Lifecycle of a queued job.</summary>
public enum JobStatus
{
    /// <summary>Waiting to run (includes jobs scheduled for a future time).</summary>
    Pending,
    /// <summary>Claimed by a worker and currently executing.</summary>
    Running,
    /// <summary>Finished successfully.</summary>
    Completed,
    /// <summary>Failed an attempt but still has retries left.</summary>
    Failed,
    /// <summary>Exhausted all attempts; parked for manual inspection/retry.</summary>
    DeadLettered
}
