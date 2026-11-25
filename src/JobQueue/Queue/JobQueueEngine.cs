using JobQueue.Jobs;
using JobQueue.Store;

namespace JobQueue.Queue;

/// <summary>
/// The in-memory heart of the queue. Jobs live durably in the <see cref="IJobStore"/>;
/// this class owns the scheduling policy on top of it:
///
/// <list type="bullet">
///   <item>Due jobs are claimed highest-<see cref="Job.Priority"/> first, then earliest due, then oldest.</item>
///   <item>A claimed job is marked <see cref="JobStatus.Running"/> in the store immediately, so two workers can never take the same job.</item>
///   <item>Failing a job schedules a retry with exponential backoff (plus jitter) until <see cref="Job.MaxAttempts"/> is spent, then dead-letters it.</item>
/// </list>
/// The clock and the random source are injectable so the policy is unit-testable
/// without sleeping or depending on wall-clock time.
/// </summary>
public sealed class JobQueueEngine
{
    private readonly IJobStore _store;
    private readonly Func<DateTimeOffset> _clock;
    private readonly Random _random;
    private readonly object _gate = new();

    /// <summary>Base delay before the first retry. Doubled per attempt, capped at <see cref="MaxBackoff"/>.</summary>
    public TimeSpan BaseBackoff { get; init; } = TimeSpan.FromSeconds(5);

    public TimeSpan MaxBackoff { get; init; } = TimeSpan.FromMinutes(5);

    /// <summary>Jitter applied to the computed backoff (± this fraction). Default 20%.</summary>
    public double JitterFraction { get; init; } = 0.2;

    public JobQueueEngine(
        IJobStore store,
        Func<DateTimeOffset>? clock = null,
        Random? random = null)
    {
        _store = store;
        _clock = clock ?? (() => DateTimeOffset.UtcNow);
        _random = random ?? Random.Shared;
    }

    /// <summary>Enqueue a new job, filling in id and timestamps. Returns the stored job.</summary>
    public Job Enqueue(Job job)
    {
        ArgumentNullException.ThrowIfNull(job);
        if (string.IsNullOrWhiteSpace(job.Type))
            throw new ArgumentException("Job type must not be empty.", nameof(job));
        if (job.MaxAttempts < 1)
            throw new ArgumentException("MaxAttempts must be at least 1.", nameof(job));

        var now = _clock();
        job.Id = string.IsNullOrWhiteSpace(job.Id) ? NewId() : job.Id;
        job.Status = JobStatus.Pending;
        job.Attempts = 0;
        job.CreatedAt = now;
        job.UpdatedAt = now;
        job.NextAttemptAt = null;
        job.LastError = null;
        _store.Upsert(job);
        return job;
    }

    /// <summary>Claim the highest-priority due job, or null when nothing is ready.</summary>
    public Job? TryClaimNext()
    {
        var now = _clock();
        lock (_gate)
        {
            var next = _store.All()
                .Where(j => j.IsDue(now))
                .OrderByDescending(j => j.Priority)
                .ThenBy(j => j.DueAt)
                .ThenBy(j => j.CreatedAt)
                .FirstOrDefault();
            if (next is null) return null;

            next.Status = JobStatus.Running;
            next.Attempts++;
            next.UpdatedAt = now;
            _store.Upsert(next);
            return next;
        }
    }

    public void Complete(string id)
    {
        var job = _store.Get(id) ?? throw new KeyNotFoundException($"No job with id '{id}'.");
        job.Status = JobStatus.Completed;
        job.UpdatedAt = _clock();
        _store.Upsert(job);
    }

    /// <summary>
    /// Record a failed attempt. Retries while attempts remain, scheduling the next
    /// attempt after the backoff; otherwise moves the job to the dead-letter state.
    /// Returns the updated job.
    /// </summary>
    public Job Fail(string id, string error)
    {
        var job = _store.Get(id) ?? throw new KeyNotFoundException($"No job with id '{id}'.");
        var now = _clock();
        job.LastError = error;
        job.UpdatedAt = now;
        if (job.Attempts < job.MaxAttempts)
        {
            job.Status = JobStatus.Failed;
            job.NextAttemptAt = now + ComputeBackoff(job.Attempts);
        }
        else
        {
            job.Status = JobStatus.DeadLettered;
            job.NextAttemptAt = null;
        }
        _store.Upsert(job);
        return job;
    }

    /// <summary>Move a dead-lettered or failed job back to pending for another run.</summary>
    public Job Retry(string id)
    {
        var job = _store.Get(id) ?? throw new KeyNotFoundException($"No job with id '{id}'.");
        if (job.Status is not (JobStatus.DeadLettered or JobStatus.Failed))
            throw new InvalidOperationException($"Job '{id}' is {job.Status} and cannot be retried.");

        var now = _clock();
        job.Status = JobStatus.Pending;
        job.Attempts = 0;
        job.NextAttemptAt = null;
        job.RunAt = null;
        job.LastError = null;
        job.UpdatedAt = now;
        _store.Upsert(job);
        return job;
    }

    /// <summary>
    /// Give up a Running job without counting the attempt — used when a worker
    /// is shut down mid-job. The job returns to Pending for the next run.
    /// </summary>
    public void Abandon(string id)
    {
        var job = _store.Get(id);
        if (job is not { Status: JobStatus.Running }) return;
        job.Status = JobStatus.Pending;
        job.Attempts = Math.Max(0, job.Attempts - 1);
        job.UpdatedAt = _clock();
        _store.Upsert(job);
    }

    public Job? Get(string id) => _store.Get(id);

    public IReadOnlyList<Job> List(JobStatus? status = null) =>
        _store.All()
            .Where(j => status is null || j.Status == status)
            .OrderByDescending(j => j.CreatedAt)
            .ToList();

    public (int Pending, int Running, int Completed, int Failed, int DeadLettered) Stats()
    {
        var all = _store.All();
        return (
            all.Count(j => j.Status == JobStatus.Pending),
            all.Count(j => j.Status == JobStatus.Running),
            all.Count(j => j.Status == JobStatus.Completed),
            all.Count(j => j.Status == JobStatus.Failed),
            all.Count(j => j.Status == JobStatus.DeadLettered));
    }

    internal TimeSpan ComputeBackoff(int attemptsSpent)
    {
        var exponent = Math.Min(attemptsSpent - 1, 10);
        var delay = BaseBackoff * Math.Pow(2, exponent);
        if (delay > MaxBackoff) delay = MaxBackoff;
        var jitter = delay * JitterFraction * (_random.NextDouble() * 2 - 1);
        var result = delay + jitter;
        return result < TimeSpan.Zero ? TimeSpan.Zero : result;
    }

    private static string NewId() =>
        "job_" + Guid.NewGuid().ToString("N")[..20];
}
