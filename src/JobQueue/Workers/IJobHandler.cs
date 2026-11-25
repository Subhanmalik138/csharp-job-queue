using JobQueue.Jobs;

namespace JobQueue.Workers;

/// <summary>
/// Runs one job. Return <c>true</c> when the work is done; return <c>false</c>
/// or throw to fail the attempt (the engine retries it, then dead-letters it).
/// Handlers must be safe to run concurrently and idempotent where possible —
/// a crash between "work done" and "job completed" means the job may run twice.
/// </summary>
public interface IJobHandler
{
    Task<bool> HandleAsync(Job job, CancellationToken cancellationToken);
}
