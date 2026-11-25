using JobQueue.Jobs;

namespace JobQueue.Store;

/// <summary>Durable storage for jobs. All operations are synchronous and atomic
/// from the caller's point of view; the implementation decides how to persist.</summary>
public interface IJobStore : IDisposable
{
    /// <summary>Insert or replace a job.</summary>
    void Upsert(Job job);

    /// <summary>Remove a job. No-op when the id does not exist.</summary>
    void Delete(string id);

    Job? Get(string id);

    /// <summary>All jobs, in no particular order.</summary>
    IReadOnlyList<Job> All();

    /// <summary>Rewrite storage compactly (e.g. fold the journal into a snapshot).</summary>
    void Compact();
}
