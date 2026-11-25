using JobQueue.Jobs;
using JobQueue.Store;
using Xunit;

namespace JobQueue.Tests;

public sealed class StoreTests
{
    [Fact]
    public void RoundTrip_UpsertGetAllDelete()
    {
        using var fixture = new TestStore();
        using var store = fixture.Store;

        var job = new Job { Id = "job_1", Type = "shell", Payload = "{}", CreatedAt = DateTimeOffset.UtcNow, UpdatedAt = DateTimeOffset.UtcNow };
        store.Upsert(job);
        Assert.Equal("shell", store.Get("job_1")?.Type);
        Assert.Single(store.All());

        job.Priority = 7;
        store.Upsert(job);
        Assert.Equal(7, store.Get("job_1")?.Priority);
        Assert.Single(store.All());

        store.Delete("job_1");
        Assert.Null(store.Get("job_1"));
        Assert.Empty(store.All());
    }

    [Fact]
    public void Reload_ReplaysJournal()
    {
        using var fixture = new TestStore();
        using (var store = fixture.Store)
        {
            store.Upsert(new Job { Id = "job_1", Type = "a", Payload = "{}", CreatedAt = DateTimeOffset.UtcNow, UpdatedAt = DateTimeOffset.UtcNow });
            store.Upsert(new Job { Id = "job_2", Type = "b", Payload = "{}", CreatedAt = DateTimeOffset.UtcNow, UpdatedAt = DateTimeOffset.UtcNow });
            store.Delete("job_2");
        }

        using var reopened = fixture.Store;
        Assert.Equal("a", reopened.Get("job_1")?.Type);
        Assert.Null(reopened.Get("job_2"));
        Assert.Single(reopened.All());
    }

    [Fact]
    public void Reload_RecoversRunningJobsToPending()
    {
        using var fixture = new TestStore();
        using (var store = fixture.Store)
        {
            store.Upsert(new Job
            {
                Id = "job_1", Type = "a", Payload = "{}",
                Status = JobStatus.Running, Attempts = 1,
                CreatedAt = DateTimeOffset.UtcNow, UpdatedAt = DateTimeOffset.UtcNow
            });
        }

        using var reopened = fixture.Store;
        var job = reopened.Get("job_1");
        Assert.Equal(JobStatus.Pending, job?.Status);
        Assert.Equal(0, job?.Attempts);
    }

    [Fact]
    public void Compact_FoldsJournalIntoSnapshot()
    {
        using var fixture = new TestStore();
        using (var store = fixture.Store)
        {
            store.Upsert(new Job { Id = "job_1", Type = "a", Payload = "{}", CreatedAt = DateTimeOffset.UtcNow, UpdatedAt = DateTimeOffset.UtcNow });
            store.Compact();
            store.Upsert(new Job { Id = "job_2", Type = "b", Payload = "{}", CreatedAt = DateTimeOffset.UtcNow, UpdatedAt = DateTimeOffset.UtcNow });
        }

        Assert.True(File.Exists(Path.Combine(fixture.Dir, "snapshot.json")));

        using var reopened = fixture.Store;
        Assert.Equal(2, reopened.All().Count);
    }
}
