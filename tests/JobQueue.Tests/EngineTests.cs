using JobQueue.Jobs;
using JobQueue.Queue;
using Xunit;

namespace JobQueue.Tests;

public sealed class EngineTests
{
    [Fact]
    public void Enqueue_AssignsIdAndDefaults()
    {
        using var fixture = new TestStore();
        using var store = fixture.Store;
        var clock = new FakeClock();
        var engine = Factory.Engine(store, clock);

        var job = engine.Enqueue(Factory.NewJob());

        Assert.StartsWith("job_", job.Id);
        Assert.Equal(JobStatus.Pending, job.Status);
        Assert.Equal(0, job.Attempts);
        Assert.Equal(3, job.MaxAttempts);
        Assert.Equal(clock.Now, job.CreatedAt);
        Assert.Equal(job.Id, store.Get(job.Id)?.Id);
    }

    [Fact]
    public void Enqueue_RejectsEmptyTypeAndBadMaxAttempts()
    {
        using var fixture = new TestStore();
        using var store = fixture.Store;
        var engine = Factory.Engine(store, new FakeClock());

        Assert.Throws<ArgumentException>(() => engine.Enqueue(new Job { Type = "" }));
        Assert.Throws<ArgumentException>(() => engine.Enqueue(new Job { Type = "x", MaxAttempts = 0 }));
    }

    [Fact]
    public void TryClaimNext_PicksHighestPriorityThenOldest()
    {
        using var fixture = new TestStore();
        using var store = fixture.Store;
        var clock = new FakeClock();
        var engine = Factory.Engine(store, clock);

        var low = engine.Enqueue(Factory.NewJob(priority: 1));
        var high = engine.Enqueue(Factory.NewJob(priority: 9));
        var mid = engine.Enqueue(Factory.NewJob(priority: 5));

        Assert.Equal(high.Id, engine.TryClaimNext()?.Id);
        Assert.Equal(mid.Id, engine.TryClaimNext()?.Id);
        Assert.Equal(low.Id, engine.TryClaimNext()?.Id);
        Assert.Null(engine.TryClaimNext());
    }

    [Fact]
    public void TryClaimNext_SkipsJobsNotDueYet()
    {
        using var fixture = new TestStore();
        using var store = fixture.Store;
        var clock = new FakeClock();
        var engine = Factory.Engine(store, clock);

        var future = engine.Enqueue(Factory.NewJob(runAt: clock.Now.AddMinutes(5)));
        var now = engine.Enqueue(Factory.NewJob());

        Assert.Equal(now.Id, engine.TryClaimNext()?.Id);
        Assert.Null(engine.TryClaimNext());

        clock.Now = clock.Now.AddMinutes(5);
        Assert.Equal(future.Id, engine.TryClaimNext()?.Id);
    }

    [Fact]
    public void Claim_MarksRunningAndCountsAttempt()
    {
        using var fixture = new TestStore();
        using var store = fixture.Store;
        var engine = Factory.Engine(store, new FakeClock());

        var job = engine.Enqueue(Factory.NewJob());
        var claimed = engine.TryClaimNext();

        Assert.Equal(JobStatus.Running, claimed?.Status);
        Assert.Equal(1, claimed?.Attempts);
        Assert.Equal(JobStatus.Running, store.Get(job.Id)?.Status);
    }

    [Fact]
    public void Complete_MarksJobCompleted()
    {
        using var fixture = new TestStore();
        using var store = fixture.Store;
        var engine = Factory.Engine(store, new FakeClock());

        var job = engine.Enqueue(Factory.NewJob());
        engine.TryClaimNext();
        engine.Complete(job.Id);

        Assert.Equal(JobStatus.Completed, store.Get(job.Id)?.Status);
    }

    [Fact]
    public void Fail_RetriesWithBackoffThenDeadLetters()
    {
        using var fixture = new TestStore();
        using var store = fixture.Store;
        var clock = new FakeClock();
        var engine = Factory.Engine(store, clock, baseBackoff: TimeSpan.FromSeconds(10));

        var job = engine.Enqueue(Factory.NewJob(maxAttempts: 3));

        // Attempt 1 fails -> Failed, next try in 10s.
        engine.TryClaimNext();
        var failed = engine.Fail(job.Id, "boom");
        Assert.Equal(JobStatus.Failed, failed.Status);
        Assert.Equal(clock.Now.AddSeconds(10), failed.NextAttemptAt);
        Assert.Equal("boom", failed.LastError);
        Assert.Null(engine.TryClaimNext()); // not due yet

        // Attempt 2 fails -> backoff doubles to 20s.
        clock.Now = clock.Now.AddSeconds(10);
        engine.TryClaimNext();
        failed = engine.Fail(job.Id, "boom again");
        Assert.Equal(JobStatus.Failed, failed.Status);
        Assert.Equal(clock.Now.AddSeconds(20), failed.NextAttemptAt);

        // Attempt 3 fails -> no retries left, dead-lettered.
        clock.Now = clock.Now.AddSeconds(20);
        engine.TryClaimNext();
        failed = engine.Fail(job.Id, "boom final");
        Assert.Equal(JobStatus.DeadLettered, failed.Status);
        Assert.Null(failed.NextAttemptAt);
    }

    [Fact]
    public void Retry_ReturnsDeadLetteredJobToPending()
    {
        using var fixture = new TestStore();
        using var store = fixture.Store;
        var clock = new FakeClock();
        var engine = Factory.Engine(store, clock, baseBackoff: TimeSpan.FromSeconds(1));

        var job = engine.Enqueue(Factory.NewJob(maxAttempts: 1));
        engine.TryClaimNext();
        engine.Fail(job.Id, "boom");
        Assert.Equal(JobStatus.DeadLettered, store.Get(job.Id)?.Status);

        var retried = engine.Retry(job.Id);
        Assert.Equal(JobStatus.Pending, retried.Status);
        Assert.Equal(0, retried.Attempts);
        Assert.Null(retried.LastError);
        Assert.Equal(job.Id, engine.TryClaimNext()?.Id);
    }

    [Fact]
    public void Retry_RejectsJobsThatAreNotFailedOrDeadLettered()
    {
        using var fixture = new TestStore();
        using var store = fixture.Store;
        var engine = Factory.Engine(store, new FakeClock());

        var job = engine.Enqueue(Factory.NewJob());
        Assert.Throws<InvalidOperationException>(() => engine.Retry(job.Id));
    }

    [Fact]
    public void Abandon_ReturnsRunningJobToPendingWithoutCountingAttempt()
    {
        using var fixture = new TestStore();
        using var store = fixture.Store;
        var engine = Factory.Engine(store, new FakeClock());

        var job = engine.Enqueue(Factory.NewJob());
        engine.TryClaimNext();
        engine.Abandon(job.Id);

        var stored = store.Get(job.Id);
        Assert.Equal(JobStatus.Pending, stored?.Status);
        Assert.Equal(0, stored?.Attempts);
    }

    [Fact]
    public void Stats_CountsByStatus()
    {
        using var fixture = new TestStore();
        using var store = fixture.Store;
        var engine = Factory.Engine(store, new FakeClock());

        var a = engine.Enqueue(Factory.NewJob());
        var b = engine.Enqueue(Factory.NewJob());
        engine.TryClaimNext();
        engine.Complete(a.Id);
        engine.TryClaimNext();
        engine.Fail(b.Id, "x");

        var (pending, running, completed, failed, deadLettered) = engine.Stats();
        Assert.Equal(0, pending);
        Assert.Equal(0, running);
        Assert.Equal(1, completed);
        Assert.Equal(1, failed);
        Assert.Equal(0, deadLettered);
    }

    [Fact]
    public void Delete_RemovesJobAndThrowsForUnknownId()
    {
        using var fixture = new TestStore();
        using var store = fixture.Store;
        var engine = Factory.Engine(store, new FakeClock());

        var job = engine.Enqueue(Factory.NewJob());
        engine.Delete(job.Id);
        Assert.Null(store.Get(job.Id));
        Assert.Throws<KeyNotFoundException>(() => engine.Delete("nope"));
    }

    [Fact]
    public void ComputeBackoff_DoublesPerAttemptAndCaps()
    {
        using var fixture = new TestStore();
        using var store = fixture.Store;
        var engine = new JobQueueEngine(store, new FakeClock().Func, new Random(42))
        {
            BaseBackoff = TimeSpan.FromSeconds(10),
            JitterFraction = 0,
            MaxBackoff = TimeSpan.FromSeconds(30)
        };

        Assert.Equal(TimeSpan.FromSeconds(10), engine.ComputeBackoff(1));
        Assert.Equal(TimeSpan.FromSeconds(20), engine.ComputeBackoff(2));
        Assert.Equal(TimeSpan.FromSeconds(30), engine.ComputeBackoff(3)); // capped
        Assert.Equal(TimeSpan.FromSeconds(30), engine.ComputeBackoff(9));
    }
}
