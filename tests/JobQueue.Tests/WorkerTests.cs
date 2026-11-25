using JobQueue.Jobs;
using JobQueue.Workers;
using Xunit;

namespace JobQueue.Tests;

public sealed class WorkerTests
{
    private sealed class RecordingHandler : IJobHandler
    {
        public List<Job> Seen { get; } = [];
        public Func<Job, bool> Result { get; set; } = _ => true;

        public Task<bool> HandleAsync(Job job, CancellationToken cancellationToken)
        {
            Seen.Add(job);
            return Task.FromResult(Result(job));
        }
    }

    private sealed class ThrowingHandler : IJobHandler
    {
        public Task<bool> HandleAsync(Job job, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("handler exploded");
    }

    [Fact]
    public async Task ProcessOne_RunsHandlerAndCompletesJob()
    {
        using var fixture = new TestStore();
        using var store = fixture.Store;
        var engine = Factory.Engine(store, new FakeClock());
        var handler = new RecordingHandler();
        using var pool = new WorkerPool(engine, new Dictionary<string, IJobHandler> { ["work"] = handler });

        var job = engine.Enqueue(Factory.NewJob(type: "work"));
        Assert.True(await pool.ProcessOneAsync());
        Assert.Single(handler.Seen);
        Assert.Equal(JobStatus.Completed, store.Get(job.Id)?.Status);
    }

    [Fact]
    public async Task ProcessOne_ReturnsFalseWhenQueueEmpty()
    {
        using var fixture = new TestStore();
        using var store = fixture.Store;
        var engine = Factory.Engine(store, new FakeClock());
        using var pool = new WorkerPool(engine);

        Assert.False(await pool.ProcessOneAsync());
    }

    [Fact]
    public async Task ProcessOne_FailingHandlerRetriesThenDeadLetters()
    {
        using var fixture = new TestStore();
        using var store = fixture.Store;
        var clock = new FakeClock();
        var engine = Factory.Engine(store, clock, baseBackoff: TimeSpan.FromSeconds(1));
        using var pool = new WorkerPool(engine, new Dictionary<string, IJobHandler> { ["work"] = new ThrowingHandler() });

        var job = engine.Enqueue(Factory.NewJob(type: "work", maxAttempts: 2));

        Assert.True(await pool.ProcessOneAsync());
        Assert.Equal(JobStatus.Failed, store.Get(job.Id)?.Status);

        clock.Now = clock.Now.AddSeconds(1);
        Assert.True(await pool.ProcessOneAsync());
        Assert.Equal(JobStatus.DeadLettered, store.Get(job.Id)?.Status);
        Assert.Equal("handler exploded", store.Get(job.Id)?.LastError);
    }

    [Fact]
    public async Task ProcessOne_UnknownTypeFailsJob()
    {
        using var fixture = new TestStore();
        using var store = fixture.Store;
        var engine = Factory.Engine(store, new FakeClock());
        using var pool = new WorkerPool(engine, new Dictionary<string, IJobHandler>());

        var job = engine.Enqueue(Factory.NewJob(type: "no-such-handler", maxAttempts: 1));
        Assert.True(await pool.ProcessOneAsync());
        var stored = store.Get(job.Id);
        Assert.Equal(JobStatus.DeadLettered, stored?.Status);
        Assert.Contains("No handler registered", stored?.LastError);
    }

    [Fact]
    public async Task Drain_ProcessesAllDueJobs()
    {
        using var fixture = new TestStore();
        using var store = fixture.Store;
        var engine = Factory.Engine(store, new FakeClock());
        var handler = new RecordingHandler();
        using var pool = new WorkerPool(engine, new Dictionary<string, IJobHandler> { ["work"] = handler });

        engine.Enqueue(Factory.NewJob(type: "work"));
        engine.Enqueue(Factory.NewJob(type: "work"));
        engine.Enqueue(Factory.NewJob(type: "work"));

        Assert.Equal(3, await pool.DrainAsync());
        Assert.Equal(3, handler.Seen.Count);
    }

    [Fact]
    public async Task Start_RunsWorkersInBackground()
    {
        using var fixture = new TestStore();
        using var store = fixture.Store;
        var engine = Factory.Engine(store, new FakeClock());
        var handler = new RecordingHandler();
        using var pool = new WorkerPool(engine,
            new Dictionary<string, IJobHandler> { ["work"] = handler },
            concurrency: 2, pollInterval: TimeSpan.FromMilliseconds(10));

        engine.Enqueue(Factory.NewJob(type: "work"));
        engine.Enqueue(Factory.NewJob(type: "work"));
        pool.Start();

        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (handler.Seen.Count < 2 && DateTime.UtcNow < deadline)
            await Task.Delay(20);

        await pool.StopAsync();
        Assert.Equal(2, handler.Seen.Count);
    }

    [Fact]
    public async Task ShellHandler_RunsCommandAndReportsFailure()
    {
        using var fixture = new TestStore();
        using var store = fixture.Store;
        var engine = Factory.Engine(store, new FakeClock());
        using var pool = new WorkerPool(engine); // default "shell" handler

        var ok = engine.Enqueue(new Job
        {
            Type = "shell",
            Payload = """{"command":"echo","args":["hi"]}""",
            MaxAttempts = 1
        });
        Assert.True(await pool.ProcessOneAsync());
        Assert.Equal(JobStatus.Completed, store.Get(ok.Id)?.Status);

        var bad = engine.Enqueue(new Job
        {
            Type = "shell",
            Payload = """{"command":"false"}""",
            MaxAttempts = 1
        });
        Assert.True(await pool.ProcessOneAsync());
        var stored = store.Get(bad.Id);
        Assert.Equal(JobStatus.DeadLettered, stored?.Status);
        Assert.Contains("exited with code 1", stored?.LastError);
    }

    [Fact]
    public async Task ShellHandler_RejectsPayloadWithoutCommand()
    {
        var handler = new ShellJobHandler();
        var job = new Job { Id = "job_x", Type = "shell", Payload = """{"args":[]}""" };
        await Assert.ThrowsAsync<InvalidOperationException>(() => handler.HandleAsync(job, CancellationToken.None));
    }
}
