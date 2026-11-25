using JobQueue.Scheduling;
using Xunit;

namespace JobQueue.Tests;

public sealed class SchedulerTests
{
    [Fact]
    public void Add_ValidatesInputsAndUniqueness()
    {
        using var fixture = new TestStore();
        using var store = fixture.Store;
        using var recurring = new RecurringJobStore(fixture.Dir);
        var engine = Factory.Engine(store, new FakeClock());
        var scheduler = new RecurringScheduler(engine, recurring);

        Assert.Throws<ArgumentException>(() => scheduler.Add("", "shell", "{}", 60));
        Assert.Throws<ArgumentException>(() => scheduler.Add("x", "", "{}", 60));
        Assert.Throws<ArgumentException>(() => scheduler.Add("x", "shell", "{}", 0));

        scheduler.Add("nightly", "shell", "{}", 60);
        Assert.Throws<InvalidOperationException>(() => scheduler.Add("nightly", "shell", "{}", 60));
    }

    [Fact]
    public void Tick_EnqueuesDueJobsAndAdvancesNextRun()
    {
        using var fixture = new TestStore();
        using var store = fixture.Store;
        using var recurring = new RecurringJobStore(fixture.Dir);
        var clock = new FakeClock();
        var engine = Factory.Engine(store, clock);
        var scheduler = new RecurringScheduler(engine, recurring, clock.Func);

        var definition = scheduler.Add("every-minute", "shell", """{"command":"echo"}""", 60);
        Assert.Empty(scheduler.Tick()); // not due yet

        clock.Now = clock.Now.AddSeconds(60);
        var enqueued = scheduler.Tick();
        Assert.Single(enqueued);
        Assert.Equal("shell", enqueued[0].Type);
        Assert.Equal(definition.Id, enqueued[0].RecurringJobId);

        var updated = recurring.Get(definition.Id);
        Assert.Equal(clock.Now.AddSeconds(60), updated?.NextRunAt);
    }

    [Fact]
    public void Tick_SkipsDisabledAndDoesNotBurstAfterOutage()
    {
        using var fixture = new TestStore();
        using var store = fixture.Store;
        using var recurring = new RecurringJobStore(fixture.Dir);
        var clock = new FakeClock();
        var engine = Factory.Engine(store, clock);
        var scheduler = new RecurringScheduler(engine, recurring, clock.Func);

        var disabled = scheduler.Add("off", "shell", "{}", 60);
        scheduler.SetEnabled(disabled.Id, false);
        scheduler.Add("on", "shell", "{}", 60);

        // Simulate a 10-hour outage: many intervals overdue.
        clock.Now = clock.Now.AddHours(10);
        var enqueued = scheduler.Tick();
        Assert.Single(enqueued); // only the enabled one, once — no catch-up burst
        Assert.Equal("on", recurring.Get(enqueued[0].RecurringJobId!)?.Name);
    }

    [Fact]
    public void Remove_DeletesDefinition()
    {
        using var fixture = new TestStore();
        using var store = fixture.Store;
        using var recurring = new RecurringJobStore(fixture.Dir);
        var engine = Factory.Engine(store, new FakeClock());
        var scheduler = new RecurringScheduler(engine, recurring);

        var definition = scheduler.Add("temp", "shell", "{}", 60);
        scheduler.Remove(definition.Id);
        Assert.Empty(scheduler.List());
    }
}
