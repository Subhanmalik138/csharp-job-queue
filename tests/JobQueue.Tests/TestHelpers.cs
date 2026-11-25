using JobQueue.Jobs;
using JobQueue.Queue;
using JobQueue.Store;

namespace JobQueue.Tests;

/// <summary>Mutable clock so scheduling tests run without sleeping.</summary>
internal sealed class FakeClock
{
    public DateTimeOffset Now { get; set; } = new(2026, 11, 25, 12, 0, 0, TimeSpan.Zero);
    public Func<DateTimeOffset> Func => () => Now;
}

internal sealed class TestStore : IDisposable
{
    public string Dir { get; } = Path.Combine(Path.GetTempPath(), "jobqueue-tests", Guid.NewGuid().ToString("N"));
    public JsonlJobStore Store => new(Dir);
    public void Dispose()
    {
        try { Directory.Delete(Dir, recursive: true); } catch (IOException) { } catch (UnauthorizedAccessException) { }
    }
}

internal static class Factory
{
    public static JobQueueEngine Engine(JsonlJobStore store, FakeClock clock, TimeSpan? baseBackoff = null) =>
        new(store, clock.Func, new Random(42))
        {
            BaseBackoff = baseBackoff ?? TimeSpan.FromSeconds(10),
            JitterFraction = 0 // deterministic backoff in tests
        };

    public static Job NewJob(string type = "shell", int priority = 0, int maxAttempts = 3, DateTimeOffset? runAt = null) =>
        new() { Type = type, Priority = priority, MaxAttempts = maxAttempts, RunAt = runAt, Payload = "{}" };
}
