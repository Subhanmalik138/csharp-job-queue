using JobQueue.Jobs;
using JobQueue.Queue;

namespace JobQueue.Scheduling;

/// <summary>
/// Turns <see cref="RecurringJob"/> definitions into real jobs. Call
/// <see cref="Tick"/> on a timer (or <see cref="TickAsync"/> in a loop):
/// every enabled definition whose <see cref="RecurringJob.NextRunAt"/> has
/// passed enqueues one job and advances to the next firing.
///
/// If the process was down for a while, a definition may be many intervals
/// overdue. Instead of flooding the queue with catch-up jobs, at most one
/// instance is enqueued per tick and the next run is anchored to "now +
/// interval" — a missed firing is skipped, never replayed in a burst.
/// </summary>
public sealed class RecurringScheduler : IDisposable
{
    private readonly JobQueueEngine _engine;
    private readonly RecurringJobStore _store;
    private readonly Func<DateTimeOffset> _clock;
    private readonly TimeSpan _tickInterval;
    private CancellationTokenSource? _cts;
    private Task? _loop;
    private bool _disposed;

    public RecurringScheduler(
        JobQueueEngine engine,
        RecurringJobStore store,
        Func<DateTimeOffset>? clock = null,
        TimeSpan? tickInterval = null)
    {
        _engine = engine;
        _store = store;
        _clock = clock ?? (() => DateTimeOffset.UtcNow);
        _tickInterval = tickInterval ?? TimeSpan.FromSeconds(1);
    }

    public bool IsRunning => _loop is { IsCompleted: false };

    /// <summary>Register a new recurring job. Names must be unique.</summary>
    public RecurringJob Add(
        string name,
        string type,
        string payload,
        int intervalSeconds,
        int priority = 0,
        int maxAttempts = 3)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Name must not be empty.", nameof(name));
        if (string.IsNullOrWhiteSpace(type))
            throw new ArgumentException("Type must not be empty.", nameof(type));
        if (intervalSeconds < 1)
            throw new ArgumentException("Interval must be at least 1 second.", nameof(intervalSeconds));
        if (_store.GetByName(name) is not null)
            throw new InvalidOperationException($"A recurring job named '{name}' already exists.");

        var now = _clock();
        var job = new RecurringJob
        {
            Id = "rec_" + Guid.NewGuid().ToString("N")[..20],
            Name = name,
            Type = type,
            Payload = string.IsNullOrWhiteSpace(payload) ? "{}" : payload,
            Priority = priority,
            MaxAttempts = maxAttempts,
            IntervalSeconds = intervalSeconds,
            NextRunAt = now.AddSeconds(intervalSeconds),
            CreatedAt = now
        };
        _store.Upsert(job);
        return job;
    }

    public void Remove(string id) => _store.Delete(id);

    public void SetEnabled(string id, bool enabled)
    {
        var job = _store.Get(id) ?? throw new KeyNotFoundException($"No recurring job with id '{id}'.");
        job.Enabled = enabled;
        _store.Upsert(job);
    }

    public IReadOnlyList<RecurringJob> List() => _store.All();

    /// <summary>Fire every due definition once. Returns the jobs enqueued.</summary>
    public IReadOnlyList<Job> Tick()
    {
        var now = _clock();
        var enqueued = new List<Job>();
        foreach (var definition in _store.All().Where(d => d.Enabled && d.NextRunAt <= now))
        {
            var job = new Job
            {
                Type = definition.Type,
                Payload = definition.Payload,
                Priority = definition.Priority,
                MaxAttempts = definition.MaxAttempts,
                RecurringJobId = definition.Id
            };
            enqueued.Add(_engine.Enqueue(job));

            // Anchor the next firing to now, not to the overdue slot, so a
            // long outage produces one catch-up job, not a burst.
            definition.NextRunAt = now.AddSeconds(definition.IntervalSeconds);
            _store.Upsert(definition);
        }
        return enqueued;
    }

    public void Start()
    {
        if (IsRunning) return;
        _cts = new CancellationTokenSource();
        _loop = Task.Run(() => LoopAsync(_cts.Token));
    }

    public async Task StopAsync()
    {
        if (_cts is null) return;
        await _cts.CancelAsync();
        if (_loop is not null)
        {
            try { await _loop; } catch (OperationCanceledException) { }
        }
        _cts.Dispose();
        _cts = null;
        _loop = null;
    }

    private async Task LoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try { Tick(); }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[scheduler] tick failed: {ex.Message}");
            }
            try { await Task.Delay(_tickInterval, cancellationToken); }
            catch (OperationCanceledException) { }
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _cts?.Cancel();
        _cts?.Dispose();
    }
}
