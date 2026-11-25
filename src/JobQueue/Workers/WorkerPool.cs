using JobQueue.Jobs;
using JobQueue.Queue;

namespace JobQueue.Workers;

/// <summary>
/// Runs <see cref="JobQueueEngine"/> jobs with a bounded number of concurrent
/// workers. Each worker loops: claim the next due job, dispatch it to the
/// handler registered for its <see cref="Job.Type"/> (falling back to the
/// <c>shell</c> handler), then complete or fail it through the engine.
/// Unknown job types fail immediately with a clear error instead of spinning.
/// Polling is deliberately simple: a short sleep between empty claims keeps
/// CPU use near zero while staying responsive. Stop is cooperative — in-flight
/// jobs finish, and anything still running at process exit is recovered to
/// Pending by the store on the next start.
/// </summary>
public sealed class WorkerPool : IDisposable
{
    private readonly JobQueueEngine _engine;
    private readonly Dictionary<string, IJobHandler> _handlers;
    private readonly int _concurrency;
    private readonly TimeSpan _pollInterval;
    private readonly List<Task> _workers = [];
    private CancellationTokenSource? _cts;
    private bool _disposed;

    public WorkerPool(
        JobQueueEngine engine,
        IReadOnlyDictionary<string, IJobHandler>? handlers = null,
        int concurrency = 4,
        TimeSpan? pollInterval = null)
    {
        _engine = engine;
        _handlers = new Dictionary<string, IJobHandler>(handlers ?? new Dictionary<string, IJobHandler>(), StringComparer.OrdinalIgnoreCase);
        if (!_handlers.ContainsKey("shell"))
            _handlers["shell"] = new ShellJobHandler();
        _concurrency = Math.Max(1, concurrency);
        _pollInterval = pollInterval ?? TimeSpan.FromMilliseconds(250);
    }

    public int Concurrency => _concurrency;

    public bool IsRunning => _cts is { IsCancellationRequested: false };

    public void Start()
    {
        if (IsRunning) return;
        _cts = new CancellationTokenSource();
        _workers.Clear();
        for (var i = 0; i < _concurrency; i++)
            _workers.Add(Task.Run(() => WorkerLoopAsync(_cts.Token)));
    }

    /// <summary>Stop accepting new work and wait for in-flight jobs to finish.</summary>
    public async Task StopAsync()
    {
        if (_cts is null) return;
        await _cts.CancelAsync();
        try { await Task.WhenAll(_workers); }
        catch (OperationCanceledException) { }
        _cts.Dispose();
        _cts = null;
        _workers.Clear();
    }

    /// <summary>Process one due job synchronously. Useful for tests and one-shot runs.</summary>
    public async Task<bool> ProcessOneAsync(CancellationToken cancellationToken = default)
    {
        var job = _engine.TryClaimNext();
        if (job is null) return false;
        await ExecuteAsync(job, cancellationToken);
        return true;
    }

    /// <summary>Process due jobs until the queue is empty. Returns the number processed.</summary>
    public async Task<int> DrainAsync(CancellationToken cancellationToken = default)
    {
        var processed = 0;
        while (await ProcessOneAsync(cancellationToken))
            processed++;
        return processed;
    }

    private async Task WorkerLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            Job? job;
            try
            {
                job = _engine.TryClaimNext();
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[worker] claim failed: {ex.Message}");
                await DelayAsync(cancellationToken);
                continue;
            }

            if (job is null)
            {
                await DelayAsync(cancellationToken);
                continue;
            }

            await ExecuteAsync(job, cancellationToken);
        }
    }

    private async Task ExecuteAsync(Job job, CancellationToken cancellationToken)
    {
        try
        {
            if (!_handlers.TryGetValue(job.Type, out var handler))
                throw new InvalidOperationException($"No handler registered for job type '{job.Type}'.");

            var ok = await handler.HandleAsync(job, cancellationToken);
            if (ok)
                _engine.Complete(job.Id);
            else
                _engine.Fail(job.Id, "Handler returned false.");
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Shutdown mid-job: leave the store state as-is; recovery on next
            // start returns the job to Pending. Do not mark it failed here —
            // the attempt never got a verdict.
            _engine.Abandon(job.Id);
        }
        catch (Exception ex)
        {
            try { _engine.Fail(job.Id, ex.Message); }
            catch (Exception failEx)
            {
                Console.Error.WriteLine($"[worker] could not record failure of {job.Id}: {failEx.Message}");
            }
        }
    }

    private async Task DelayAsync(CancellationToken cancellationToken)
    {
        try { await Task.Delay(_pollInterval, cancellationToken); }
        catch (OperationCanceledException) { }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _cts?.Cancel();
        _cts?.Dispose();
    }
}
