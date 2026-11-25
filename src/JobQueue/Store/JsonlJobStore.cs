using System.Text.Json;
using JobQueue.Jobs;

namespace JobQueue.Store;

/// <summary>
/// Append-only journal + snapshot store.
///
/// Layout inside the data directory:
/// <list type="bullet">
///   <item><c>snapshot.json</c> — full job table plus the journal sequence it covers.</item>
///   <item><c>journal.jsonl</c> — one line per mutation after the snapshot.</item>
/// </list>
/// Every mutation is appended to the journal and flushed to disk before the
/// call returns, so a crash can lose at most the mutation in flight. Loading
/// replays the journal over the snapshot, and any job left <see cref="JobStatus.Running"/>
/// is returned to <see cref="JobStatus.Pending"/> — the worker that held it is
/// gone, so the job must become runnable again instead of being stuck forever.
/// </summary>
public sealed class JsonlJobStore : IJobStore
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = false,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    private readonly string _snapshotPath;
    private readonly string _journalPath;
    private readonly Dictionary<string, Job> _jobs = new(StringComparer.Ordinal);
    private readonly object _gate = new();
    private long _sequence;
    private bool _disposed;

    public JsonlJobStore(string directory)
    {
        Directory.CreateDirectory(directory);
        _snapshotPath = Path.Combine(directory, "snapshot.json");
        _journalPath = Path.Combine(directory, "journal.jsonl");
        Load();
    }

    public void Upsert(Job job)
    {
        ArgumentNullException.ThrowIfNull(job);
        if (string.IsNullOrWhiteSpace(job.Id))
            throw new ArgumentException("Job id must not be empty.", nameof(job));

        lock (_gate)
        {
            _jobs[job.Id] = Clone(job);
            AppendJournal(new JournalEntry(++_sequence, "upsert", Clone(job)));
        }
    }

    public void Delete(string id)
    {
        lock (_gate)
        {
            if (!_jobs.Remove(id)) return;
            AppendJournal(new JournalEntry(++_sequence, "delete", null, id));
        }
    }

    public Job? Get(string id)
    {
        lock (_gate)
            return _jobs.TryGetValue(id, out var job) ? Clone(job) : null;
    }

    public IReadOnlyList<Job> All()
    {
        lock (_gate)
            return _jobs.Values.Select(Clone).ToList();
    }

    public void Compact()
    {
        lock (_gate)
        {
            var snapshot = new Snapshot(_sequence, _jobs.Values.ToList());
            var tmp = _snapshotPath + ".tmp";
            File.WriteAllText(tmp, JsonSerializer.Serialize(snapshot, JsonOptions));
            File.Move(tmp, _snapshotPath, overwrite: true);
            File.WriteAllText(_journalPath, string.Empty);
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
    }

    private void Load()
    {
        if (File.Exists(_snapshotPath))
        {
            var snapshot = JsonSerializer.Deserialize<Snapshot>(
                File.ReadAllText(_snapshotPath), JsonOptions);
            if (snapshot is not null)
            {
                _sequence = snapshot.Sequence;
                foreach (var job in snapshot.Jobs)
                    _jobs[job.Id] = job;
            }
        }

        if (!File.Exists(_journalPath)) return;
        foreach (var line in File.ReadLines(_journalPath))
        {
            if (string.IsNullOrWhiteSpace(line)) continue;
            var entry = JsonSerializer.Deserialize<JournalEntry>(line, JsonOptions);
            if (entry is null || entry.Sequence <= _sequence) continue;
            _sequence = entry.Sequence;
            if (entry.Op == "delete" && entry.Id is not null)
                _jobs.Remove(entry.Id);
            else if (entry.Op == "upsert" && entry.Job is not null)
                _jobs[entry.Job.Id] = entry.Job;
        }

        // Crash recovery: a job still marked Running belonged to a dead worker.
        var recovered = false;
        foreach (var job in _jobs.Values.Where(j => j.Status == JobStatus.Running))
        {
            job.Status = JobStatus.Pending;
            job.Attempts = Math.Max(0, job.Attempts - 1); // the attempt never completed
            job.UpdatedAt = DateTimeOffset.UtcNow;
            recovered = true;
        }

        if (recovered)
            Compact();
    }

    private void AppendJournal(JournalEntry entry)
    {
        var line = JsonSerializer.Serialize(entry, JsonOptions) + "\n";
        File.AppendAllText(_journalPath, line);
    }

    private static Job Clone(Job job) =>
        JsonSerializer.Deserialize<Job>(JsonSerializer.Serialize(job, JsonOptions), JsonOptions)!;

    private sealed record JournalEntry(long Sequence, string Op, Job? Job, string? Id = null);

    private sealed record Snapshot(long Sequence, List<Job> Jobs);
}
