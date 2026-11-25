using System.Text.Json;
using JobQueue.Jobs;

namespace JobQueue.Scheduling;

/// <summary>
/// Durable registry for <see cref="RecurringJob"/> definitions, kept in a
/// single <c>recurring.json</c> file written atomically (write temp + rename),
/// so a crash can never leave a half-written registry behind.
/// </summary>
public sealed class RecurringJobStore : IDisposable
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    private readonly string _path;
    private readonly Dictionary<string, RecurringJob> _jobs = new(StringComparer.Ordinal);
    private readonly object _gate = new();
    private bool _disposed;

    public RecurringJobStore(string directory)
    {
        Directory.CreateDirectory(directory);
        _path = Path.Combine(directory, "recurring.json");
        Load();
    }

    public void Upsert(RecurringJob job)
    {
        ArgumentNullException.ThrowIfNull(job);
        if (string.IsNullOrWhiteSpace(job.Id))
            throw new ArgumentException("Recurring job id must not be empty.", nameof(job));
        lock (_gate)
        {
            _jobs[job.Id] = Clone(job);
            SaveLocked();
        }
    }

    public void Delete(string id)
    {
        lock (_gate)
        {
            if (_jobs.Remove(id))
                SaveLocked();
        }
    }

    public RecurringJob? Get(string id)
    {
        lock (_gate)
            return _jobs.TryGetValue(id, out var job) ? Clone(job) : null;
    }

    public RecurringJob? GetByName(string name)
    {
        lock (_gate)
        {
            var job = _jobs.Values.FirstOrDefault(j =>
                string.Equals(j.Name, name, StringComparison.OrdinalIgnoreCase));
            return job is null ? null : Clone(job);
        }
    }

    public IReadOnlyList<RecurringJob> All()
    {
        lock (_gate)
            return _jobs.Values.Select(Clone).ToList();
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
    }

    private void Load()
    {
        if (!File.Exists(_path)) return;
        var jobs = JsonSerializer.Deserialize<List<RecurringJob>>(
            File.ReadAllText(_path), JsonOptions);
        if (jobs is null) return;
        foreach (var job in jobs)
            _jobs[job.Id] = job;
    }

    private void SaveLocked()
    {
        var tmp = _path + ".tmp";
        File.WriteAllText(tmp, JsonSerializer.Serialize(_jobs.Values.ToList(), JsonOptions));
        File.Move(tmp, _path, overwrite: true);
    }

    private static RecurringJob Clone(RecurringJob job) =>
        JsonSerializer.Deserialize<RecurringJob>(JsonSerializer.Serialize(job, JsonOptions), JsonOptions)!;
}
