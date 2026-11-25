using System.Text.Json;
using JobQueue;
using JobQueue.Http;
using JobQueue.Jobs;
using JobQueue.Queue;
using JobQueue.Scheduling;
using JobQueue.Store;
using JobQueue.Workers;

// jobqueue — persistent distributed job queue.
//
//   jobqueue server [--data <dir>] [--port <n>] [--workers <n>]
//   jobqueue enqueue --type <name> [--payload '<json>'] [--priority <n>]
//                    [--delay <seconds>] [--max-attempts <n>] [--data <dir>]
//   jobqueue list [--status <Pending|Running|Completed|Failed|DeadLettered>] [--data <dir>]
//   jobqueue get <id> [--data <dir>]
//   jobqueue retry <id> [--data <dir>]
//   jobqueue stats [--data <dir>]
//   jobqueue recurring add --name <n> --type <t> --interval <seconds> [--payload '<json>'] [--data <dir>]
//   jobqueue recurring list [--data <dir>]
//   jobqueue recurring remove <id> [--data <dir>]

var cliArgs = Environment.GetCommandLineArgs().Skip(1).ToArray();
if (cliArgs.Length == 0 || cliArgs[0] is "-h" or "--help" or "help")
{
    PrintUsage();
    return 0;
}

var dataDir = Option(cliArgs, "--data") ?? Path.Combine(Directory.GetCurrentDirectory(), "data");
var port = int.TryParse(Option(cliArgs, "--port"), out var p) ? p : 8080;
var workers = int.TryParse(Option(cliArgs, "--workers"), out var w) ? w : 4;

try
{
    return cliArgs[0] switch
    {
        "server" => await RunServerAsync(dataDir, port, workers),
        "enqueue" => Enqueue(cliArgs, dataDir),
        "list" => List(cliArgs, dataDir),
        "get" => Get(cliArgs, dataDir),
        "retry" => Retry(cliArgs, dataDir),
        "stats" => Stats(dataDir),
        "recurring" => Recurring(cliArgs, dataDir),
        _ => UnknownCommand(cliArgs[0])
    };
}
catch (Exception ex) when (ex is ArgumentException or InvalidOperationException or KeyNotFoundException or JsonException)
{
    Console.Error.WriteLine($"error: {ex.Message}");
    return 1;
}

static void PrintUsage()
{
    Console.WriteLine("jobqueue — persistent distributed job queue (C#)");
    Console.WriteLine();
    Console.WriteLine("Usage:");
    Console.WriteLine("  jobqueue server [--data <dir>] [--port <n>] [--workers <n>]");
    Console.WriteLine("  jobqueue enqueue --type <name> [--payload '<json>'] [--priority <n>]");
    Console.WriteLine("                   [--delay <seconds>] [--max-attempts <n>] [--data <dir>]");
    Console.WriteLine("  jobqueue list [--status <Pending|Running|Completed|Failed|DeadLettered>] [--data <dir>]");
    Console.WriteLine("  jobqueue get <id> [--data <dir>]");
    Console.WriteLine("  jobqueue retry <id> [--data <dir>]");
    Console.WriteLine("  jobqueue stats [--data <dir>]");
    Console.WriteLine("  jobqueue recurring add --name <n> --type <t> --interval <seconds>");
    Console.WriteLine("                           [--payload '<json>'] [--data <dir>]");
    Console.WriteLine("  jobqueue recurring list [--data <dir>]");
    Console.WriteLine("  jobqueue recurring remove <id> [--data <dir>]");
    Console.WriteLine();
    Console.WriteLine("Job type \"shell\" runs a command: payload {\"command\": \"echo\", \"args\": [\"hi\"]}.");
    Console.WriteLine("Data defaults to ./data. The server also serves the HTTP API on --port.");
}

static int UnknownCommand(string command)
{
    Console.Error.WriteLine($"error: unknown command '{command}'. Run 'jobqueue --help'.");
    return 1;
}

static async Task<int> RunServerAsync(string dataDir, int port, int workerCount)
{
    using var store = new JsonlJobStore(dataDir);
    using var recurringStore = new RecurringJobStore(dataDir);
    var engine = new JobQueueEngine(store);
    var scheduler = new RecurringScheduler(engine, recurringStore);
    using var pool = new WorkerPool(engine, concurrency: workerCount);
    var api = new ApiHandler(engine, scheduler);
    using var server = new HttpServer(port, api.HandleAsync);

    pool.Start();
    scheduler.Start();
    server.Start();
    Console.WriteLine($"jobqueue server running — data: {dataDir}, api: http://localhost:{server.Port}, workers: {workerCount}");
    Console.WriteLine("Press Ctrl+C to stop.");

    using var cts = new CancellationTokenSource();
    Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };
    try { await Task.Delay(Timeout.Infinite, cts.Token); }
    catch (OperationCanceledException) { }

    Console.WriteLine("Stopping…");
    await scheduler.StopAsync();
    await pool.StopAsync();
    await server.StopAsync();
    return 0;
}

static int Enqueue(string[] args, string dataDir)
{
    var type = Required(args, "--type");
    var payload = Option(args, "--payload") ?? "{}";
    _ = JsonDocument.Parse(payload); // fail fast on invalid JSON
    var priority = int.TryParse(Option(args, "--priority"), out var pri) ? pri : 0;
    var maxAttempts = int.TryParse(Option(args, "--max-attempts"), out var ma) ? ma : 3;
    var delay = int.TryParse(Option(args, "--delay"), out var d) ? d : 0;

    using var store = new JsonlJobStore(dataDir);
    var engine = new JobQueueEngine(store);
    var job = engine.Enqueue(new Job
    {
        Type = type,
        Payload = payload,
        Priority = priority,
        MaxAttempts = maxAttempts,
        RunAt = delay > 0 ? DateTimeOffset.UtcNow.AddSeconds(delay) : null
    });
    Console.WriteLine(Serialize(job));
    return 0;
}

static int List(string[] args, string dataDir)
{
    var statusText = Option(args, "--status");
    JobStatus? status = null;
    if (!string.IsNullOrWhiteSpace(statusText))
    {
        if (!Enum.TryParse<JobStatus>(statusText, ignoreCase: true, out var parsed))
            throw new ArgumentException($"Unknown status '{statusText}'.");
        status = parsed;
    }

    using var store = new JsonlJobStore(dataDir);
    var engine = new JobQueueEngine(store);
    foreach (var job in engine.List(status))
        Console.WriteLine($"{job.Id}  {job.Status,-12} pri={job.Priority,-3} attempts={job.Attempts}/{job.MaxAttempts}  {job.Type}  {job.Payload}");
    return 0;
}

static int Get(string[] args, string dataDir)
{
    var id = Positional(args, 1);
    using var store = new JsonlJobStore(dataDir);
    var engine = new JobQueueEngine(store);
    var job = engine.Get(id) ?? throw new KeyNotFoundException($"No job with id '{id}'.");
    Console.WriteLine(Serialize(job));
    return 0;
}

static int Retry(string[] args, string dataDir)
{
    var id = Positional(args, 1);
    using var store = new JsonlJobStore(dataDir);
    var engine = new JobQueueEngine(store);
    Console.WriteLine(Serialize(engine.Retry(id)));
    return 0;
}

static int Stats(string dataDir)
{
    using var store = new JsonlJobStore(dataDir);
    var engine = new JobQueueEngine(store);
    var (pending, running, completed, failed, deadLettered) = engine.Stats();
    Console.WriteLine($"pending:       {pending}");
    Console.WriteLine($"running:       {running}");
    Console.WriteLine($"completed:     {completed}");
    Console.WriteLine($"failed:        {failed}");
    Console.WriteLine($"dead-lettered: {deadLettered}");
    Console.WriteLine($"total:         {pending + running + completed + failed + deadLettered}");
    return 0;
}

static int Recurring(string[] args, string dataDir)
{
    if (args.Length < 2)
        throw new ArgumentException("Usage: jobqueue recurring <add|list|remove> …");

    using var store = new JsonlJobStore(dataDir);
    using var recurringStore = new RecurringJobStore(dataDir);
    var engine = new JobQueueEngine(store);
    var scheduler = new RecurringScheduler(engine, recurringStore);

    switch (args[1])
    {
        case "add":
        {
            var name = Required(args, "--name");
            var type = Required(args, "--type");
            var intervalText = Required(args, "--interval");
            if (!int.TryParse(intervalText, out var interval) || interval < 1)
                throw new ArgumentException("--interval must be at least 1 second.");
            var payload = Option(args, "--payload") ?? "{}";
            _ = JsonDocument.Parse(payload);
            var priority = int.TryParse(Option(args, "--priority"), out var pri) ? pri : 0;
            var job = scheduler.Add(name, type, payload, interval, priority);
            Console.WriteLine(Serialize(job));
            return 0;
        }
        case "list":
        {
            foreach (var job in scheduler.List())
                Console.WriteLine($"{job.Id}  {(job.Enabled ? "enabled " : "disabled")} every {job.IntervalSeconds}s  next {job.NextRunAt:u}  {job.Name} ({job.Type})");
            return 0;
        }
        case "remove":
        {
            var id = Positional(args, 2);
            scheduler.Remove(id);
            Console.WriteLine($"removed {id}");
            return 0;
        }
        default:
            throw new ArgumentException($"Unknown recurring subcommand '{args[1]}'.");
    }
}

static string? Option(string[] args, string name)
{
    for (var i = 0; i < args.Length - 1; i++)
        if (args[i] == name)
            return args[i + 1];
    return null;
}

static string Required(string[] args, string name) =>
    Option(args, name) ?? throw new ArgumentException($"Missing required option {name}.");

static string Positional(string[] args, int index) =>
    args.Length > index ? args[index] : throw new ArgumentException("Missing required argument.");

static string Serialize<T>(T value) =>
    JsonSerializer.Serialize(value, new JsonSerializerOptions
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        Converters = { new System.Text.Json.Serialization.JsonStringEnumConverter() }
    });
