# JobQueue — Persistent Distributed Job Queue (C# / .NET 8)

A zero-dependency, persistent job queue: enqueue work, run it with a bounded
worker pool, retry failures with exponential backoff, and park exhausted jobs
in a dead-letter queue for inspection. Jobs survive crashes — the queue is an
append-only journal on disk, and anything a dead worker left `Running` is
recovered to `Pending` on the next start.

## What it does

- **Priority queue** — due jobs run highest-priority first, then earliest-due,
  then oldest. Jobs can be delayed (`RunAt` / `--delay`).
- **Durable storage** — every mutation is appended to a JSONL journal and
  flushed before the call returns; snapshots compact the journal. A crash
  loses at most the mutation in flight.
- **Worker pool** — N concurrent workers claim jobs atomically (a claimed job
  is marked `Running` in the store immediately, so two workers never run the
  same job). Handlers are pluggable by job type; a built-in `shell` handler
  runs any external command.
- **Retries with backoff** — exponential backoff with jitter, capped at 5
  minutes. When `MaxAttempts` is spent the job moves to the dead-letter
  queue, where it can be retried manually.
- **Recurring jobs** — fixed-interval schedules (`every 60s`). A long outage
  produces one catch-up job, never a burst.
- **HTTP API + CLI** — the same queue is driven from the command line or over
  HTTP (`POST /jobs`, `GET /jobs`, `GET /stats`, …).

## Design decisions

1. **Journal + snapshot instead of a database.** A queue's write pattern is
   append-heavy and the dataset is small; an append-only journal gives
   crash safety without a server dependency, and snapshots keep replays fast.
   The store is behind `IJobStore`, so SQLite or Postgres can replace it
   without touching the engine.
2. **Priority is a scheduling hint, not a separate queue.** One ordered scan
   over due jobs keeps the code simple and makes "due" and "priority"
   interact in exactly one place (`TryClaimNext`).
3. **Injectable clock and randomness.** The engine takes a `Func<DateTimeOffset>`
   clock and a `Random`, so retry timing and scheduling are fully testable
   without sleeping.
4. **Hand-rolled HTTP server.** The API is a handful of JSON endpoints; a
   ~200-line `TcpListener` server covers it with zero dependencies instead of
   pulling in ASP.NET Core.
5. **At-least-once delivery.** A job whose worker dies mid-run is requeued,
   so handlers must be idempotent — the README and the `IJobHandler` docs say
   so up front rather than pretending exactly-once is possible.

## Quick start

```bash
dotnet run --project src/JobQueue.Cli -- server --data ./data --port 8080 --workers 4
```

Enqueue a job (runs `echo hello` via the built-in `shell` handler):

```bash
dotnet run --project src/JobQueue.Cli -- enqueue --type shell \
  --payload '{"command":"echo","args":["hello"]}' --data ./data
```

Delayed and recurring jobs:

```bash
# run once, 10 minutes from now
dotnet run --project src/JobQueue.Cli -- enqueue --type shell \
  --payload '{"command":"./backup.sh"}' --delay 600 --data ./data

# run every hour, forever
dotnet run --project src/JobQueue.Cli -- recurring add --name hourly-backup \
  --type shell --interval 3600 --payload '{"command":"./backup.sh"}' --data ./data
```

Inspect and repair:

```bash
dotnet run --project src/JobQueue.Cli -- list --status DeadLettered --data ./data
dotnet run --project src/JobQueue.Cli -- retry <job-id> --data ./data
dotnet run --project src/JobQueue.Cli -- stats --data ./data
```

Or use the HTTP API while the server runs:

```bash
curl -X POST localhost:8080/jobs -H 'Content-Type: application/json' \
  -d '{"type":"shell","payload":"{\"command\":\"echo\",\"args\":[\"hi\"]}","priority":5}'

curl localhost:8080/stats
curl "localhost:8080/jobs?status=DeadLettered"
curl -X POST localhost:8080/jobs/<job-id>/retry
```

## API reference

| Method | Route | Description |
| ------ | ----- | ----------- |
| GET | `/health` | Liveness probe |
| GET | `/stats` | Job counts by status |
| POST | `/jobs` | Enqueue (`type`, `payload`, `priority?`, `maxAttempts?`, `delaySeconds?`, `runAt?`) |
| GET | `/jobs?status=&limit=` | List jobs, newest first |
| GET | `/jobs/{id}` | One job |
| POST | `/jobs/{id}/retry` | Requeue a failed / dead-lettered job |
| DELETE | `/jobs/{id}` | Delete a job |
| POST | `/recurring` | Register a recurring job (`name`, `type`, `intervalSeconds`, …) |
| GET | `/recurring` | List recurring jobs |
| DELETE | `/recurring/{id}` | Remove a recurring job |

## Writing a handler

```csharp
public sealed class EmailHandler : IJobHandler
{
    public async Task<bool> HandleAsync(Job job, CancellationToken ct)
    {
        // Parse job.Payload (JSON), do the work, return true on success.
        // Return false or throw to fail the attempt — the engine retries it.
        return true;
    }
}

var pool = new WorkerPool(engine, new Dictionary<string, IJobHandler>
{
    ["send-email"] = new EmailHandler()
});
```

Register handlers by job type; anything with no handler fails fast with a
clear error instead of spinning. The `shell` handler is always registered:
its payload is `{"command": "...", "args": [...], "input": "..."}` and exit
code 0 means success.

## Project layout

```
src/JobQueue/            # library: engine, store, workers, scheduler, HTTP
  Jobs/                  # Job + JobStatus
  Store/                 # IJobStore + JSONL journal/snapshot store
  Queue/                 # JobQueueEngine (priority, claiming, retries)
  Workers/               # IJobHandler, ShellJobHandler, WorkerPool
  Scheduling/            # RecurringJob, store, scheduler
  Http/                  # tiny TcpListener HTTP server
  ApiHandler.cs          # HTTP routing
src/JobQueue.Cli/        # `jobqueue` command-line tool
tests/JobQueue.Tests/    # 38 xUnit tests
```

## Running tests

```bash
dotnet test
```

38 tests cover the engine (priority, delays, backoff, dead-lettering),
store recovery (journal replay, `Running` → `Pending` after a crash),
workers (success, failure, unknown types, shell handler), the scheduler
(no catch-up bursts after an outage), and the HTTP API (including a live
socket round-trip).

## Tech stack

- C# 12 / .NET 8, no third-party runtime dependencies
- xUnit for tests
- `System.Text.Json` throughout; `TcpListener` for the HTTP API
