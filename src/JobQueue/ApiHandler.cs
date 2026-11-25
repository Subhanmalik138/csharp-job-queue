using System.Text.Json;
using System.Text.Json.Serialization;
using JobQueue.Http;
using JobQueue.Jobs;
using JobQueue.Queue;
using JobQueue.Scheduling;

namespace JobQueue;

/// <summary>
/// Routes HTTP requests to the engine and scheduler. Kept separate from
/// <see cref="HttpServer"/> so routing can be tested by calling
/// <see cref="HandleAsync"/> directly, with no socket involved.
///
/// <list type="table">
///   <item><term>GET /health</term><description>liveness probe</description></item>
///   <item><term>GET /stats</term><description>job counts by status</description></item>
///   <item><term>POST /jobs</term><description>enqueue a job</description></item>
///   <item><term>GET /jobs?status=&amp;limit=</term><description>list jobs</description></item>
///   <item><term>GET /jobs/{id}</term><description>one job</description></item>
///   <item><term>POST /jobs/{id}/retry</term><description>requeue a dead-lettered/failed job</description></item>
///   <item><term>DELETE /jobs/{id}</term><description>delete a job</description></item>
///   <item><term>POST /recurring</term><description>register a recurring job</description></item>
///   <item><term>GET /recurring</term><description>list recurring jobs</description></item>
///   <item><term>DELETE /recurring/{id}</term><description>remove a recurring job</description></item>
/// </list>
/// </summary>
public sealed class ApiHandler
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        Converters = { new JsonStringEnumConverter() }
    };

    private readonly JobQueueEngine _engine;
    private readonly RecurringScheduler _scheduler;
    private readonly Func<DateTimeOffset> _clock;

    public ApiHandler(JobQueueEngine engine, RecurringScheduler scheduler, Func<DateTimeOffset>? clock = null)
    {
        _engine = engine;
        _scheduler = scheduler;
        _clock = clock ?? (() => DateTimeOffset.UtcNow);
    }

    public Task<HttpResponse> HandleAsync(HttpRequest request)
    {
        try
        {
            var segments = request.Path.Trim('/').Split('/', StringSplitOptions.RemoveEmptyEntries);
            return Task.FromResult(Route(request, segments));
        }
        catch (KeyNotFoundException ex)
        {
            return Task.FromResult(Error(404, ex.Message));
        }
        catch (ArgumentException ex)
        {
            return Task.FromResult(Error(400, ex.Message));
        }
        catch (InvalidOperationException ex)
        {
            return Task.FromResult(Error(400, ex.Message));
        }
        catch (JsonException ex)
        {
            return Task.FromResult(Error(400, "Invalid JSON: " + ex.Message));
        }
    }

    private HttpResponse Route(HttpRequest request, string[] segments)
    {
        if (segments.Length == 0)
            return Error(404, "Not found. See GET /health.");

        return segments[0] switch
        {
            "health" when request.Method == "GET" => Ok(new { status = "ok" }),
            "stats" when request.Method == "GET" => Stats(),
            "jobs" => Jobs(request, segments),
            "recurring" => Recurring(request, segments),
            _ => Error(404, $"No route for {request.Method} {request.Path}.")
        };
    }

    private HttpResponse Stats()
    {
        var (pending, running, completed, failed, deadLettered) = _engine.Stats();
        return Ok(new
        {
            pending,
            running,
            completed,
            failed,
            deadLettered,
            total = pending + running + completed + failed + deadLettered
        });
    }

    private HttpResponse Jobs(HttpRequest request, string[] segments)
    {
        if (segments.Length == 1)
        {
            if (request.Method == "POST") return Enqueue(request);
            if (request.Method == "GET") return ListJobs(request);
            return Error(405, $"Method {request.Method} not allowed on /jobs.");
        }

        var id = segments[1];
        if (segments.Length == 2)
        {
            if (request.Method == "GET")
            {
                var job = _engine.Get(id) ?? throw new KeyNotFoundException($"No job with id '{id}'.");
                return Ok(job);
            }
            if (request.Method == "DELETE")
            {
                _engine.Delete(id);
                return Ok(new { deleted = id });
            }
            return Error(405, $"Method {request.Method} not allowed on /jobs/{{id}}.");
        }

        if (segments.Length == 3 && segments[2] == "retry" && request.Method == "POST")
            return Ok(_engine.Retry(id));

        return Error(404, $"No route for {request.Method} {request.Path}.");
    }

    private HttpResponse Enqueue(HttpRequest request)
    {
        var body = JsonSerializer.Deserialize<EnqueueRequest>(request.Body, JsonOptions)
                   ?? throw new ArgumentException("Request body must be a JSON object.");
        if (string.IsNullOrWhiteSpace(body.Type))
            throw new ArgumentException("Field \"type\" is required.");
        if (body.MaxAttempts is < 1)
            throw new ArgumentException("Field \"maxAttempts\" must be at least 1.");

        DateTimeOffset? runAt = body.RunAt;
        if (body.DelaySeconds is > 0)
            runAt = _clock().AddSeconds(body.DelaySeconds.Value);

        var job = new Job
        {
            Type = body.Type,
            Payload = string.IsNullOrWhiteSpace(body.Payload) ? "{}" : body.Payload,
            Priority = body.Priority ?? 0,
            MaxAttempts = body.MaxAttempts ?? 3,
            RunAt = runAt
        };
        return Created(_engine.Enqueue(job));
    }

    private HttpResponse ListJobs(HttpRequest request)
    {
        JobStatus? status = null;
        if (request.Query.TryGetValue("status", out var statusText) &&
            !string.IsNullOrWhiteSpace(statusText))
        {
            if (!Enum.TryParse<JobStatus>(statusText, ignoreCase: true, out var parsed))
                throw new ArgumentException($"Unknown status '{statusText}'.");
            status = parsed;
        }

        var limit = 100;
        if (request.Query.TryGetValue("limit", out var limitText) &&
            int.TryParse(limitText, out var parsedLimit) && parsedLimit > 0)
        {
            limit = Math.Min(parsedLimit, 1000);
        }

        return Ok(_engine.List(status).Take(limit).ToList());
    }

    private HttpResponse Recurring(HttpRequest request, string[] segments)
    {
        if (segments.Length == 1)
        {
            if (request.Method == "POST") return AddRecurring(request);
            if (request.Method == "GET") return Ok(_scheduler.List());
            return Error(405, $"Method {request.Method} not allowed on /recurring.");
        }

        if (segments.Length == 2 && request.Method == "DELETE")
        {
            _scheduler.Remove(segments[1]);
            return Ok(new { deleted = segments[1] });
        }

        return Error(404, $"No route for {request.Method} {request.Path}.");
    }

    private HttpResponse AddRecurring(HttpRequest request)
    {
        var body = JsonSerializer.Deserialize<RecurringRequest>(request.Body, JsonOptions)
                   ?? throw new ArgumentException("Request body must be a JSON object.");
        if (string.IsNullOrWhiteSpace(body.Name))
            throw new ArgumentException("Field \"name\" is required.");
        if (string.IsNullOrWhiteSpace(body.Type))
            throw new ArgumentException("Field \"type\" is required.");
        if (body.IntervalSeconds is null or < 1)
            throw new ArgumentException("Field \"intervalSeconds\" must be at least 1.");

        var job = _scheduler.Add(
            body.Name, body.Type, body.Payload ?? "{}",
            body.IntervalSeconds.Value, body.Priority ?? 0, body.MaxAttempts ?? 3);
        return Created(job);
    }

    private static HttpResponse Ok(object value) =>
        HttpResponse.Json(200, JsonSerializer.Serialize(value, JsonOptions));

    private static HttpResponse Created(object value) =>
        HttpResponse.Json(201, JsonSerializer.Serialize(value, JsonOptions));

    private static HttpResponse Error(int statusCode, string message) =>
        HttpResponse.Json(statusCode, JsonSerializer.Serialize(new { error = message }, JsonOptions));

    private sealed record EnqueueRequest(
        string? Type,
        string? Payload,
        int? Priority,
        int? MaxAttempts,
        int? DelaySeconds,
        DateTimeOffset? RunAt);

    private sealed record RecurringRequest(
        string? Name,
        string? Type,
        string? Payload,
        int? IntervalSeconds,
        int? Priority,
        int? MaxAttempts);
}
