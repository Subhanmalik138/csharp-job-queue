using System.Net;
using System.Text;
using JobQueue.Http;
using JobQueue.Scheduling;
using Xunit;

namespace JobQueue.Tests;

public sealed class ApiTests
{
    private static (ApiHandler Handler, JobQueue.Queue.JobQueueEngine Engine) BuildApi(FakeClock clock)
    {
        var fixtureDir = Path.Combine(Path.GetTempPath(), "jobqueue-tests", Guid.NewGuid().ToString("N"));
        var store = new JobQueue.Store.JsonlJobStore(fixtureDir);
        var recurring = new RecurringJobStore(fixtureDir);
        var engine = Factory.Engine(store, clock);
        var scheduler = new RecurringScheduler(engine, recurring, clock.Func);
        return (new ApiHandler(engine, scheduler, clock.Func), engine);
    }

    private static HttpRequest Request(string method, string path, string body = "") =>
        new() { Method = method, Path = path, Body = body };

    [Fact]
    public async Task Health_ReturnsOk()
    {
        var (api, _) = BuildApi(new FakeClock());
        var response = await api.HandleAsync(Request("GET", "/health"));
        Assert.Equal(200, response.StatusCode);
        Assert.Contains("ok", response.Body);
    }

    [Fact]
    public async Task Enqueue_ThenGet_ThenList_ThenDelete()
    {
        var clock = new FakeClock();
        var (api, _) = BuildApi(clock);

        var created = await api.HandleAsync(Request("POST", "/jobs",
            """{"type":"shell","payload":"{\"command\":\"echo\"}","priority":3}"""));
        Assert.Equal(201, created.StatusCode);
        Assert.Contains("job_", created.Body);

        var list = await api.HandleAsync(Request("GET", "/jobs"));
        Assert.Equal(200, list.StatusCode);
        Assert.Contains("shell", list.Body);

        var id = System.Text.Json.JsonDocument.Parse(created.Body).RootElement.GetProperty("id").GetString()!;
        var got = await api.HandleAsync(Request("GET", $"/jobs/{id}"));
        Assert.Equal(200, got.StatusCode);

        var deleted = await api.HandleAsync(Request("DELETE", $"/jobs/{id}"));
        Assert.Equal(200, deleted.StatusCode);

        var missing = await api.HandleAsync(Request("GET", $"/jobs/{id}"));
        Assert.Equal(404, missing.StatusCode);
    }

    [Fact]
    public async Task Enqueue_ValidatesInput()
    {
        var (api, _) = BuildApi(new FakeClock());

        var noType = await api.HandleAsync(Request("POST", "/jobs", """{"payload":"{}"}"""));
        Assert.Equal(400, noType.StatusCode);

        var badJson = await api.HandleAsync(Request("POST", "/jobs", "{not json"));
        Assert.Equal(400, badJson.StatusCode);

        var badAttempts = await api.HandleAsync(Request("POST", "/jobs", """{"type":"x","maxAttempts":0}"""));
        Assert.Equal(400, badAttempts.StatusCode);
    }

    [Fact]
    public async Task Enqueue_WithDelay_SetsRunAt()
    {
        var clock = new FakeClock();
        var (api, engine) = BuildApi(clock);

        var created = await api.HandleAsync(Request("POST", "/jobs", """{"type":"x","delaySeconds":120}"""));
        Assert.Equal(201, created.StatusCode);

        var id = System.Text.Json.JsonDocument.Parse(created.Body).RootElement.GetProperty("id").GetString()!;
        var job = engine.Get(id);
        Assert.Equal(clock.Now.AddSeconds(120), job?.RunAt);
    }

    [Fact]
    public async Task Retry_DeadLetteredJobViaApi()
    {
        var clock = new FakeClock();
        var (api, engine) = BuildApi(clock);

        var created = await api.HandleAsync(Request("POST", "/jobs", """{"type":"x","maxAttempts":1}"""));
        var id = System.Text.Json.JsonDocument.Parse(created.Body).RootElement.GetProperty("id").GetString()!;

        engine.TryClaimNext();
        engine.Fail(id, "boom");

        var retried = await api.HandleAsync(Request("POST", $"/jobs/{id}/retry"));
        Assert.Equal(200, retried.StatusCode);
        Assert.Contains("Pending", retried.Body);
    }

    [Fact]
    public async Task Stats_ReflectsQueueState()
    {
        var (api, engine) = BuildApi(new FakeClock());
        engine.Enqueue(Factory.NewJob());
        engine.Enqueue(Factory.NewJob());

        var stats = await api.HandleAsync(Request("GET", "/stats"));
        Assert.Equal(200, stats.StatusCode);
        Assert.Contains("\"pending\":2", stats.Body);
    }

    [Fact]
    public async Task Recurring_AddListDeleteViaApi()
    {
        var (api, _) = BuildApi(new FakeClock());

        var created = await api.HandleAsync(Request("POST", "/recurring",
            """{"name":"hourly","type":"shell","intervalSeconds":3600}"""));
        Assert.Equal(201, created.StatusCode);

        var duplicate = await api.HandleAsync(Request("POST", "/recurring",
            """{"name":"hourly","type":"shell","intervalSeconds":60}"""));
        Assert.Equal(400, duplicate.StatusCode);

        var list = await api.HandleAsync(Request("GET", "/recurring"));
        Assert.Contains("hourly", list.Body);

        var id = System.Text.Json.JsonDocument.Parse(created.Body).RootElement.GetProperty("id").GetString()!;
        var deleted = await api.HandleAsync(Request("DELETE", $"/recurring/{id}"));
        Assert.Equal(200, deleted.StatusCode);
    }

    [Fact]
    public async Task UnknownRoute_Returns404()
    {
        var (api, _) = BuildApi(new FakeClock());
        var response = await api.HandleAsync(Request("GET", "/nope"));
        Assert.Equal(404, response.StatusCode);
    }

    [Fact]
    public async Task HttpServer_ServesApiOverRealSocket()
    {
        var clock = new FakeClock();
        var (api, _) = BuildApi(clock);
        using var server = new HttpServer(0, api.HandleAsync);
        server.Start();
        try
        {
            using var client = new HttpClient { BaseAddress = new Uri($"http://127.0.0.1:{server.Port}") };

            var health = await client.GetAsync("/health");
            Assert.Equal(HttpStatusCode.OK, health.StatusCode);

            var post = await client.PostAsync("/jobs",
                new StringContent("""{"type":"shell","payload":"{}"}""", Encoding.UTF8, "application/json"));
            Assert.Equal(HttpStatusCode.Created, post.StatusCode);

            var stats = await client.GetStringAsync("/stats");
            Assert.Contains("\"pending\":1", stats);
        }
        finally
        {
            await server.StopAsync();
        }
    }
}
