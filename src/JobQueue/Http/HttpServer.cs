using System.Net;
using System.Net.Sockets;
using System.Text;

namespace JobQueue.Http;

/// <summary>One parsed HTTP request.</summary>
public sealed class HttpRequest
{
    public required string Method { get; init; }
    public required string Path { get; init; }
    public Dictionary<string, string> Query { get; init; } = new(StringComparer.OrdinalIgnoreCase);
    public Dictionary<string, string> Headers { get; init; } = new(StringComparer.OrdinalIgnoreCase);
    public string Body { get; init; } = "";
}

/// <summary>One HTTP response to send back.</summary>
public sealed class HttpResponse
{
    public required int StatusCode { get; init; }
    public string Body { get; init; } = "";
    public string ContentType { get; init; } = "application/json";

    public static HttpResponse Json(int statusCode, string body) => new() { StatusCode = statusCode, Body = body };
    public static HttpResponse Text(int statusCode, string body) => new() { StatusCode = statusCode, Body = body, ContentType = "text/plain" };
}

/// <summary>
/// A tiny dependency-free HTTP/1.1 server on top of <see cref="TcpListener"/>.
/// One request per connection, <c>Content-Length</c> bodies only — enough for
/// a JSON control API without pulling in ASP.NET Core. The handler is a plain
/// function so the routing logic stays unit-testable without a socket.
/// </summary>
public sealed class HttpServer : IDisposable
{
    private readonly Func<HttpRequest, Task<HttpResponse>> _handler;
    private readonly TcpListener _listener;
    private CancellationTokenSource? _cts;
    private Task? _acceptLoop;
    private bool _disposed;

    public HttpServer(int port, Func<HttpRequest, Task<HttpResponse>> handler)
    {
        _handler = handler;
        _listener = new TcpListener(IPAddress.Loopback, port);
    }

    /// <summary>The port actually bound (useful when 0 was requested for a free port).</summary>
    public int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

    public bool IsRunning => _acceptLoop is { IsCompleted: false };

    public void Start()
    {
        if (IsRunning) return;
        _listener.Start();
        _cts = new CancellationTokenSource();
        _acceptLoop = Task.Run(() => AcceptLoopAsync(_cts.Token));
    }

    public async Task StopAsync()
    {
        if (_cts is null) return;
        await _cts.CancelAsync();
        _listener.Stop();
        if (_acceptLoop is not null)
        {
            try { await _acceptLoop; } catch (OperationCanceledException) { }
        }
        _cts.Dispose();
        _cts = null;
        _acceptLoop = null;
    }

    private async Task AcceptLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            TcpClient client;
            try
            {
                client = await _listener.AcceptTcpClientAsync(cancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (SocketException)
            {
                break;
            }
            _ = Task.Run(() => HandleClientAsync(client, cancellationToken), cancellationToken);
        }
    }

    private async Task HandleClientAsync(TcpClient client, CancellationToken cancellationToken)
    {
        using (client)
        using (var stream = client.GetStream())
        {
            HttpResponse response;
            try
            {
                var request = await ReadRequestAsync(stream, cancellationToken);
                response = request is null
                    ? HttpResponse.Text(400, "Bad request")
                    : await _handler(request);
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (Exception ex)
            {
                response = HttpResponse.Json(500, $$"""{"error":"{{Escape(ex.Message)}}"}""");
            }

            var bytes = Encoding.UTF8.GetBytes(response.Body);
            var head = $"HTTP/1.1 {response.StatusCode} {Reason(response.StatusCode)}\r\n" +
                       $"Content-Type: {response.ContentType}\r\n" +
                       $"Content-Length: {bytes.Length}\r\n" +
                       "Connection: close\r\n\r\n";
            var headBytes = Encoding.UTF8.GetBytes(head);
            try
            {
                await stream.WriteAsync(headBytes, cancellationToken);
                await stream.WriteAsync(bytes, cancellationToken);
                await stream.FlushAsync(cancellationToken);
            }
            catch (Exception)
            {
                // Client went away mid-response; nothing to do.
            }
        }
    }

    private static async Task<HttpRequest?> ReadRequestAsync(NetworkStream stream, CancellationToken cancellationToken)
    {
        var headerBytes = new List<byte>();
        var buffer = new byte[1];
        var foundEnd = false;
        while (!foundEnd)
        {
            var read = await stream.ReadAsync(buffer, cancellationToken);
            if (read == 0) return null;
            headerBytes.Add(buffer[0]);
            var n = headerBytes.Count;
            if (n >= 4 &&
                headerBytes[n - 4] == (byte)'\r' && headerBytes[n - 3] == (byte)'\n' &&
                headerBytes[n - 2] == (byte)'\r' && headerBytes[n - 1] == (byte)'\n')
            {
                foundEnd = true;
            }
            if (headerBytes.Count > 64 * 1024) return null; // absurd header block
        }

        var headerText = Encoding.UTF8.GetString(headerBytes.ToArray());
        var lines = headerText.Split(["\r\n"], StringSplitOptions.RemoveEmptyEntries);
        if (lines.Length == 0) return null;
        var requestLine = lines[0].Split(' ');
        if (requestLine.Length < 2) return null;

        var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var line in lines.Skip(1))
        {
            var colon = line.IndexOf(':');
            if (colon > 0)
                headers[line[..colon].Trim()] = line[(colon + 1)..].Trim();
        }

        var body = "";
        if (headers.TryGetValue("Content-Length", out var lengthText) &&
            int.TryParse(lengthText, out var length) && length > 0)
        {
            if (length > 16 * 1024 * 1024) return null; // refuse >16MB bodies
            var bodyBytes = new byte[length];
            var readSoFar = 0;
            while (readSoFar < length)
            {
                var read = await stream.ReadAsync(bodyBytes.AsMemory(readSoFar, length - readSoFar), cancellationToken);
                if (read == 0) break;
                readSoFar += read;
            }
            body = Encoding.UTF8.GetString(bodyBytes, 0, readSoFar);
        }

        var uri = new Uri("http://localhost" + requestLine[1]);
        var query = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var rawQuery = uri.Query;
        if (rawQuery.Length > 1)
        {
            foreach (var pair in rawQuery[1..].Split('&', StringSplitOptions.RemoveEmptyEntries))
            {
                var eq = pair.IndexOf('=');
                if (eq > 0)
                    query[Uri.UnescapeDataString(pair[..eq])] = Uri.UnescapeDataString(pair[(eq + 1)..]);
                else
                    query[Uri.UnescapeDataString(pair)] = "";
            }
        }

        return new HttpRequest
        {
            Method = requestLine[0].ToUpperInvariant(),
            Path = uri.AbsolutePath,
            Query = query,
            Headers = headers,
            Body = body
        };
    }

    private static string Reason(int statusCode) => statusCode switch
    {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        500 => "Internal Server Error",
        _ => "OK"
    };

    private static string Escape(string value) =>
        value.Replace("\\", "\\\\").Replace("\"", "\\\"").Replace("\n", "\\n").Replace("\r", "\\r");

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _cts?.Cancel();
        _cts?.Dispose();
        _listener.Stop();
    }
}
