using System.Diagnostics;
using System.Text.Json;
using JobQueue.Jobs;

namespace JobQueue.Workers;

/// <summary>
/// Runs a job by spawning an external command. The payload is JSON of the form
/// <c>{ "command": "python3", "args": ["script.py"], "input": "stdin text" }</c>.
/// The payload is also written to the process's standard input when no explicit
/// <c>input</c> is given, so a handler script can read its job either way.
/// Exit code 0 means success; anything else (or a timeout) fails the attempt.
/// </summary>
public sealed class ShellJobHandler : IJobHandler
{
    public TimeSpan Timeout { get; init; } = TimeSpan.FromMinutes(5);

    public async Task<bool> HandleAsync(Job job, CancellationToken cancellationToken)
    {
        using var document = JsonDocument.Parse(job.Payload);
        var root = document.RootElement;
        if (!root.TryGetProperty("command", out var commandElement) ||
            commandElement.ValueKind != JsonValueKind.String ||
            string.IsNullOrWhiteSpace(commandElement.GetString()))
        {
            throw new InvalidOperationException(
                $"Shell job '{job.Id}' has no \"command\" in its payload.");
        }

        var command = commandElement.GetString()!;
        var args = root.TryGetProperty("args", out var argsElement) &&
                   argsElement.ValueKind == JsonValueKind.Array
            ? argsElement.EnumerateArray()
                .Where(a => a.ValueKind == JsonValueKind.String)
                .Select(a => a.GetString()!)
                .ToList()
            : [];
        var input = root.TryGetProperty("input", out var inputElement) &&
                    inputElement.ValueKind == JsonValueKind.String
            ? inputElement.GetString()
            : job.Payload;

        var startInfo = new ProcessStartInfo
        {
            FileName = command,
            RedirectStandardInput = true,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false
        };
        foreach (var arg in args)
            startInfo.ArgumentList.Add(arg);

        using var process = new Process { StartInfo = startInfo };
        process.Start();

        await process.StandardInput.WriteAsync(input ?? "");
        process.StandardInput.Close();

        using var timeout = new CancellationTokenSource(Timeout);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeout.Token);
        try
        {
            await process.WaitForExitAsync(linked.Token);
        }
        catch (OperationCanceledException) when (timeout.IsCancellationRequested)
        {
            try { process.Kill(entireProcessTree: true); } catch (InvalidOperationException) { }
            throw new TimeoutException($"Shell job '{job.Id}' timed out after {Timeout}.");
        }

        if (process.ExitCode != 0)
        {
            var stderr = await process.StandardError.ReadToEndAsync(cancellationToken);
            throw new InvalidOperationException(
                $"Command '{command}' exited with code {process.ExitCode}." +
                (string.IsNullOrWhiteSpace(stderr) ? "" : $" stderr: {stderr.Trim()}"));
        }

        return true;
    }
}
