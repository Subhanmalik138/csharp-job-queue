// Placeholder CLI — full commands land in the HTTP API + CLI stage.
Console.WriteLine("jobqueue — persistent distributed job queue (C#)");
Console.WriteLine("Usage: jobqueue server [--data <dir>] [--port <n>]");
Console.WriteLine("       jobqueue enqueue --type <name> --payload '<json>'");
Console.WriteLine("       jobqueue list [--status <Pending|Running|Completed|Failed|DeadLettered>]");
Console.WriteLine("       jobqueue stats");
return 0;
