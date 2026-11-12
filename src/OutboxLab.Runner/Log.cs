using Microsoft.Extensions.Logging;

namespace OutboxLab.Runner;

/// <summary>Source-generated log messages: structured fields, no formatting cost when the level is disabled.</summary>
internal static partial class Log
{
    [LoggerMessage(Level = LogLevel.Information, Message = "{Engine} is ready")]
    public static partial void Ready(ILogger logger, string engine);

    [LoggerMessage(Level = LogLevel.Information, Message = "Waiting for {Engine}: {Reason}")]
    public static partial void Waiting(ILogger logger, string engine, string reason);

    [LoggerMessage(Level = LogLevel.Information, Message = "outbox-lab {Command}: {Steps} configurations x {Runs} run(s). All data is synthetic")]
    public static partial void Starting(ILogger logger, string command, int steps, int runs);

    [LoggerMessage(Level = LogLevel.Information, Message = "{Text}")]
    public static partial void Progress(ILogger logger, string text);

    [LoggerMessage(Level = LogLevel.Information,
        Message = "{Scenario} {Strategy} w={Workers} run {Run}: {Throughput:0}/s p50={P50:0.0} ms missing={Missing} inversions={Inversions}")]
    public static partial void RunFinished(ILogger logger, string scenario, string strategy, int workers, int run, double throughput, double p50, long? missing, long? inversions);

    [LoggerMessage(Level = LogLevel.Information, Message = "Wrote {Directory}/{Name}.json and .md")]
    public static partial void Wrote(ILogger logger, string directory, string name);
}
