using OutboxLab.Core.Measurement;
using OutboxLab.Core.Model;

namespace OutboxLab.Core.Lab;

/// <summary>Scenario kinds the lab runs.</summary>
public static class Scenarios
{
    /// <summary>Pre-filled backlog drained by the relay: maximum relay throughput.</summary>
    public const string Drain = "drain";

    /// <summary>Open-loop fixed publish rate: commit-to-delivery latency.</summary>
    public const string Latency = "latency";

    /// <summary>Concurrent producers with commit reordering while the relay runs: loss, duplicates, order.</summary>
    public const string Ordering = "ordering";

    /// <summary>Closed-loop producers with no relay: what the strategy costs the business transaction.</summary>
    public const string Publish = "publish";
}

/// <summary>The outcome of one scenario run; serialised to <c>benchmark-results/*.json</c>.</summary>
public sealed record ScenarioResult(
    string Scenario,
    string Strategy,
    string Engine,
    int Workers,
    int Producers,
    int Messages,
    double DurationSeconds,
    double ThroughputPerSecond,
    double? OfferedRatePerSecond = null,
    LatencySummary? Latency = null,
    OrderingReport? Ordering = null,
    string? Notes = null);

/// <summary>Settings shared by every scenario run.</summary>
/// <param name="PayloadSize">Payload bytes per message.</param>
/// <param name="BatchSize">Relay claim size, and the producer batch for pre-filling a backlog.</param>
/// <param name="PollInterval">Idle wait for polling relays and wait timeout for push-based relays.</param>
/// <param name="Aggregates">Distinct aggregates the workload writes to.</param>
/// <param name="Seed">Seed for the synthetic workload.</param>
public sealed record ScenarioSettings(
    int PayloadSize = 256,
    int BatchSize = 100,
    TimeSpan? PollInterval = null,
    int Aggregates = 64,
    int Seed = 20261112)
{
    public RelayOptions RelayOptions(int workers) =>
        new(workers, BatchSize, PollInterval ?? TimeSpan.FromMilliseconds(100), PayloadSize);
}
