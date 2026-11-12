using OutboxLab.Core.Lab;
using OutboxLab.Core.Model;

namespace OutboxLab.Runner;

/// <summary>One measured configuration: a scenario on one strategy, repeated and summarised by its median run.</summary>
/// <param name="Scenario">Scenario kind (see <see cref="Scenarios"/>).</param>
/// <param name="StrategyId">Strategy to run.</param>
/// <param name="Run">Runs the scenario once.</param>
/// <param name="MedianKey">The value whose median run represents the configuration.</param>
internal sealed record LabStep(
    string Scenario,
    string StrategyId,
    Func<ScenarioRunner, IOutboxStrategy, CancellationToken, Task<ScenarioResult>> Run,
    Func<ScenarioResult, double> MedianKey);

/// <summary>The demo and the benchmark matrix. Sizes are in one place so docs/benchmarks.md can quote them.</summary>
internal static class LabPlans
{
    /// <summary>Strategies that must keep per-aggregate order in the demo (single ordered reader).</summary>
    public static readonly HashSet<string> OrderedInDemo = ["pg-xmin", "pg-logical", "ora-aq"];

    private static readonly int[] LatencyRates = [100, 500];
    private static readonly int[] PublishProducers = [1, 8, 32];

    public static IReadOnlyList<LabStep> Demo() =>
    [
        Drain("pg-skip-locked", 4, 5_000),
        Drain("pg-xmin", 1, 5_000),
        Drain("pg-logical", 1, 5_000),
        Drain("ora-skip-locked", 4, 5_000),
        Drain("ora-aq", 4, 5_000),
        Ordering("pg-id-cursor", 1, 1_500, 6, 3),
        Ordering("pg-skip-locked", 4, 1_500, 6, 3),
        Ordering("pg-xmin", 1, 1_500, 6, 3),
        Ordering("pg-logical", 1, 1_500, 6, 3),
        Ordering("ora-skip-locked", 4, 1_500, 6, 3),
        Ordering("ora-aq", 1, 1_500, 6, 3),
        Latency("pg-skip-locked", 1, 100, 5, 1),
        Latency("pg-notify", 1, 100, 5, 1),
        Latency("pg-logical", 1, 100, 5, 1),
        Latency("ora-aq", 1, 100, 5, 1),
    ];

    public static IReadOnlyList<LabStep> Bench(string scenario) => scenario switch
    {
        Scenarios.Drain =>
        [
            Drain("pg-skip-locked", 1, 20_000), Drain("pg-skip-locked", 4, 20_000),
            Drain("pg-xmin", 1, 20_000), Drain("pg-logical", 1, 20_000),
            Drain("ora-skip-locked", 1, 20_000), Drain("ora-skip-locked", 4, 20_000),
            Drain("ora-aq", 1, 20_000), Drain("ora-aq", 4, 20_000),
        ],
        Scenarios.Latency =>
        [
            .. LatencyRates.SelectMany(rate => new[]
            {
                Latency("pg-skip-locked", 1, rate, 15, 2), Latency("pg-notify", 1, rate, 15, 2),
                Latency("pg-xmin", 1, rate, 15, 2), Latency("pg-logical", 1, rate, 15, 2),
                Latency("ora-skip-locked", 1, rate, 15, 2), Latency("ora-aq", 1, rate, 15, 2),
            }),
        ],
        Scenarios.Ordering =>
        [
            Ordering("pg-id-cursor", 1, 4_000, 8, 5),
            Ordering("pg-skip-locked", 1, 4_000, 8, 5), Ordering("pg-skip-locked", 4, 4_000, 8, 5),
            Ordering("pg-xmin", 1, 4_000, 8, 5), Ordering("pg-logical", 1, 4_000, 8, 5),
            Ordering("ora-skip-locked", 1, 4_000, 8, 5), Ordering("ora-skip-locked", 4, 4_000, 8, 5),
            Ordering("ora-aq", 1, 4_000, 8, 5), Ordering("ora-aq", 4, 4_000, 8, 5),
        ],
        Scenarios.Publish =>
        [
            .. PublishProducers.SelectMany(producers => new[]
            {
                Publish("pg-skip-locked", producers, 8), Publish("pg-notify", producers, 8), Publish("pg-logical", producers, 8),
                Publish("ora-skip-locked", producers, 8), Publish("ora-aq", producers, 8),
            }),
        ],
        _ => throw new ArgumentException($"Unknown scenario '{scenario}'.", nameof(scenario)),
    };

    private static LabStep Drain(string id, int workers, int messages) =>
        new(Scenarios.Drain, id, (r, s, ct) => r.DrainAsync(s, workers, messages, ct), x => x.ThroughputPerSecond);

    private static LabStep Ordering(string id, int workers, int messages, int producers, int maxHoldMs) =>
        new(Scenarios.Ordering, id, (r, s, ct) => r.OrderingAsync(s, workers, messages, producers, TimeSpan.FromMilliseconds(maxHoldMs), ct),
            x => (x.Ordering?.Inversions ?? 0) + (x.Ordering?.Missing ?? 0));

    private static LabStep Latency(string id, int workers, int rate, int seconds, int warmupSeconds) =>
        new(Scenarios.Latency, id, (r, s, ct) => r.LatencyAsync(s, workers, rate, TimeSpan.FromSeconds(seconds), 4, TimeSpan.FromSeconds(warmupSeconds), ct),
            x => x.Latency?.P50 ?? double.NaN);

    private static LabStep Publish(string id, int producers, int seconds) =>
        new(Scenarios.Publish, id, (r, s, ct) => r.PublishAsync(s, producers, TimeSpan.FromSeconds(seconds), ct), x => x.ThroughputPerSecond);
}
