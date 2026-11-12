using System.Globalization;
using System.Text;

namespace OutboxLab.Core.Lab;

/// <summary>
/// Renders scenario results as the Markdown tables used in the README and docs/benchmarks.md. Output is
/// culture-invariant and stable so it can be golden-tested.
/// </summary>
public static class ReportWriter
{
    public static string ToMarkdown(IEnumerable<ScenarioResult> results)
    {
        ArgumentNullException.ThrowIfNull(results);
        var all = results.ToList();
        var sb = new StringBuilder();

        Section(sb, all, Scenarios.Drain, "Drain throughput (pre-filled backlog)",
            "| Strategy | Engine | Workers | Messages | Seconds | Messages/s | Inversions | Duplicates | Missing |",
            "|---|---|---:|---:|---:|---:|---:|---:|---:|",
            r => $"| {r.Strategy} | {r.Engine} | {r.Workers} | {N(r.Messages)} | {F(r.DurationSeconds, 2)} | {N(r.ThroughputPerSecond)} | {N(r.Ordering?.Inversions)} | {N(r.Ordering?.Duplicates)} | {N(r.Ordering?.Missing)} |");

        Section(sb, all, Scenarios.Latency, "Commit-to-delivery latency (open loop)",
            "| Strategy | Engine | Workers | Offered/s | Achieved/s | Samples | p50 ms | p95 ms | p99 ms | max ms | Notes |",
            "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|",
            r => $"| {r.Strategy} | {r.Engine} | {r.Workers} | {N(r.OfferedRatePerSecond)} | {N(r.ThroughputPerSecond)} | {N(r.Latency?.Count)} | {F(r.Latency?.P50, 1)} | {F(r.Latency?.P95, 1)} | {F(r.Latency?.P99, 1)} | {F(r.Latency?.Max, 1)} | {r.Notes} |");

        Section(sb, all, Scenarios.Ordering, "Ordering, loss and duplicates under commit reordering",
            "| Strategy | Engine | Workers | Messages | Delivered | Missing | Duplicates | Inversions | Aggregates hit | Notes |",
            "|---|---|---:|---:|---:|---:|---:|---:|---:|---|",
            r => $"| {r.Strategy} | {r.Engine} | {r.Workers} | {N(r.Messages)} | {N(r.Ordering?.Unique)} | {N(r.Ordering?.Missing)} | {N(r.Ordering?.Duplicates)} | {N(r.Ordering?.Inversions)} | {N(r.Ordering?.AggregatesWithInversion)} | {r.Notes} |");

        Section(sb, all, Scenarios.Publish, "Publish cost (closed loop, no relay)",
            "| Strategy | Engine | Producers | Transactions | Transactions/s | p50 ms | p99 ms |",
            "|---|---|---:|---:|---:|---:|---:|",
            r => $"| {r.Strategy} | {r.Engine} | {r.Producers} | {N(r.Messages)} | {N(r.ThroughputPerSecond)} | {F(r.Latency?.P50, 2)} | {F(r.Latency?.P99, 2)} |");

        return sb.ToString();
    }

    private static void Section(StringBuilder sb, List<ScenarioResult> all, string scenario, string title, string header, string rule, Func<ScenarioResult, string> row)
    {
        var rows = all.Where(r => r.Scenario == scenario).ToList();
        if (rows.Count == 0)
        {
            return;
        }

        if (sb.Length > 0)
        {
            sb.Append('\n');
        }

        sb.Append("### ").Append(title).Append("\n\n").Append(header).Append('\n').Append(rule).Append('\n');
        foreach (var r in rows)
        {
            sb.Append(row(r)).Append('\n');
        }
    }

    private static string N(double? value) =>
        value is null || double.IsNaN(value.Value) ? "-" : Math.Round(value.Value).ToString("#,0", CultureInfo.InvariantCulture);

    private static string F(double? value, int decimals) =>
        value is null || double.IsNaN(value.Value) ? "-" : value.Value.ToString("#,0." + new string('0', decimals), CultureInfo.InvariantCulture);
}
