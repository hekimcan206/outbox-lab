namespace OutboxLab.Core.Measurement;

/// <summary>Latency distribution in milliseconds. Percentiles use the nearest-rank method on the exact samples.</summary>
public sealed record LatencySummary(int Count, double P50, double P95, double P99, double Max, double Mean)
{
    public static LatencySummary From(IEnumerable<double> samplesMs)
    {
        ArgumentNullException.ThrowIfNull(samplesMs);
        var sorted = samplesMs.ToArray();
        if (sorted.Length == 0)
        {
            return new LatencySummary(0, double.NaN, double.NaN, double.NaN, double.NaN, double.NaN);
        }

        Array.Sort(sorted);
        return new LatencySummary(
            sorted.Length,
            Percentile(sorted, 50),
            Percentile(sorted, 95),
            Percentile(sorted, 99),
            sorted[^1],
            sorted.Average());
    }

    /// <summary>Nearest rank: the smallest sample such that at least <paramref name="percent"/>% are less or equal.</summary>
    internal static double Percentile(double[] sorted, double percent)
    {
        var rank = (int)Math.Ceiling(percent / 100.0 * sorted.Length);
        return sorted[Math.Clamp(rank, 1, sorted.Length) - 1];
    }
}
