namespace OutboxLab.Core.Measurement;

/// <summary>What a delivery log says about loss, duplication and per-aggregate order.</summary>
/// <param name="Delivered">Deliveries, duplicates included.</param>
/// <param name="Unique">Distinct (aggregate, sequence) pairs delivered.</param>
/// <param name="Duplicates">Deliveries of a pair that had already been delivered.</param>
/// <param name="Missing">Expected pairs never delivered (loss).</param>
/// <param name="Inversions">First deliveries that arrived after a higher sequence of the same aggregate.</param>
/// <param name="AggregatesWithInversion">Aggregates that saw at least one inversion.</param>
public sealed record OrderingReport(long Delivered, long Unique, long Duplicates, long Missing, long Inversions, int AggregatesWithInversion)
{
    public bool LossFree => Missing == 0;

    public bool InOrder => Inversions == 0;
}

public static class OrderingAnalyzer
{
    /// <summary>Analyses deliveries in the order the sink saw them.</summary>
    /// <param name="deliveries">(aggregate, sequence) in delivery order.</param>
    /// <param name="expectedLastSequence">
    /// Per aggregate, the last sequence that was committed; sequences run 1..last without gaps.
    /// </param>
    /// <remarks>
    /// An inversion is counted once per late message. A redelivery of an old message is a duplicate, not an
    /// inversion: a consumer that deduplicates by (aggregate, sequence) never acts on it.
    /// </remarks>
    public static OrderingReport Analyze(IEnumerable<(int AggregateId, long Sequence)> deliveries, IReadOnlyDictionary<int, long> expectedLastSequence)
    {
        ArgumentNullException.ThrowIfNull(deliveries);
        ArgumentNullException.ThrowIfNull(expectedLastSequence);

        var seen = new HashSet<(int, long)>();
        var highest = new Dictionary<int, long>();
        var inverted = new HashSet<int>();
        long delivered = 0, duplicates = 0, inversions = 0;

        foreach (var (aggregate, sequence) in deliveries)
        {
            delivered++;
            if (!seen.Add((aggregate, sequence)))
            {
                duplicates++;
                continue;
            }

            if (highest.TryGetValue(aggregate, out var max) && sequence < max)
            {
                inversions++;
                inverted.Add(aggregate);
            }
            else
            {
                highest[aggregate] = sequence;
            }
        }

        long missing = 0;
        foreach (var (aggregate, last) in expectedLastSequence)
        {
            for (long s = 1; s <= last; s++)
            {
                if (!seen.Contains((aggregate, s)))
                {
                    missing++;
                }
            }
        }

        return new OrderingReport(delivered, seen.Count, duplicates, missing, inversions, inverted.Count);
    }

    public static OrderingReport Analyze(IEnumerable<Delivery> deliveries, IReadOnlyDictionary<int, long> expectedLastSequence) =>
        Analyze(deliveries.OrderBy(d => d.Index).Select(d => (d.AggregateId, d.Sequence)), expectedLastSequence);
}
