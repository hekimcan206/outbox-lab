using OutboxLab.Core.Model;

namespace OutboxLab.Core.Lab;

/// <summary>
/// Synthetic workload for one producer. Producer <c>p</c> of <c>P</c> owns the aggregates with <c>id % P == p</c>, so
/// each aggregate is written by exactly one producer, one transaction at a time. That makes the commit order of an
/// aggregate's messages equal to its sequence order, which is the order a correct relay must preserve.
/// </summary>
public sealed class AggregateSequencer
{
    private readonly int[] _owned;
    private readonly Dictionary<int, long> _last = [];
    private readonly Random _random;

    public AggregateSequencer(int producer, int producers, int aggregates, int seed)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(producers);
        ArgumentOutOfRangeException.ThrowIfLessThan(aggregates, producers);
        ArgumentOutOfRangeException.ThrowIfNegative(producer);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(producer, producers);

        _owned = [.. Enumerable.Range(0, aggregates).Where(a => a % producers == producer)];
        _random = new Random(HashCode.Combine(seed, producer));
    }

    /// <summary>Last sequence handed out per aggregate (sequences start at 1 and have no gaps).</summary>
    public IReadOnlyDictionary<int, long> LastSequences => _last;

    /// <summary>Next message for a randomly chosen owned aggregate.</summary>
    public OutboxEvent Next(long producedAt)
    {
        var aggregate = _owned[_random.Next(_owned.Length)];
        var sequence = _last.TryGetValue(aggregate, out var s) ? s + 1 : 1;
        _last[aggregate] = sequence;
        return new OutboxEvent(aggregate, sequence, producedAt);
    }

    /// <summary>Merges the expectations of several producers (their aggregate sets are disjoint).</summary>
    public static IReadOnlyDictionary<int, long> Merge(IEnumerable<AggregateSequencer> sequencers)
    {
        var merged = new Dictionary<int, long>();
        foreach (var sequencer in sequencers)
        {
            foreach (var (aggregate, last) in sequencer._last)
            {
                merged.Add(aggregate, last);
            }
        }

        return merged;
    }
}
