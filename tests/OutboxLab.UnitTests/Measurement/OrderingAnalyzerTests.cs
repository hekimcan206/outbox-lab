using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using OutboxLab.Core.Measurement;

namespace OutboxLab.UnitTests.Measurement;

public sealed class OrderingAnalyzerTests
{
    private static readonly Dictionary<int, long> TwoAggregatesOfThree = new() { [1] = 3, [2] = 3 };

    [Fact]
    public void An_in_order_complete_log_is_clean()
    {
        var log = new[] { (1, 1L), (2, 1L), (1, 2L), (2, 2L), (2, 3L), (1, 3L) };

        var report = OrderingAnalyzer.Analyze(log, TwoAggregatesOfThree);

        report.Should().Be(new OrderingReport(6, 6, 0, 0, 0, 0));
        report.LossFree.Should().BeTrue();
        report.InOrder.Should().BeTrue();
    }

    [Fact]
    public void A_late_message_is_one_inversion_on_its_aggregate()
    {
        var log = new[] { (1, 1L), (1, 3L), (1, 2L), (2, 1L), (2, 2L), (2, 3L) };

        var report = OrderingAnalyzer.Analyze(log, TwoAggregatesOfThree);

        report.Inversions.Should().Be(1);
        report.AggregatesWithInversion.Should().Be(1);
        report.Missing.Should().Be(0);
    }

    [Fact]
    public void A_redelivery_is_a_duplicate_not_an_inversion()
    {
        var log = new[] { (1, 1L), (1, 2L), (1, 3L), (1, 2L), (2, 1L), (2, 2L), (2, 3L) };

        var report = OrderingAnalyzer.Analyze(log, TwoAggregatesOfThree);

        report.Duplicates.Should().Be(1);
        report.Inversions.Should().Be(0);
        report.Unique.Should().Be(6);
        report.Delivered.Should().Be(7);
    }

    [Fact]
    public void A_skipped_message_is_missing_even_when_later_ones_arrive()
    {
        var log = new[] { (1, 1L), (1, 3L), (2, 1L), (2, 2L), (2, 3L) };

        var report = OrderingAnalyzer.Analyze(log, TwoAggregatesOfThree);

        report.Missing.Should().Be(1);
        report.LossFree.Should().BeFalse();
    }

    [Fact]
    public void Deliveries_are_analysed_in_sink_order_not_list_order()
    {
        var deliveries = new[]
        {
            new Delivery(1, 1, 2, 0, 0, 0),
            new Delivery(0, 1, 1, 0, 0, 0),
        };

        OrderingAnalyzer.Analyze(deliveries, new Dictionary<int, long> { [1] = 2 }).Inversions.Should().Be(0);
    }

    /// <summary>Any interleaving that keeps each aggregate's own order is reported clean, whatever the mix.</summary>
    [Property(MaxTest = 200)]
    public Property Interleavings_that_keep_per_aggregate_order_have_no_inversions() =>
        Prop.ForAll(Interleaving(), t =>
        {
            var report = OrderingAnalyzer.Analyze(t.Log, t.Expected);
            return report.Inversions == 0 && report.Missing == 0 && report.Duplicates == 0 && report.Unique == t.Log.Count;
        });

    /// <summary>Dropping k distinct messages from a complete log is reported as exactly k missing.</summary>
    [Property(MaxTest = 200)]
    public Property Dropped_messages_are_counted_exactly() =>
        Prop.ForAll(Interleaving(), Gen.Choose(0, 1000).ToArbitrary(), (t, seed) =>
        {
            var random = new Random(seed);
            var kept = t.Log.Where(_ => random.Next(4) != 0).ToList();
            return OrderingAnalyzer.Analyze(kept, t.Expected).Missing == t.Log.Count - kept.Count;
        });

    /// <summary>Delivering the whole log twice doubles deliveries without creating inversions or loss.</summary>
    [Property(MaxTest = 100)]
    public Property Replaying_the_log_only_adds_duplicates() =>
        Prop.ForAll(Interleaving(), t =>
        {
            var report = OrderingAnalyzer.Analyze(t.Log.Concat(t.Log), t.Expected);
            return report.Duplicates == t.Log.Count && report.Inversions == 0 && report.Missing == 0;
        });

    /// <summary>Reversing an aggregate that has n &gt; 1 messages produces n - 1 inversions on it.</summary>
    [Property(MaxTest = 100)]
    public Property Reversing_one_aggregate_counts_every_late_message() =>
        Prop.ForAll(Gen.Choose(2, 50).ToArbitrary(), n =>
        {
            var log = Enumerable.Range(1, n).Reverse().Select(s => (7, (long)s));
            var report = OrderingAnalyzer.Analyze(log, new Dictionary<int, long> { [7] = n });
            return report.Inversions == n - 1 && report.AggregatesWithInversion == 1;
        });

    private static Arbitrary<(List<(int, long)> Log, Dictionary<int, long> Expected)> Interleaving() =>
        (from aggregates in Gen.Choose(1, 8)
         from counts in Gen.ArrayOf(Gen.Choose(1, 20), aggregates)
         from seed in Gen.Choose(0, int.MaxValue)
         select Build(counts, seed)).ToArbitrary();

    private static (List<(int, long)>, Dictionary<int, long>) Build(int[] counts, int seed)
    {
        var random = new Random(seed);
        var next = new long[counts.Length];
        var log = new List<(int, long)>();
        while (log.Count < counts.Sum())
        {
            var a = random.Next(counts.Length);
            if (next[a] < counts[a])
            {
                log.Add((a, ++next[a]));
            }
        }

        return (log, counts.Select((c, a) => (a, (long)c)).ToDictionary(x => x.a, x => x.Item2));
    }
}
