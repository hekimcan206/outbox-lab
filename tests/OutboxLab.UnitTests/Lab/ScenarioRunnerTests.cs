using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using OutboxLab.Core.Lab;
using OutboxLab.Core.Model;

namespace OutboxLab.UnitTests.Lab;

public sealed class AggregateSequencerTests
{
    [Property(MaxTest = 100)]
    public Property Producers_own_disjoint_aggregates_and_sequences_have_no_gaps() =>
        Prop.ForAll(Gen.Choose(1, 8).ToArbitrary(), Gen.Choose(0, 40).ToArbitrary(), Gen.Choose(1, 500).ToArbitrary(),
            (producers, extraAggregates, messages) =>
            {
                var sequencers = Enumerable.Range(0, producers)
                    .Select(p => new AggregateSequencer(p, producers, producers + extraAggregates, 1)).ToArray();
                var emitted = sequencers.SelectMany((s, p) => Enumerable.Range(0, messages).Select(_ => (p, e: s.Next(0)))).ToList();

                var ownersPerAggregate = emitted.GroupBy(x => x.e.AggregateId).Select(g => g.Select(x => x.p).Distinct().Count());
                var gapless = emitted.GroupBy(x => x.e.AggregateId)
                    .All(g => g.Select(x => x.e.Sequence).SequenceEqual(Enumerable.Range(1, g.Count()).Select(i => (long)i)));
                var merged = AggregateSequencer.Merge(sequencers);

                return ownersPerAggregate.All(c => c == 1) && gapless && merged.Values.Sum() == producers * messages;
            });

    [Fact]
    public void The_same_seed_produces_the_same_workload()
    {
        var a = new AggregateSequencer(0, 2, 10, 42);
        var b = new AggregateSequencer(0, 2, 10, 42);

        Enumerable.Range(0, 50).Select(_ => a.Next(0)).Should().Equal(Enumerable.Range(0, 50).Select(_ => b.Next(0)));
    }

    [Fact]
    public void Fewer_aggregates_than_producers_is_rejected()
    {
        var act = () => new AggregateSequencer(0, 4, 3, 1);

        act.Should().Throw<ArgumentOutOfRangeException>();
    }
}

public sealed class ScenarioRunnerTests
{
    private static readonly ScenarioSettings Settings = new(PayloadSize: 64, BatchSize: 50, PollInterval: TimeSpan.FromMilliseconds(5), Aggregates: 16);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Drain_delivers_the_whole_backlog_in_order_with_a_single_stream()
    {
        var strategy = new InMemoryStrategy();

        var result = await new ScenarioRunner(Settings).DrainAsync(strategy, workers: 4, messages: 5_000, Ct);

        result.Scenario.Should().Be(Scenarios.Drain);
        result.Workers.Should().Be(1, "a single-stream strategy ignores extra workers");
        result.Ordering!.Unique.Should().Be(5_000);
        result.Ordering.Missing.Should().Be(0);
        result.Ordering.Inversions.Should().Be(0);
        result.ThroughputPerSecond.Should().BePositive();
        strategy.Resets.Should().Be(1);
    }

    [Fact]
    public async Task Drain_with_parallel_workers_loses_nothing()
    {
        var result = await new ScenarioRunner(Settings).DrainAsync(new InMemoryStrategy(parallel: true), workers: 4, messages: 5_000, Ct);

        result.Workers.Should().Be(4);
        result.Ordering!.Missing.Should().Be(0);
        result.Ordering.Duplicates.Should().Be(0);
    }

    [Fact]
    public async Task Latency_measures_every_message_after_the_warm_up()
    {
        var result = await new ScenarioRunner(Settings).LatencyAsync(
            new InMemoryStrategy(), workers: 1, ratePerSecond: 400, TimeSpan.FromSeconds(1), producers: 2, TimeSpan.FromMilliseconds(250), Ct);

        result.Messages.Should().Be(500);
        result.OfferedRatePerSecond.Should().Be(400);
        result.Latency!.Count.Should().BeInRange(300, 500, "messages produced during the warm-up are excluded");
        result.Latency.P50.Should().BeLessThan(1_000);
        result.Ordering!.Missing.Should().Be(0);
    }

    [Fact]
    public async Task Ordering_reports_loss_from_a_lossy_relay_instead_of_hanging()
    {
        var result = await new ScenarioRunner(Settings).OrderingAsync(
            new InMemoryStrategy(dropEvery: 10), workers: 1, messages: 400, producers: 4, TimeSpan.FromMilliseconds(1), Ct);

        result.Ordering!.Missing.Should().Be(40);
        result.Ordering.Unique.Should().Be(360);
    }

    [Fact]
    public async Task Publish_counts_committed_transactions()
    {
        var result = await new ScenarioRunner(Settings).PublishAsync(new InMemoryStrategy(), producers: 2, TimeSpan.FromMilliseconds(200), Ct);

        result.Scenario.Should().Be(Scenarios.Publish);
        result.Messages.Should().BePositive();
        result.Latency!.Count.Should().Be(result.Messages);
    }

    [Fact]
    public async Task A_failing_relay_fails_the_scenario_instead_of_waiting()
    {
        var act = () => new ScenarioRunner(Settings).DrainAsync(new BrokenStrategy(), 1, 10, Ct);

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("relay broke");
    }

    private sealed class BrokenStrategy : IOutboxStrategy
    {
        private readonly InMemoryStrategy _inner = new();

        public string Id => "broken";

        public string Engine => "memory";

        public string OrderingModel => "none";

        public bool SupportsParallelWorkers => false;

        public Task ResetAsync(CancellationToken cancellationToken) => _inner.ResetAsync(cancellationToken);

        public Task<IOutboxProducer> CreateProducerAsync(RelayOptions options, CancellationToken cancellationToken) =>
            _inner.CreateProducerAsync(options, cancellationToken);

        public IOutboxRelay CreateRelay(RelayOptions options) => new Relay();

        private sealed class Relay : IOutboxRelay
        {
            public Task RunAsync(IDeliverySink sink, CancellationToken cancellationToken) =>
                Task.FromException(new InvalidOperationException("relay broke"));
        }
    }
}
