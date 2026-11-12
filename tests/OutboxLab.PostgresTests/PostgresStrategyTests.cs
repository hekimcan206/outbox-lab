using Npgsql;
using OutboxLab.Core.Lab;
using OutboxLab.Core.Measurement;
using OutboxLab.Core.Model;
using OutboxLab.Postgres;

namespace OutboxLab.PostgresTests;

/// <summary>
/// Each strategy's delivery semantics, step by step, against a real PostgreSQL 17: what is delivered, when, in what
/// order, and what happens when a relay dies before it acknowledges.
/// </summary>
public sealed class PostgresStrategyTests(PostgresFixture db)
{
    private static readonly RelayOptions Fast = new(Workers: 1, BatchSize: 100, PollInterval: TimeSpan.FromMilliseconds(20), PayloadSize: 128);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Id_cursor_skips_a_message_that_commits_behind_the_cursor()
    {
        await using var strategy = db.Strategy(PgStrategyKind.IdCursor);
        await strategy.ResetAsync(Ct);
        var recorder = new DeliveryRecorder();
        await using var relay = RelayHost.Start(strategy, Fast, recorder);

        var (slow, fast) = await CommitOutOfOrderAsync(strategy);

        (await recorder.WaitForUniqueAsync(1, TimeSpan.FromSeconds(10), Ct)).Should().BeTrue();
        await slow;
        await Task.Delay(TimeSpan.FromSeconds(1), Ct);

        var delivered = recorder.Snapshot().Select(d => d.AggregateId).ToList();
        delivered.Should().Equal([fast.AggregateId], "id 1 committed after the cursor had moved past it to id 2: lost for good");
    }

    [Fact]
    public async Task Xmin_relay_waits_for_the_older_transaction_and_then_delivers_both_in_transaction_order()
    {
        await using var strategy = db.Strategy(PgStrategyKind.Xmin);
        await strategy.ResetAsync(Ct);
        var recorder = new DeliveryRecorder();
        await using var relay = RelayHost.Start(strategy, Fast, recorder);

        var (slow, _) = await CommitOutOfOrderAsync(strategy);
        await Task.Delay(TimeSpan.FromMilliseconds(500), Ct);
        recorder.Count.Should().Be(0, "the open transaction holds the commit horizon back, so the newer row must wait too");

        await slow;
        (await recorder.WaitForUniqueAsync(2, TimeSpan.FromSeconds(10), Ct)).Should().BeTrue();
        recorder.Snapshot().Select(d => d.AggregateId).Should().Equal([1, 2], "(txid, id) order: the slow transaction wrote first");
    }

    [Fact]
    public async Task Logical_decoding_delivers_in_commit_order_and_never_delivers_a_rollback()
    {
        await using var strategy = db.Strategy(PgStrategyKind.Logical);
        await strategy.ResetAsync(Ct);
        var recorder = new DeliveryRecorder();
        await using var relay = RelayHost.Start(strategy, Fast, recorder);

        await using (var doomed = await strategy.CreateProducerAsync(Fast, Ct))
        {
            using var abort = CancellationTokenSource.CreateLinkedTokenSource(Ct);
            var rolledBack = doomed.PublishAsync([new OutboxEvent(99, 1, LabClock.Now())], TimeSpan.FromSeconds(30), abort.Token);
            await Task.Delay(200, Ct);
            await abort.CancelAsync();
            await FluentActions.Awaiting(() => rolledBack).Should().ThrowAsync<OperationCanceledException>();
        }

        var (slow, _) = await CommitOutOfOrderAsync(strategy);
        await slow;

        (await recorder.WaitForUniqueAsync(2, TimeSpan.FromSeconds(10), Ct)).Should().BeTrue();
        await Task.Delay(300, Ct);
        recorder.Snapshot().Select(d => d.AggregateId).Should().Equal([2, 1], "the WAL stream follows commit order, not write order; aggregate 99 rolled back");
    }

    [Fact]
    public async Task Logical_relay_resumes_from_the_slot_after_a_restart_without_loss()
    {
        await using var strategy = db.Strategy(PgStrategyKind.Logical);
        await strategy.ResetAsync(Ct);
        await using var producer = await strategy.CreateProducerAsync(Fast, Ct);

        var first = new DeliveryRecorder();
        await using (RelayHost.Start(strategy, Fast, first))
        {
            await PublishAsync(producer, aggregate: 1, from: 1, count: 100);
            (await first.WaitForUniqueAsync(100, TimeSpan.FromSeconds(15), Ct)).Should().BeTrue();
            await Task.Delay(TimeSpan.FromSeconds(1.5), Ct); // let one status update reach the server
        }

        await PublishAsync(producer, aggregate: 1, from: 101, count: 100);
        var second = new DeliveryRecorder();
        await using (RelayHost.Start(strategy, Fast, second))
        {
            (await second.WaitForUniqueAsync(100, TimeSpan.FromSeconds(15), Ct)).Should().BeTrue();
            await Task.Delay(500, Ct);
        }

        var all = first.Snapshot().Concat(second.Snapshot()).Select(d => (d.AggregateId, d.Sequence));
        var report = OrderingAnalyzer.Analyze(all, new Dictionary<int, long> { [1] = 200 });
        report.Missing.Should().Be(0);
        report.Inversions.Should().Be(0);
        second.Snapshot().Should().Contain(d => d.Sequence == 101).And.Contain(d => d.Sequence == 200);
    }

    [Fact]
    public async Task Skip_locked_relay_that_crashes_before_commit_leaves_every_message_for_redelivery()
    {
        await using var strategy = db.Strategy(PgStrategyKind.SkipLocked);
        await strategy.ResetAsync(Ct);
        await using (var producer = await strategy.CreateProducerAsync(Fast, Ct))
        {
            await PublishAsync(producer, aggregate: 5, from: 1, count: 50, batch: 50);
        }

        var crashing = new FailingSink(failOnDelivery: 10);
        var crashed = RelayHost.Start(strategy, Fast with { BatchSize = 50 }, crashing);
        await FluentActions.Awaiting(() => crashed.Completion).Should().ThrowAsync<InvalidOperationException>();
        await crashed.DisposeAsync();
        crashing.Recorder.Count.Should().Be(9, "nine messages reached the broker before the crash");

        var recovered = new DeliveryRecorder();
        await using (RelayHost.Start(strategy, Fast, recovered))
        {
            (await recovered.WaitForUniqueAsync(50, TimeSpan.FromSeconds(10), Ct)).Should().BeTrue();
        }

        recovered.Count.Should().Be(50, "the crashed transaction rolled back, so all 50 rows were still there");
        // The nine early deliveries are now duplicates downstream: consumers must deduplicate (at-least-once).
    }

    [Fact]
    public async Task Notify_wakes_an_idle_relay_long_before_its_poll_interval_while_plain_polling_sleeps()
    {
        var slowPoll = Fast with { PollInterval = TimeSpan.FromSeconds(10) };

        await using var notify = db.Strategy(PgStrategyKind.Notify);
        await notify.ResetAsync(Ct);
        var woken = new DeliveryRecorder();
        await using (RelayHost.Start(notify, slowPoll, woken))
        {
            await Task.Delay(TimeSpan.FromSeconds(1), Ct); // the relay has drained nothing and is now waiting
            await using var producer = await notify.CreateProducerAsync(slowPoll, Ct);
            await PublishAsync(producer, aggregate: 1, from: 1, count: 1);
            (await woken.WaitForUniqueAsync(1, TimeSpan.FromSeconds(3), Ct)).Should().BeTrue();
        }

        var latency = LabClock.MillisecondsBetween(woken.Snapshot()[0].ProducedAt, woken.Snapshot()[0].DeliveredAt);
        latency.Should().BeLessThan(1_000);

        await using var polling = db.Strategy(PgStrategyKind.SkipLocked);
        await polling.ResetAsync(Ct);
        var sleeping = new DeliveryRecorder();
        await using (RelayHost.Start(polling, slowPoll, sleeping))
        {
            await Task.Delay(TimeSpan.FromSeconds(1), Ct);
            await using var producer = await polling.CreateProducerAsync(slowPoll, Ct);
            await PublishAsync(producer, aggregate: 1, from: 1, count: 1);
            (await sleeping.WaitForUniqueAsync(1, TimeSpan.FromSeconds(2), Ct)).Should().BeFalse("the poller sleeps 10 s");
        }
    }

    [Fact]
    public async Task Roles_are_least_privilege()
    {
        await using (var app = new NpgsqlConnection(db.Connections.App))
        {
            await app.OpenAsync(Ct);
            var read = () => new NpgsqlCommand("SELECT count(*) FROM outbox_lab.outbox", app).ExecuteScalarAsync(Ct);
            (await read.Should().ThrowAsync<PostgresException>()).Which.SqlState.Should().Be(PostgresErrorCodes.InsufficientPrivilege);
        }

        await using (var relay = new NpgsqlConnection(db.Connections.Relay))
        {
            await relay.OpenAsync(Ct);
            var read = () => new NpgsqlCommand("SELECT count(*) FROM outbox_lab.orders", relay).ExecuteScalarAsync(Ct);
            (await read.Should().ThrowAsync<PostgresException>()).Which.SqlState.Should().Be(PostgresErrorCodes.InsufficientPrivilege);
            var insert = () => new NpgsqlCommand("INSERT INTO outbox_lab.outbox (aggregate_id, aggregate_seq, payload) VALUES (1, 1, '\\x00')", relay).ExecuteNonQueryAsync(Ct);
            (await insert.Should().ThrowAsync<PostgresException>()).Which.SqlState.Should().Be(PostgresErrorCodes.InsufficientPrivilege);
        }
    }

    /// <summary>
    /// Transaction A (aggregate 1) writes first and stays open; transaction B (aggregate 2) writes second and commits at
    /// once. Returns A's completion (it commits after about 1.5 s) and B's event.
    /// </summary>
    private static async Task<(Task SlowCommit, OutboxEvent Fast)> CommitOutOfOrderAsync(PgStrategy strategy)
    {
        var slowProducer = await strategy.CreateProducerAsync(Fast, Ct);
        var slow = Task.Run(async () =>
        {
            await using (slowProducer)
            {
                await slowProducer.PublishAsync([new OutboxEvent(1, 1, LabClock.Now())], TimeSpan.FromSeconds(1.5), Ct);
            }
        }, Ct);
        await Task.Delay(300, Ct); // A has inserted (and taken the lower id and txid) and is holding its transaction

        var fast = new OutboxEvent(2, 1, LabClock.Now());
        await using (var fastProducer = await strategy.CreateProducerAsync(Fast, Ct))
        {
            await fastProducer.PublishAsync([fast], TimeSpan.Zero, Ct);
        }

        return (slow, fast);
    }

    private static async Task PublishAsync(IOutboxProducer producer, int aggregate, long from, int count, int batch = 1)
    {
        for (var s = from; s < from + count; s += batch)
        {
            var events = Enumerable.Range(0, (int)Math.Min(batch, from + count - s))
                .Select(i => new OutboxEvent(aggregate, s + i, LabClock.Now())).ToArray();
            await producer.PublishAsync(events, TimeSpan.Zero, Ct);
        }
    }
}

/// <summary>The lab's scenarios end to end on every PostgreSQL strategy, at test scale.</summary>
public sealed class PostgresScenarioTests(PostgresFixture db)
{
    private static readonly ScenarioSettings Settings = new(PayloadSize: 128, BatchSize: 100, PollInterval: TimeSpan.FromMilliseconds(20), Aggregates: 32);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(PgStrategyKind.SkipLocked, 4)]
    [InlineData(PgStrategyKind.Notify, 4)]
    [InlineData(PgStrategyKind.Xmin, 1)]
    [InlineData(PgStrategyKind.Logical, 1)]
    public async Task Drain_delivers_every_message_exactly_once(PgStrategyKind kind, int workers)
    {
        await using var strategy = db.Strategy(kind);

        var result = await new ScenarioRunner(Settings).DrainAsync(strategy, workers, 3_000, Ct);

        result.Ordering!.Unique.Should().Be(3_000);
        result.Ordering.Duplicates.Should().Be(0);
        result.ThroughputPerSecond.Should().BePositive();
    }

    [Theory]
    [InlineData(PgStrategyKind.Xmin)]
    [InlineData(PgStrategyKind.Logical)]
    public async Task Ordered_strategies_keep_per_aggregate_order_under_commit_reordering(PgStrategyKind kind)
    {
        await using var strategy = db.Strategy(kind);

        var result = await new ScenarioRunner(Settings).OrderingAsync(strategy, 1, 600, producers: 6, TimeSpan.FromMilliseconds(3), Ct);

        result.Ordering!.Missing.Should().Be(0);
        result.Ordering.Inversions.Should().Be(0);
    }

    [Theory]
    [InlineData(PgStrategyKind.SkipLocked)]
    [InlineData(PgStrategyKind.Notify)]
    public async Task Competing_workers_lose_nothing_under_commit_reordering(PgStrategyKind kind)
    {
        await using var strategy = db.Strategy(kind);

        var result = await new ScenarioRunner(Settings).OrderingAsync(strategy, 4, 600, producers: 6, TimeSpan.FromMilliseconds(3), Ct);

        result.Ordering!.Missing.Should().Be(0);
        result.Ordering.Duplicates.Should().Be(0);
    }

    [Fact]
    public async Task Publish_and_latency_scenarios_run_on_postgres()
    {
        await using var strategy = db.Strategy(PgStrategyKind.Notify);
        var runner = new ScenarioRunner(Settings);

        var publish = await runner.PublishAsync(strategy, producers: 2, TimeSpan.FromSeconds(1), Ct);
        var latency = await runner.LatencyAsync(strategy, 2, 100, TimeSpan.FromSeconds(1), producers: 2, TimeSpan.FromMilliseconds(200), Ct);

        publish.ThroughputPerSecond.Should().BePositive();
        latency.Latency!.Count.Should().BePositive();
        latency.Ordering!.Missing.Should().Be(0);
    }
}
