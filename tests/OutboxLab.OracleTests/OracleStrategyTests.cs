using Oracle.ManagedDataAccess.Client;
using OutboxLab.Core.Lab;
using OutboxLab.Core.Measurement;
using OutboxLab.Core.Model;
using OutboxLab.Oracle;

namespace OutboxLab.OracleTests;

/// <summary>Delivery semantics of the Oracle strategies against a real Oracle Database 23ai Free.</summary>
public sealed class OracleStrategyTests(OracleFixture db)
{
    private static readonly RelayOptions Fast = new(Workers: 1, BatchSize: 50, PollInterval: TimeSpan.FromMilliseconds(20), PayloadSize: 128);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Rownum_with_skip_locked_returns_nothing_while_the_cursor_pattern_gets_the_next_rows()
    {
        Assert.SkipUnless(OracleFixture.Enabled, OracleFixture.SkipReason);
        var strategy = db.Strategy(OraStrategyKind.SkipLocked);
        await strategy.ResetAsync(Ct);
        await using (var producer = await strategy.CreateProducerAsync(Fast, Ct))
        {
            await producer.PublishAsync([.. Enumerable.Range(1, 20).Select(s => new OutboxEvent(1, s, 0))], TimeSpan.Zero, Ct);
        }

        await using var holder = new OracleConnection(db.Connections.Relay);
        await holder.OpenAsync(Ct);
        await using var held = (OracleTransaction)await holder.BeginTransactionAsync(Ct);
        var first = await LockAsync(holder, held, "SELECT id FROM outbox_owner.outbox WHERE ROWNUM <= 10 FOR UPDATE SKIP LOCKED", int.MaxValue);
        first.Should().HaveCount(10);

        await using var other = new OracleConnection(db.Connections.Relay);
        await other.OpenAsync(Ct);
        await using (var tx = (OracleTransaction)await other.BeginTransactionAsync(Ct))
        {
            var rownum = await LockAsync(other, tx, "SELECT id FROM outbox_owner.outbox WHERE ROWNUM <= 10 FOR UPDATE SKIP LOCKED", int.MaxValue);
            rownum.Should().BeEmpty("ROWNUM picks the same first 10 rows before SKIP LOCKED drops them all");
            await tx.RollbackAsync(Ct);
        }

        await using (var tx = (OracleTransaction)await other.BeginTransactionAsync(Ct))
        {
            var cursor = await LockAsync(other, tx, "SELECT id FROM outbox_owner.outbox ORDER BY id FOR UPDATE SKIP LOCKED", 10);
            cursor.Should().HaveCount(10).And.NotIntersectWith(first);
            await tx.RollbackAsync(Ct);
        }

        await held.RollbackAsync(Ct);
    }

    [Fact]
    public async Task Skip_locked_relay_that_crashes_before_commit_leaves_every_message_for_redelivery()
    {
        Assert.SkipUnless(OracleFixture.Enabled, OracleFixture.SkipReason);
        await CrashBeforeCommitIsRedeliveredAsync(db.Strategy(OraStrategyKind.SkipLocked));
    }

    [Fact]
    public async Task Aq_relay_that_crashes_before_commit_leaves_every_message_for_redelivery()
    {
        Assert.SkipUnless(OracleFixture.Enabled, OracleFixture.SkipReason);
        await CrashBeforeCommitIsRedeliveredAsync(db.Strategy(OraStrategyKind.AdvancedQueuing));
    }

    [Fact]
    public async Task Aq_enqueue_joins_the_business_transaction_so_a_rollback_is_never_delivered()
    {
        Assert.SkipUnless(OracleFixture.Enabled, OracleFixture.SkipReason);
        var strategy = db.Strategy(OraStrategyKind.AdvancedQueuing);
        await strategy.ResetAsync(Ct);
        var recorder = new DeliveryRecorder();
        await using var relay = RelayHost.Start(strategy, Fast, recorder);

        await using (var doomed = await strategy.CreateProducerAsync(Fast, Ct))
        {
            using var abort = CancellationTokenSource.CreateLinkedTokenSource(Ct);
            var rolledBack = doomed.PublishAsync([new OutboxEvent(99, 1, LabClock.Now())], TimeSpan.FromSeconds(30), abort.Token);
            await Task.Delay(500, Ct);
            recorder.Count.Should().Be(0, "an uncommitted enqueue is invisible to the dequeuer");
            await abort.CancelAsync();
            await FluentActions.Awaiting(() => rolledBack).Should().ThrowAsync<OperationCanceledException>();
        }

        await using (var producer = await strategy.CreateProducerAsync(Fast, Ct))
        {
            await producer.PublishAsync([new OutboxEvent(1, 1, LabClock.Now())], TimeSpan.Zero, Ct);
        }

        (await recorder.WaitForUniqueAsync(1, TimeSpan.FromSeconds(10), Ct)).Should().BeTrue();
        await Task.Delay(500, Ct);
        recorder.Snapshot().Select(d => d.AggregateId).Should().Equal([1]);
        LabClock.MillisecondsBetween(recorder.Snapshot()[0].ProducedAt, recorder.Snapshot()[0].DeliveredAt)
            .Should().BeLessThan(1_000, "a commit wakes the blocked dequeuer; it does not wait for the 1 s dequeue timeout");
    }

    [Fact]
    public async Task Users_are_least_privilege()
    {
        Assert.SkipUnless(OracleFixture.Enabled, OracleFixture.SkipReason);

        await using (var app = new OracleConnection(db.Connections.App))
        {
            await app.OpenAsync(Ct);
            var read = () => new OracleCommand("SELECT COUNT(*) FROM outbox_owner.outbox", app).ExecuteScalarAsync(Ct);
            (await read.Should().ThrowAsync<OracleException>()).Which.Number.Should().Be(41900, "ORA-41900 (23ai): the app can see the table through its INSERT grant but may not read it");

            using var queue = new OracleAQQueue("OUTBOX_OWNER.OUTBOX_Q", app, OracleAQMessageType.Raw);
            queue.DequeueOptions.Wait = 0;
            var dequeue = () => queue.Dequeue();
            dequeue.Should().Throw<OracleException>("the application may enqueue but not dequeue");
        }

        await using (var relay = new OracleConnection(db.Connections.Relay))
        {
            await relay.OpenAsync(Ct);
            var read = () => new OracleCommand("SELECT COUNT(*) FROM outbox_owner.orders", relay).ExecuteScalarAsync(Ct);
            (await read.Should().ThrowAsync<OracleException>()).Which.Number.Should().Be(942, "ORA-00942: with no grant at all the table does not exist for the relay");
        }
    }

    private static async Task CrashBeforeCommitIsRedeliveredAsync(OraStrategy strategy)
    {
        await strategy.ResetAsync(Ct);
        await using (var producer = await strategy.CreateProducerAsync(Fast, Ct))
        {
            await producer.PublishAsync([.. Enumerable.Range(1, 40).Select(s => new OutboxEvent(5, s, 0))], TimeSpan.Zero, Ct);
        }

        var crashing = new FailingSink(failOnDelivery: 10);
        var crashed = RelayHost.Start(strategy, Fast, crashing);
        await FluentActions.Awaiting(() => crashed.Completion).Should().ThrowAsync<InvalidOperationException>();
        await crashed.DisposeAsync();
        crashing.Recorder.Count.Should().Be(9, "nine messages reached the broker before the crash");

        var recovered = new DeliveryRecorder();
        await using (RelayHost.Start(strategy, Fast, recovered))
        {
            (await recovered.WaitForUniqueAsync(40, TimeSpan.FromSeconds(20), Ct)).Should().BeTrue();
        }

        recovered.Count.Should().Be(40, "the crashed transaction rolled back, so all 40 messages were still there");
        OrderingAnalyzer.Analyze(recovered.Snapshot(), new Dictionary<int, long> { [5] = 40 }).Inversions.Should().Be(0);
    }

    private static async Task<List<long>> LockAsync(OracleConnection conn, OracleTransaction tx, string sql, int take)
    {
        await using var cmd = new OracleCommand(sql, conn) { Transaction = tx };
        await using var reader = (OracleDataReader)await cmd.ExecuteReaderAsync(Ct);
        reader.FetchSize = reader.RowSize * Math.Min(take, 1000);
        var ids = new List<long>();
        while (ids.Count < take && await reader.ReadAsync(Ct))
        {
            ids.Add(reader.GetInt64(0));
        }

        return ids;
    }
}

/// <summary>The lab's scenarios end to end on both Oracle strategies, at test scale.</summary>
public sealed class OracleScenarioTests(OracleFixture db)
{
    private static readonly ScenarioSettings Settings = new(PayloadSize: 128, BatchSize: 100, PollInterval: TimeSpan.FromMilliseconds(20), Aggregates: 32);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(OraStrategyKind.SkipLocked)]
    [InlineData(OraStrategyKind.AdvancedQueuing)]
    public async Task Drain_with_competing_workers_delivers_every_message_exactly_once(OraStrategyKind kind)
    {
        Assert.SkipUnless(OracleFixture.Enabled, OracleFixture.SkipReason);

        var result = await new ScenarioRunner(Settings).DrainAsync(db.Strategy(kind), workers: 4, messages: 3_000, Ct);

        result.Ordering!.Unique.Should().Be(3_000);
        result.Ordering.Duplicates.Should().Be(0);
    }

    [Theory]
    [InlineData(OraStrategyKind.SkipLocked)]
    [InlineData(OraStrategyKind.AdvancedQueuing)]
    public async Task A_single_worker_keeps_per_aggregate_order_under_commit_reordering(OraStrategyKind kind)
    {
        Assert.SkipUnless(OracleFixture.Enabled, OracleFixture.SkipReason);

        var result = await new ScenarioRunner(Settings).OrderingAsync(db.Strategy(kind), 1, 600, producers: 6, TimeSpan.FromMilliseconds(3), Ct);

        result.Ordering!.Missing.Should().Be(0);
        result.Ordering.Inversions.Should().Be(0);
    }

    [Fact]
    public async Task Publish_and_latency_scenarios_run_on_oracle()
    {
        Assert.SkipUnless(OracleFixture.Enabled, OracleFixture.SkipReason);
        var strategy = db.Strategy(OraStrategyKind.AdvancedQueuing);
        var runner = new ScenarioRunner(Settings);

        var publish = await runner.PublishAsync(strategy, producers: 2, TimeSpan.FromSeconds(1), Ct);
        var latency = await runner.LatencyAsync(strategy, 1, 50, TimeSpan.FromSeconds(1), producers: 2, TimeSpan.FromMilliseconds(200), Ct);

        publish.ThroughputPerSecond.Should().BePositive();
        latency.Latency!.Count.Should().BePositive();
        latency.Ordering!.Missing.Should().Be(0);
    }
}
