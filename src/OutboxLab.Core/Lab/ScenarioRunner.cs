using System.Diagnostics;
using System.Threading.Channels;
using OutboxLab.Core.Measurement;
using OutboxLab.Core.Model;

namespace OutboxLab.Core.Lab;

/// <summary>
/// Runs the four lab scenarios against any <see cref="IOutboxStrategy"/>. Every strategy goes through exactly this
/// code, so differences in the results come from the strategy, not from the harness.
/// </summary>
public sealed class ScenarioRunner(ScenarioSettings settings, Action<string>? log = null)
{
    private static readonly TimeSpan DeliveryTimeout = TimeSpan.FromMinutes(5);

    private readonly ScenarioSettings _settings = settings;
    private readonly Action<string> _log = log ?? (_ => { });

    /// <summary>
    /// Fills the outbox with <paramref name="messages"/> committed messages, then starts the relay and measures how fast
    /// it drains them. Throughput is messages divided by the time from the first to the last delivery.
    /// </summary>
    public async Task<ScenarioResult> DrainAsync(IOutboxStrategy strategy, int workers, int messages, CancellationToken cancellationToken)
    {
        using var activity = Start(Scenarios.Drain, strategy, workers);
        workers = EffectiveWorkers(strategy, workers);
        var options = _settings.RelayOptions(workers);
        await strategy.ResetAsync(cancellationToken).ConfigureAwait(false);

        const int fillers = 4;
        var sequencers = Enumerable.Range(0, fillers)
            .Select(p => new AggregateSequencer(p, fillers, _settings.Aggregates, _settings.Seed)).ToArray();
        await Task.WhenAll(sequencers.Select(async (sequencer, p) =>
        {
            await using var producer = await strategy.CreateProducerAsync(options, cancellationToken).ConfigureAwait(false);
            var share = messages / fillers + (p < messages % fillers ? 1 : 0);
            for (var done = 0; done < share; done += _settings.BatchSize)
            {
                var batch = Enumerable.Range(0, Math.Min(_settings.BatchSize, share - done))
                    .Select(_ => sequencer.Next(LabClock.Now())).ToArray();
                await producer.PublishAsync(batch, TimeSpan.Zero, cancellationToken).ConfigureAwait(false);
            }
        })).ConfigureAwait(false);
        var expected = AggregateSequencer.Merge(sequencers);
        _log($"{strategy.Id}: backlog of {messages} committed, starting {workers} worker(s)");

        var recorder = new DeliveryRecorder(strategy.Id);
        await RunRelayUntilAsync(strategy, options, recorder,
            ct => recorder.WaitForUniqueAsync(messages, DeliveryTimeout, ct), cancellationToken).ConfigureAwait(false);

        var seconds = LabClock.MillisecondsBetween(recorder.FirstDeliveredAt, recorder.LastDeliveredAt) / 1000.0;
        var ordering = OrderingAnalyzer.Analyze(recorder.Snapshot(), expected);
        return new ScenarioResult(Scenarios.Drain, strategy.Id, strategy.Engine, workers, fillers, messages, seconds,
            seconds > 0 ? messages / seconds : double.NaN, Ordering: ordering);
    }

    /// <summary>
    /// Publishes single-message transactions at a fixed rate (open loop, so a slow relay cannot slow the producers down)
    /// and measures latency from the start of each producing transaction to its delivery.
    /// </summary>
    public async Task<ScenarioResult> LatencyAsync(
        IOutboxStrategy strategy, int workers, double ratePerSecond, TimeSpan duration, int producers, TimeSpan warmup, CancellationToken cancellationToken)
    {
        using var activity = Start(Scenarios.Latency, strategy, workers);
        workers = EffectiveWorkers(strategy, workers);
        var options = _settings.RelayOptions(workers);
        await strategy.ResetAsync(cancellationToken).ConfigureAwait(false);

        var recorder = new DeliveryRecorder(strategy.Id);
        var total = (int)Math.Round(ratePerSecond * (duration + warmup).TotalSeconds);
        var sequencers = Enumerable.Range(0, producers)
            .Select(p => new AggregateSequencer(p, producers, _settings.Aggregates, _settings.Seed)).ToArray();
        long measureFrom = 0;
        double achievedRate = 0;

        await RunRelayUntilAsync(strategy, options, recorder, async ct =>
        {
            var instances = await Task.WhenAll(Enumerable.Range(0, producers)
                .Select(_ => strategy.CreateProducerAsync(options, ct))).ConfigureAwait(false);
            try
            {
                // Give push-based relays (replication stream, AQ wait) time to be listening before the clock starts.
                await Task.Delay(TimeSpan.FromMilliseconds(500), ct).ConfigureAwait(false);
                var ticks = Channel.CreateUnbounded<int>(new UnboundedChannelOptions { SingleWriter = true });
                var publishers = instances.Select((producer, p) => Task.Run(async () =>
                {
                    await foreach (var _ in ticks.Reader.ReadAllAsync(ct).ConfigureAwait(false))
                    {
                        var message = sequencers[p].Next(LabClock.Now());
                        await producer.PublishAsync([message], TimeSpan.Zero, ct).ConfigureAwait(false);
                    }
                }, ct)).ToArray();

                // Each producer takes the next tick; per-producer sequencers keep aggregates single-writer.
                var started = Stopwatch.StartNew();
                measureFrom = LabClock.Now() + (long)(warmup.TotalSeconds * Stopwatch.Frequency);
                var sent = 0;
                while (sent < total)
                {
                    var due = Math.Min(total, (int)(started.Elapsed.TotalSeconds * ratePerSecond));
                    for (; sent < due; sent++)
                    {
                        ticks.Writer.TryWrite(sent);
                    }

                    await Task.Delay(1, ct).ConfigureAwait(false);
                }

                ticks.Writer.Complete();
                await Task.WhenAll(publishers).ConfigureAwait(false);
                achievedRate = total / started.Elapsed.TotalSeconds;
                return await recorder.WaitForUniqueAsync(total, DeliveryTimeout, ct).ConfigureAwait(false);
            }
            finally
            {
                foreach (var producer in instances)
                {
                    await producer.DisposeAsync().ConfigureAwait(false);
                }
            }
        }, cancellationToken).ConfigureAwait(false);

        var deliveries = recorder.Snapshot();
        var firsts = deliveries.GroupBy(d => (d.AggregateId, d.Sequence)).Select(g => g.First());
        var latency = LatencySummary.From(firsts
            .Where(d => d.ProducedAt >= measureFrom)
            .Select(d => LabClock.MillisecondsBetween(d.ProducedAt, d.DeliveredAt)));
        var ordering = OrderingAnalyzer.Analyze(deliveries, AggregateSequencer.Merge(sequencers));
        return new ScenarioResult(Scenarios.Latency, strategy.Id, strategy.Engine, workers, producers, total,
            (duration + warmup).TotalSeconds, achievedRate, ratePerSecond, latency, ordering,
            $"poll/wait {options.EffectivePollInterval.TotalMilliseconds:0} ms");
    }

    /// <summary>
    /// Several producers commit single-message transactions concurrently, each holding its transaction open for a random
    /// time up to <paramref name="maxHold"/> so commits finish out of id order, while the relay runs. Reports loss,
    /// duplicates and per-aggregate inversions.
    /// </summary>
    public async Task<ScenarioResult> OrderingAsync(
        IOutboxStrategy strategy, int workers, int messages, int producers, TimeSpan maxHold, CancellationToken cancellationToken)
    {
        using var activity = Start(Scenarios.Ordering, strategy, workers);
        workers = EffectiveWorkers(strategy, workers);
        var options = _settings.RelayOptions(workers);
        await strategy.ResetAsync(cancellationToken).ConfigureAwait(false);

        var recorder = new DeliveryRecorder(strategy.Id);
        var sequencers = Enumerable.Range(0, producers)
            .Select(p => new AggregateSequencer(p, producers, _settings.Aggregates, _settings.Seed)).ToArray();
        var started = Stopwatch.StartNew();

        await RunRelayUntilAsync(strategy, options, recorder, async ct =>
        {
            await Task.WhenAll(sequencers.Select(async (sequencer, p) =>
            {
                var random = new Random(HashCode.Combine(_settings.Seed, p, "hold"));
                await using var producer = await strategy.CreateProducerAsync(options, ct).ConfigureAwait(false);
                var share = messages / producers + (p < messages % producers ? 1 : 0);
                for (var i = 0; i < share; i++)
                {
                    var hold = TimeSpan.FromMilliseconds(random.NextDouble() * maxHold.TotalMilliseconds);
                    await producer.PublishAsync([sequencer.Next(LabClock.Now())], hold, ct).ConfigureAwait(false);
                }
            })).ConfigureAwait(false);

            // A lossy strategy never reaches the full count, so stop once deliveries have been quiet for a while.
            if (!await recorder.WaitForUniqueAsync(messages, TimeSpan.FromSeconds(10), ct).ConfigureAwait(false))
            {
                await recorder.WaitForQuietAsync(TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(30), ct).ConfigureAwait(false);
            }

            return true;
        }, cancellationToken).ConfigureAwait(false);

        var ordering = OrderingAnalyzer.Analyze(recorder.Snapshot(), AggregateSequencer.Merge(sequencers));
        var seconds = started.Elapsed.TotalSeconds;
        return new ScenarioResult(Scenarios.Ordering, strategy.Id, strategy.Engine, workers, producers, messages, seconds,
            ordering.Unique / seconds, Ordering: ordering, Notes: $"hold 0-{maxHold.TotalMilliseconds:0} ms");
    }

    /// <summary>
    /// Closed-loop producers commit single-message business transactions as fast as they can for
    /// <paramref name="duration"/>, with no relay running: the cost the strategy adds to the write path.
    /// </summary>
    public async Task<ScenarioResult> PublishAsync(IOutboxStrategy strategy, int producers, TimeSpan duration, CancellationToken cancellationToken)
    {
        using var activity = Start(Scenarios.Publish, strategy, 0);
        var options = _settings.RelayOptions(1);
        await strategy.ResetAsync(cancellationToken).ConfigureAwait(false);

        var instances = await Task.WhenAll(Enumerable.Range(0, producers)
            .Select(_ => strategy.CreateProducerAsync(options, cancellationToken))).ConfigureAwait(false);
        var commits = new long[producers];
        var latencies = new List<double>[producers];
        try
        {
            // Warm every connection up so connection opening is not part of the measurement.
            var warmups = instances.Select((producer, p) => producer.PublishAsync(
                [new OutboxEvent(-1 - p, 1, LabClock.Now())], TimeSpan.Zero, cancellationToken));
            await Task.WhenAll(warmups).ConfigureAwait(false);

            using var window = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var stopwatch = Stopwatch.StartNew();
            window.CancelAfter(duration);
            await Task.WhenAll(instances.Select((producer, p) => Task.Run(async () =>
            {
                var sequencer = new AggregateSequencer(p, producers, Math.Max(_settings.Aggregates, producers), _settings.Seed);
                latencies[p] = [];
                while (!window.IsCancellationRequested)
                {
                    var start = LabClock.Now();
                    await producer.PublishAsync([sequencer.Next(start)], TimeSpan.Zero, CancellationToken.None).ConfigureAwait(false);
                    latencies[p].Add(LabClock.MillisecondsBetween(start, LabClock.Now()));
                    commits[p]++;
                }
            }, CancellationToken.None))).ConfigureAwait(false);
            var seconds = stopwatch.Elapsed.TotalSeconds;
            var total = commits.Sum();
            LabTelemetry.Published.Add(total, new KeyValuePair<string, object?>("strategy", strategy.Id));
            return new ScenarioResult(Scenarios.Publish, strategy.Id, strategy.Engine, 0, producers, (int)total, seconds,
                total / seconds, Latency: LatencySummary.From(latencies.SelectMany(l => l)),
                Notes: "latency = one business transaction (business row + outbox write + commit)");
        }
        finally
        {
            foreach (var producer in instances)
            {
                await producer.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    private static int EffectiveWorkers(IOutboxStrategy strategy, int workers) =>
        strategy.SupportsParallelWorkers ? Math.Max(1, workers) : 1;

    private static Activity? Start(string scenario, IOutboxStrategy strategy, int workers) =>
        LabTelemetry.Source.StartActivity(scenario)?
            .SetTag("outbox.strategy", strategy.Id)
            .SetTag("outbox.engine", strategy.Engine)
            .SetTag("outbox.workers", workers);

    /// <summary>
    /// Runs the relay while <paramref name="body"/> executes, then stops it. A relay that fails surfaces its exception
    /// instead of leaving the scenario waiting for deliveries that will never come.
    /// </summary>
    private async Task RunRelayUntilAsync(
        IOutboxStrategy strategy, RelayOptions options, DeliveryRecorder recorder, Func<CancellationToken, Task<bool>> body, CancellationToken cancellationToken)
    {
        using var relayCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var relay = strategy.CreateRelay(options);
        var relayTask = Task.Run(() => relay.RunAsync(recorder, relayCts.Token), CancellationToken.None);
        var bodyTask = body(relayCts.Token);

        var first = await Task.WhenAny(bodyTask, relayTask).ConfigureAwait(false);
        if (first == relayTask)
        {
            await relayTask.ConfigureAwait(false); // rethrows the relay failure
            throw new InvalidOperationException($"{strategy.Id}: relay stopped before the scenario finished.");
        }

        var completed = await bodyTask.ConfigureAwait(false);
        await relayCts.CancelAsync().ConfigureAwait(false);
        try
        {
            await relayTask.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Expected: the relay runs until cancelled.
        }

        if (!completed)
        {
            throw new TimeoutException($"{strategy.Id}: only {recorder.UniqueCount} distinct messages were delivered in time.");
        }

        _log($"{strategy.Id}: {recorder.UniqueCount} distinct / {recorder.Count} total deliveries");
    }
}
