using System.Collections.Concurrent;
using OutboxLab.Core.Model;

namespace OutboxLab.UnitTests.Lab;

/// <summary>
/// An in-process outbox used to test the scenario engine without a database: a committed message becomes visible in
/// a queue. <paramref name="dropEvery"/> simulates a relay that loses messages so loss reporting can be tested.
/// </summary>
internal sealed class InMemoryStrategy(bool parallel = false, int dropEvery = 0) : IOutboxStrategy
{
    private ConcurrentQueue<OutboxEvent> _queue = new();
    private readonly int _dropEvery = dropEvery;
    private long _dequeued;

    public string Id => parallel ? "mem-parallel" : "mem-ordered";

    public string Engine => "memory";

    public string OrderingModel => "queue order";

    public bool SupportsParallelWorkers => parallel;

    public int Resets { get; private set; }

    public Task ResetAsync(CancellationToken cancellationToken)
    {
        _queue = new ConcurrentQueue<OutboxEvent>();
        Resets++;
        return Task.CompletedTask;
    }

    public Task<IOutboxProducer> CreateProducerAsync(RelayOptions options, CancellationToken cancellationToken) =>
        Task.FromResult<IOutboxProducer>(new Producer(this));

    public IOutboxRelay CreateRelay(RelayOptions options) => new Relay(this, options);

    private sealed class Producer(InMemoryStrategy owner) : IOutboxProducer
    {
        public async Task PublishAsync(IReadOnlyList<OutboxEvent> events, TimeSpan holdOpen, CancellationToken cancellationToken)
        {
            if (holdOpen > TimeSpan.Zero)
            {
                await Task.Delay(holdOpen, cancellationToken);
            }

            foreach (var e in events)
            {
                owner._queue.Enqueue(e);
            }
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class Relay(InMemoryStrategy owner, RelayOptions options) : IOutboxRelay
    {
        public Task RunAsync(IDeliverySink sink, CancellationToken cancellationToken) =>
            Task.WhenAll(Enumerable.Range(0, owner.SupportsParallelWorkers ? options.Workers : 1)
                .Select(w => Task.Run(() => WorkerAsync(sink, w, cancellationToken), cancellationToken)));

        private async Task WorkerAsync(IDeliverySink sink, int worker, CancellationToken cancellationToken)
        {
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var claimed = 0;
                while (claimed < options.BatchSize && owner._queue.TryDequeue(out var e))
                {
                    claimed++;
                    var n = Interlocked.Increment(ref owner._dequeued);
                    if (owner._dropEvery == 0 || n % owner._dropEvery != 0)
                    {
                        sink.Deliver(e, worker);
                    }
                }

                if (claimed == 0)
                {
                    await Task.Delay(1, cancellationToken);
                }
            }
        }
    }
}
