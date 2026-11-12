namespace OutboxLab.Core.Model;

/// <summary>Receives messages from a relay. In the lab this is the measurement point; in production it is the broker.</summary>
public interface IDeliverySink
{
    /// <summary>Called by a relay worker before it acknowledges the message (at-least-once: deliver, then commit).</summary>
    void Deliver(OutboxEvent message, int worker);
}

/// <summary>
/// Writes a business change and its outbox message(s) in one local transaction. One instance owns one connection and
/// is used by one task at a time; concurrency comes from several producers.
/// </summary>
public interface IOutboxProducer : IAsyncDisposable
{
    /// <summary>Commits one business transaction that contains every event in <paramref name="events"/>.</summary>
    /// <param name="events">The events; each one also writes one business row.</param>
    /// <param name="holdOpen">Keeps the transaction open this long before committing, to provoke commit reordering.</param>
    /// <param name="cancellationToken">Cancels the transaction.</param>
    Task PublishAsync(IReadOnlyList<OutboxEvent> events, TimeSpan holdOpen, CancellationToken cancellationToken);
}

/// <summary>Moves committed outbox messages to an <see cref="IDeliverySink"/> until cancelled.</summary>
public interface IOutboxRelay
{
    Task RunAsync(IDeliverySink sink, CancellationToken cancellationToken);
}

/// <summary>How a relay is run.</summary>
/// <param name="Workers">Concurrent claimers. Strategies with a single ordered stream ignore values above 1.</param>
/// <param name="BatchSize">Maximum messages claimed per round trip.</param>
/// <param name="PollInterval">Idle wait between empty polls, or the wait timeout for push-based strategies.</param>
/// <param name="PayloadSize">Payload bytes per message.</param>
public sealed record RelayOptions(int Workers = 1, int BatchSize = 100, TimeSpan? PollInterval = null, int PayloadSize = 256)
{
    public TimeSpan EffectivePollInterval => PollInterval ?? TimeSpan.FromMilliseconds(100);
}

/// <summary>
/// One way of running the outbox on one engine: how producers write and how the relay reads. The lab compares
/// strategies through this interface only, so every measurement runs the exact same harness code.
/// </summary>
public interface IOutboxStrategy
{
    /// <summary>Stable identifier, for example <c>pg-skip-locked</c>.</summary>
    string Id { get; }

    /// <summary>Engine name for reports.</summary>
    string Engine { get; }

    /// <summary>What the strategy guarantees about order, in one line (for reports).</summary>
    string OrderingModel { get; }

    /// <summary>True when several workers can claim concurrently; false for single ordered streams.</summary>
    bool SupportsParallelWorkers { get; }

    /// <summary>Empties the outbox and any strategy state (slot, queue, cursor) so runs are independent.</summary>
    Task ResetAsync(CancellationToken cancellationToken);

    Task<IOutboxProducer> CreateProducerAsync(RelayOptions options, CancellationToken cancellationToken);

    IOutboxRelay CreateRelay(RelayOptions options);
}
