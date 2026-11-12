namespace OutboxLab.Core.Model;

/// <summary>
/// One outbox message as every strategy sees it: the aggregate it belongs to, its position in that aggregate's history
/// and the <see cref="LabClock"/> timestamp taken when the producing transaction started.
/// </summary>
/// <remarks>
/// Per-aggregate sequence numbers are what make ordering measurable: a relay preserves order for an aggregate exactly
/// when it never hands over sequence n after sequence m &gt; n of the same aggregate.
/// </remarks>
public readonly record struct OutboxEvent(int AggregateId, long Sequence, long ProducedAt);
