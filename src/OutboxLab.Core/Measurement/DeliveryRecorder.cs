using System.Diagnostics;
using OutboxLab.Core.Lab;
using OutboxLab.Core.Model;

namespace OutboxLab.Core.Measurement;

/// <summary>One delivery as observed by the sink. <see cref="Index"/> is the global order in which the sink saw it.</summary>
public readonly record struct Delivery(long Index, int AggregateId, long Sequence, long ProducedAt, long DeliveredAt, int Worker);

/// <summary>
/// Thread-safe sink that records every delivery in the order it arrived. The lock gives one global delivery order,
/// which is what the ordering analysis needs; at the rates measured here it costs well under a microsecond per call.
/// </summary>
public sealed class DeliveryRecorder(string? strategy = null) : IDeliverySink
{
    private readonly KeyValuePair<string, object?> _tag = new("strategy", strategy ?? "unspecified");
    private readonly Lock _gate = new();
    private readonly List<Delivery> _deliveries = [];
    private readonly HashSet<(int, long)> _seen = [];
    private TaskCompletionSource? _waiter;
    private int _waitFor = int.MaxValue;
    private long _firstDeliveredAt;
    private long _lastDeliveredAt;

    /// <summary>Total deliveries, duplicates included.</summary>
    public int Count
    {
        get
        {
            lock (_gate)
            {
                return _deliveries.Count;
            }
        }
    }

    /// <summary>Distinct (aggregate, sequence) pairs delivered.</summary>
    public int UniqueCount
    {
        get
        {
            lock (_gate)
            {
                return _seen.Count;
            }
        }
    }

    /// <summary>Timestamp of the first delivery, or 0.</summary>
    public long FirstDeliveredAt
    {
        get
        {
            lock (_gate)
            {
                return _firstDeliveredAt;
            }
        }
    }

    /// <summary>Timestamp of the most recent delivery, or 0.</summary>
    public long LastDeliveredAt
    {
        get
        {
            lock (_gate)
            {
                return _lastDeliveredAt;
            }
        }
    }

    public void Deliver(OutboxEvent message, int worker)
    {
        var now = LabClock.Now();
        TaskCompletionSource? release = null;
        lock (_gate)
        {
            _deliveries.Add(new Delivery(_deliveries.Count, message.AggregateId, message.Sequence, message.ProducedAt, now, worker));
            _seen.Add((message.AggregateId, message.Sequence));
            if (_firstDeliveredAt == 0)
            {
                _firstDeliveredAt = now;
            }

            _lastDeliveredAt = now;
            if (_waiter is not null && _seen.Count >= _waitFor)
            {
                release = _waiter;
                _waiter = null;
            }
        }

        LabTelemetry.Delivered.Add(1, _tag);
        LabTelemetry.DeliveryLatency.Record(LabClock.MillisecondsBetween(message.ProducedAt, now), _tag);
        release?.TrySetResult();
    }

    /// <summary>Completes when at least <paramref name="uniqueCount"/> distinct messages arrived; false on timeout.</summary>
    public async Task<bool> WaitForUniqueAsync(int uniqueCount, TimeSpan timeout, CancellationToken cancellationToken)
    {
        Task waitTask;
        lock (_gate)
        {
            if (_seen.Count >= uniqueCount)
            {
                return true;
            }

            _waitFor = uniqueCount;
            _waiter = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            waitTask = _waiter.Task;
        }

        try
        {
            await waitTask.WaitAsync(timeout, cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (TimeoutException)
        {
            return false;
        }
    }

    /// <summary>Waits until no delivery arrived for <paramref name="quiet"/>. Used where loss is possible.</summary>
    public async Task WaitForQuietAsync(TimeSpan quiet, TimeSpan timeout, CancellationToken cancellationToken)
    {
        var started = Stopwatch.StartNew();
        var lastCount = -1;
        var quietSince = Stopwatch.StartNew();
        while (started.Elapsed < timeout)
        {
            var count = Count;
            if (count != lastCount)
            {
                lastCount = count;
                quietSince.Restart();
            }
            else if (quietSince.Elapsed >= quiet)
            {
                return;
            }

            await Task.Delay(50, cancellationToken).ConfigureAwait(false);
        }
    }

    public IReadOnlyList<Delivery> Snapshot()
    {
        lock (_gate)
        {
            return [.. _deliveries];
        }
    }
}
