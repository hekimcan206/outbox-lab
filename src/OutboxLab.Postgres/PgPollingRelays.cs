using Npgsql;
using OutboxLab.Core.Model;

namespace OutboxLab.Postgres;

/// <summary>
/// Competing-consumer relay: each worker claims a batch with <c>FOR UPDATE SKIP LOCKED</c>, delivers it and deletes
/// it in the same transaction (at-least-once: a crash before COMMIT releases the rows for redelivery).
/// </summary>
/// <remarks>
/// Workers never block each other, which is what makes this scale, and also what breaks per-aggregate order: two
/// workers holding consecutive messages of one aggregate deliver them in whatever order their transactions run.
/// </remarks>
internal sealed class PgSkipLockedRelay(NpgsqlDataSource dataSource, RelayOptions options, bool useNotify) : IOutboxRelay
{
    private const string Claim =
        """
        WITH claimed AS (
            SELECT id FROM outbox_lab.outbox ORDER BY id LIMIT $1 FOR UPDATE SKIP LOCKED)
        DELETE FROM outbox_lab.outbox o USING claimed c WHERE o.id = c.id
        RETURNING o.id, o.payload
        """;

    public Task RunAsync(IDeliverySink sink, CancellationToken cancellationToken) =>
        Task.WhenAll(Enumerable.Range(0, Math.Max(1, options.Workers)).Select(w => WorkerAsync(sink, w, cancellationToken)));

    private async Task WorkerAsync(IDeliverySink sink, int worker, CancellationToken cancellationToken)
    {
        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        var wake = new WakeFlag();
        if (useNotify)
        {
            // A notification can arrive while this connection is busy claiming (Npgsql raises the event then, not
            // during the next WaitAsync). The flag remembers it, so the worker does not sleep through a wake-up.
            conn.Notification += (_, _) => wake.Set();
            await using var listen = new NpgsqlCommand($"LISTEN {PgStrategy.NotifyChannel}", conn);
            await listen.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        var claimed = new List<(long Id, byte[] Payload)>(options.BatchSize);
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            wake.Reset();
            claimed.Clear();

            await using (var tx = await conn.BeginTransactionAsync(cancellationToken).ConfigureAwait(false))
            {
                await using (var cmd = new NpgsqlCommand(Claim, conn, tx))
                {
                    cmd.Parameters.AddWithValue(options.BatchSize);
                    await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        claimed.Add((reader.GetInt64(0), reader.GetFieldValue<byte[]>(1)));
                    }
                }

                // RETURNING has no defined order; deliver the batch in outbox order.
                claimed.Sort(static (a, b) => a.Id.CompareTo(b.Id));
                foreach (var (_, payload) in claimed)
                {
                    sink.Deliver(EventCodec.Decode(payload), worker);
                }

                await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
            }

            if (claimed.Count == options.BatchSize)
            {
                continue; // a full batch means there is probably more
            }

            if (!useNotify)
            {
                await Task.Delay(options.EffectivePollInterval, cancellationToken).ConfigureAwait(false);
            }
            else if (!wake.IsSet)
            {
                // The timeout is the safety net for a lost notification (for example after a reconnect).
                await conn.WaitAsync(options.EffectivePollInterval, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private sealed class WakeFlag
    {
        private int _value;

        public bool IsSet => Volatile.Read(ref _value) == 1;

        public void Set() => Volatile.Write(ref _value, 1);

        public void Reset() => Volatile.Write(ref _value, 0);
    }
}

/// <summary>
/// Single ordered reader that only takes rows whose writing transaction is older than the snapshot's xmin. Every
/// transaction below xmin has finished, so the set of such rows can no longer change and (txid, id) is a stable
/// order: no row can later appear "behind" what was already delivered.
/// </summary>
/// <remarks>
/// The price: one long-running transaction anywhere in the cluster holds xmin back and stalls delivery until it ends.
/// Only one instance may run at a time (in production: behind an advisory lock or a leader election).
/// </remarks>
internal sealed class PgXminRelay(NpgsqlDataSource dataSource, RelayOptions options) : IOutboxRelay
{
    private const string Claim =
        """
        WITH ready AS (
            SELECT id FROM outbox_lab.outbox
            WHERE txid < pg_snapshot_xmin(pg_current_snapshot())
            ORDER BY txid, id LIMIT $1)
        DELETE FROM outbox_lab.outbox o USING ready r WHERE o.id = r.id
        RETURNING o.txid, o.id, o.payload
        """;

    public async Task RunAsync(IDeliverySink sink, CancellationToken cancellationToken)
    {
        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        var claimed = new List<(ulong TxId, long Id, byte[] Payload)>(options.BatchSize);
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            claimed.Clear();
            await using (var tx = await conn.BeginTransactionAsync(cancellationToken).ConfigureAwait(false))
            {
                await using (var cmd = new NpgsqlCommand(Claim, conn, tx))
                {
                    cmd.Parameters.AddWithValue(options.BatchSize);
                    await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        claimed.Add((reader.GetFieldValue<ulong>(0), reader.GetInt64(1), reader.GetFieldValue<byte[]>(2)));
                    }
                }

                claimed.Sort(static (a, b) => a.TxId != b.TxId ? a.TxId.CompareTo(b.TxId) : a.Id.CompareTo(b.Id));
                foreach (var row in claimed)
                {
                    sink.Deliver(EventCodec.Decode(row.Payload), 0);
                }

                await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
            }

            if (claimed.Count < options.BatchSize)
            {
                await Task.Delay(options.EffectivePollInterval, cancellationToken).ConfigureAwait(false);
            }
        }
    }
}

/// <summary>
/// The tempting design that is wrong: remember the highest id delivered and ask for <c>id &gt; last</c>. Identity
/// values are assigned at INSERT, not at COMMIT, so a transaction that took id 41 and commits after id 42 was read is
/// skipped forever. Kept in the lab to measure how often that happens.
/// </summary>
internal sealed class PgIdCursorRelay(NpgsqlDataSource dataSource, RelayOptions options) : IOutboxRelay
{
    private const string Next =
        "SELECT id, payload FROM outbox_lab.outbox WHERE id > $1 ORDER BY id LIMIT $2";

    public async Task RunAsync(IDeliverySink sink, CancellationToken cancellationToken)
    {
        await using var conn = await dataSource.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        long last = 0;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var count = 0;
            await using (var cmd = new NpgsqlCommand(Next, conn))
            {
                cmd.Parameters.AddWithValue(last);
                cmd.Parameters.AddWithValue(options.BatchSize);
                await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    last = reader.GetInt64(0);
                    sink.Deliver(EventCodec.Decode(reader.GetFieldValue<byte[]>(1)), 0);
                    count++;
                }
            }

            if (count < options.BatchSize)
            {
                await Task.Delay(options.EffectivePollInterval, cancellationToken).ConfigureAwait(false);
            }
        }
    }
}
