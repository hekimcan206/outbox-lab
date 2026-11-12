using System.Data;
using Oracle.ManagedDataAccess.Client;
using OutboxLab.Core.Model;

namespace OutboxLab.Oracle;

/// <summary>
/// Competing-consumer relay on the outbox table. Oracle rejects <c>FETCH FIRST n ROWS</c> together with
/// <c>FOR UPDATE</c>, and <c>ROWNUM &lt;= n</c> is applied before SKIP LOCKED (a worker then gets nothing while
/// others hold the first n rows). The working pattern is an unbounded <c>FOR UPDATE SKIP LOCKED</c> cursor from
/// which the worker fetches only n rows: with SKIP LOCKED, Oracle locks rows as they are fetched.
/// </summary>
/// <remarks>
/// The fetch size is set to exactly one batch, otherwise ODP.NET's default prefetch (128 KB) would lock several
/// batches' worth of rows that this worker never delivers, starving the other workers until it commits.
/// </remarks>
internal sealed class OraSkipLockedRelay(string connectionString, RelayOptions options) : IOutboxRelay
{
    private const string Claim =
        "SELECT id, payload FROM outbox_owner.outbox ORDER BY id FOR UPDATE SKIP LOCKED";

    public Task RunAsync(IDeliverySink sink, CancellationToken cancellationToken) =>
        Task.WhenAll(Enumerable.Range(0, Math.Max(1, options.Workers)).Select(w => WorkerAsync(sink, w, cancellationToken)));

    private async Task WorkerAsync(IDeliverySink sink, int worker, CancellationToken cancellationToken)
    {
        await using var conn = new OracleConnection(connectionString);
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
        var ids = new List<long>(options.BatchSize);

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            ids.Clear();
            await using (var tx = (OracleTransaction)await conn.BeginTransactionAsync(cancellationToken).ConfigureAwait(false))
            {
                await using (var cmd = new OracleCommand(Claim, conn) { Transaction = tx })
                await using (var reader = (OracleDataReader)await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
                {
                    reader.FetchSize = reader.RowSize * options.BatchSize;
                    while (ids.Count < options.BatchSize && await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        ids.Add(reader.GetInt64(0));
                        sink.Deliver(EventCodec.Decode(reader.GetOracleBinary(1).Value), worker);
                    }
                }

                if (ids.Count > 0)
                {
                    await using var delete = new OracleCommand("DELETE FROM outbox_owner.outbox WHERE id = :id", conn) { Transaction = tx };
                    delete.ArrayBindCount = ids.Count;
                    delete.Parameters.Add("id", OracleDbType.Int64, ids.ToArray(), ParameterDirection.Input);
                    await delete.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }

                await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
            }

            if (ids.Count < options.BatchSize)
            {
                await Task.Delay(options.EffectivePollInterval, cancellationToken).ConfigureAwait(false);
            }
        }
    }
}

/// <summary>
/// Advanced Queuing relay: each worker blocks in <c>DequeueArray</c> (REMOVE mode, ON_COMMIT visibility) until a
/// committed message is available, delivers the batch and commits, which removes the messages. A crash before the
/// commit rolls the dequeue back and the messages become available again (at-least-once).
/// </summary>
/// <remarks>
/// The ODP.NET dequeue call is synchronous, so each worker runs on its own thread. The wait is whole seconds, which
/// only bounds how quickly a worker notices cancellation: a commit wakes a waiting dequeuer at once.
/// </remarks>
internal sealed class OraAqRelay(string connectionString, RelayOptions options) : IOutboxRelay
{
    private const int NoMessage = 25228; // ORA-25228: timeout or end-of-fetch during message dequeue

    public Task RunAsync(IDeliverySink sink, CancellationToken cancellationToken) =>
        Task.WhenAll(Enumerable.Range(0, Math.Max(1, options.Workers))
            .Select(w => Task.Factory.StartNew(() => Worker(sink, w, cancellationToken), cancellationToken,
                TaskCreationOptions.LongRunning, TaskScheduler.Default)));

    private void Worker(IDeliverySink sink, int worker, CancellationToken cancellationToken)
    {
        using var conn = new OracleConnection(connectionString);
        conn.Open();
        using var queue = new OracleAQQueue(OraStrategy.QueueName, conn, OracleAQMessageType.Raw);
        queue.DequeueOptions.DequeueMode = OracleAQDequeueMode.Remove;
        // FIRST_MESSAGE restarts at the head of the queue on every call. NEXT_MESSAGE continues from a cursor position,
        // so a message whose transaction committed late, with an earlier enqueue time, could sit behind that position.
        queue.DequeueOptions.NavigationMode = OracleAQNavigationMode.FirstMessage;
        queue.DequeueOptions.Wait = Math.Max(1, (int)Math.Ceiling(options.EffectivePollInterval.TotalSeconds));

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            using var tx = conn.BeginTransaction();
            OracleAQMessage[] messages;
            try
            {
                messages = queue.DequeueArray(options.BatchSize);
            }
            catch (OracleException e) when (e.Number == NoMessage)
            {
                tx.Rollback();
                continue;
            }

            foreach (var message in messages)
            {
                sink.Deliver(EventCodec.Decode((byte[])message.Payload), worker);
            }

            tx.Commit();
        }
    }
}
