using Npgsql;
using NpgsqlTypes;
using OutboxLab.Core.Model;

namespace OutboxLab.Postgres;

/// <summary>
/// Writes business rows and their outbox messages in one transaction, in one round trip plus the commit, whatever
/// the batch size (arrays + <c>unnest</c>). The strategy decides where the message goes.
/// </summary>
internal sealed class PgProducer(NpgsqlConnection connection, PgStrategyKind kind, int payloadSize) : IOutboxProducer
{
    private const string InsertOrders =
        """
        INSERT INTO outbox_lab.orders (aggregate_id, aggregate_seq, amount)
        SELECT a, s, s % 1000 + 0.99 FROM unnest($1::int[], $2::bigint[]) AS t(a, s)
        """;

    private const string InsertOutbox =
        """
        INSERT INTO outbox_lab.outbox (aggregate_id, aggregate_seq, payload)
        SELECT * FROM unnest($1::int[], $2::bigint[], $3::bytea[])
        """;

    // Transactional (first argument true): decoded only if, and when, the transaction commits.
    private const string EmitMessages =
        "SELECT count(pg_logical_emit_message(true, 'outbox', p)) FROM unnest($1::bytea[]) AS t(p)";

    public async Task PublishAsync(IReadOnlyList<OutboxEvent> events, TimeSpan holdOpen, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(events);
        var aggregates = events.Select(e => e.AggregateId).ToArray();
        var sequences = events.Select(e => e.Sequence).ToArray();
        var payloads = events.Select(e => EventCodec.Encode(e, payloadSize)).ToArray();

        await using var tx = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        await using var batch = new NpgsqlBatch(connection, tx);

        batch.BatchCommands.Add(Command(InsertOrders, Ints(aggregates), Longs(sequences)));
        if (kind == PgStrategyKind.Logical)
        {
            batch.BatchCommands.Add(Command(EmitMessages, Bytes(payloads)));
        }
        else
        {
            batch.BatchCommands.Add(Command(InsertOutbox, Ints(aggregates), Longs(sequences), Bytes(payloads)));
        }

        if (kind == PgStrategyKind.Notify)
        {
            // Delivered to listeners only at commit; one notification per transaction is enough to wake the relay.
            batch.BatchCommands.Add(Command($"SELECT pg_notify('{PgStrategy.NotifyChannel}', '')"));
        }

        await batch.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        if (holdOpen > TimeSpan.Zero)
        {
            await Task.Delay(holdOpen, cancellationToken).ConfigureAwait(false);
        }

        await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
    }

    public ValueTask DisposeAsync() => connection.DisposeAsync();

    private static NpgsqlBatchCommand Command(string sql, params NpgsqlParameter[] parameters)
    {
        var command = new NpgsqlBatchCommand(sql);
        command.Parameters.AddRange(parameters);
        return command;
    }

    private static NpgsqlParameter Ints(int[] values) => new() { Value = values, NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer };

    private static NpgsqlParameter Longs(long[] values) => new() { Value = values, NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint };

    private static NpgsqlParameter Bytes(byte[][] values) => new() { Value = values, NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bytea };
}
