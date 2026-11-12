using System.Data;
using Oracle.ManagedDataAccess.Client;
using OutboxLab.Core.Model;

namespace OutboxLab.Oracle;

/// <summary>
/// Writes business rows and their outbox messages in one transaction. Both strategies run one anonymous PL/SQL block
/// with array binding: one round trip for the whole batch, plus the commit, exactly like the PostgreSQL producer.
/// </summary>
internal sealed class OraProducer(OracleConnection connection, OraStrategyKind kind, int payloadSize) : IOutboxProducer
{
    private const string TableBlock =
        """
        BEGIN
            INSERT INTO outbox_owner.orders (aggregate_id, aggregate_seq, amount) VALUES (:a, :s, MOD(:s, 1000) + 0.99);
            INSERT INTO outbox_owner.outbox (aggregate_id, aggregate_seq, payload) VALUES (:a, :s, :p);
        END;
        """;

    // Default enqueue options: visibility ON_COMMIT, so the message is part of this transaction and becomes
    // visible to dequeuers exactly when (and only if) it commits.
    private const string QueueBlock =
        """
        DECLARE
            eo  DBMS_AQ.ENQUEUE_OPTIONS_T;
            mp  DBMS_AQ.MESSAGE_PROPERTIES_T;
            mid RAW(16);
        BEGIN
            INSERT INTO outbox_owner.orders (aggregate_id, aggregate_seq, amount) VALUES (:a, :s, MOD(:s, 1000) + 0.99);
            DBMS_AQ.ENQUEUE(queue_name => 'OUTBOX_OWNER.OUTBOX_Q', enqueue_options => eo,
                            message_properties => mp, payload => :p, msgid => mid);
        END;
        """;

    public async Task PublishAsync(IReadOnlyList<OutboxEvent> events, TimeSpan holdOpen, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(events);
        if (events.Count == 0)
        {
            return;
        }

        await using var tx = (OracleTransaction)await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        await using (var cmd = connection.CreateCommand())
        {
            cmd.Transaction = tx;
            cmd.BindByName = true;
            cmd.CommandText = kind == OraStrategyKind.SkipLocked ? TableBlock : QueueBlock;
            cmd.ArrayBindCount = events.Count;
            cmd.Parameters.Add("a", OracleDbType.Int32, events.Select(e => e.AggregateId).ToArray(), ParameterDirection.Input);
            cmd.Parameters.Add("s", OracleDbType.Int64, events.Select(e => e.Sequence).ToArray(), ParameterDirection.Input);
            cmd.Parameters.Add("p", OracleDbType.Raw, events.Select(e => EventCodec.Encode(e, payloadSize)).ToArray(), ParameterDirection.Input);
            await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        if (holdOpen > TimeSpan.Zero)
        {
            await Task.Delay(holdOpen, cancellationToken).ConfigureAwait(false);
        }

        await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
    }

    public ValueTask DisposeAsync() => connection.DisposeAsync();
}
