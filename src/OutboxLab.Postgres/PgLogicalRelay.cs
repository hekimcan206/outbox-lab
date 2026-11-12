using Npgsql.Replication;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;
using OutboxLab.Core.Model;

namespace OutboxLab.Postgres;

/// <summary>
/// Streams the outbox out of the WAL. Producers call <c>pg_logical_emit_message(true, 'outbox', payload)</c> inside
/// the business transaction; logical decoding emits the message only when that transaction commits, in commit order,
/// and pgoutput delivers it here. There is no outbox table to poll, index, delete from or vacuum.
/// </summary>
/// <remarks>
/// Progress is acknowledged per transaction after delivery (at-least-once). A restart resumes from the slot's
/// confirmed position and may redeliver the last unacknowledged transactions. The slot pins WAL while the relay is
/// down, which is the operational cost of this strategy (see docs/adr/0003).
/// </remarks>
internal sealed class PgLogicalRelay(string connectionString) : IOutboxRelay
{
    public async Task RunAsync(IDeliverySink sink, CancellationToken cancellationToken)
    {
        await using var conn = new LogicalReplicationConnection(connectionString)
        {
            // Report progress every second so the server can recycle WAL promptly; the default is 10 s.
            WalReceiverStatusInterval = TimeSpan.FromSeconds(1),
        };
        await conn.Open(cancellationToken).ConfigureAwait(false);

        var slot = new PgOutputReplicationSlot(PgStrategy.SlotName);
        var options = new PgOutputReplicationOptions(PgStrategy.PublicationName, PgOutputProtocolVersion.V1, messages: true);
        using var buffer = new MemoryStream(EventCodec.MaxPayloadSize);

        await foreach (var message in conn.StartReplication(slot, options, cancellationToken).ConfigureAwait(false))
        {
            switch (message)
            {
                case LogicalDecodingMessage { Prefix: PgStrategy.MessagePrefix } outbox:
                    // The payload stream belongs to the replication buffer: consume it before the next message.
                    buffer.SetLength(0);
                    await outbox.Data.CopyToAsync(buffer, cancellationToken).ConfigureAwait(false);
                    sink.Deliver(EventCodec.Decode(buffer.GetBuffer().AsSpan(0, (int)buffer.Length)), 0);
                    break;

                case CommitMessage commit:
                    conn.SetReplicationStatus(commit.TransactionEndLsn);
                    break;
            }
        }
    }
}
