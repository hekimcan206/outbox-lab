using Oracle.ManagedDataAccess.Client;
using OutboxLab.Core.Model;

namespace OutboxLab.Oracle;

/// <summary>The Oracle outbox strategies the lab compares.</summary>
public enum OraStrategyKind
{
    /// <summary>Workers poll the outbox table with <c>FOR UPDATE SKIP LOCKED</c> and sleep when idle.</summary>
    SkipLocked,

    /// <summary>Producers enqueue to Advanced Queuing in the business transaction; workers block on dequeue.</summary>
    AdvancedQueuing,
}

/// <summary>Connection strings for the three users of <c>deploy/oracle/init/01-outbox-lab.sh</c>.</summary>
/// <param name="Owner">outbox_owner: owns the tables and the queue; used only to reset state between runs.</param>
/// <param name="App">outbox_app: the business service.</param>
/// <param name="Relay">outbox_relay: reads and deletes outbox rows, dequeues.</param>
public sealed record OraConnections(string Owner, string App, string Relay);

/// <summary>One Oracle outbox strategy: how producers write and how the relay reads.</summary>
public sealed class OraStrategy(OraStrategyKind kind, OraConnections connections) : IOutboxStrategy
{
    internal const string QueueName = "OUTBOX_OWNER.OUTBOX_Q";

    private readonly OraConnections _connections = connections ?? throw new ArgumentNullException(nameof(connections));

    public OraStrategyKind Kind { get; } = kind;

    public string Id => Kind == OraStrategyKind.SkipLocked ? "ora-skip-locked" : "ora-aq";

    public string Engine => "Oracle";

    public string OrderingModel => Kind == OraStrategyKind.SkipLocked
        ? "per worker batch only; workers race each other"
        : "enqueue-time FIFO per dequeuer; dequeuers race each other";

    public bool SupportsParallelWorkers => true;

    /// <summary>Empties the business table, the outbox and the queue.</summary>
    public async Task ResetAsync(CancellationToken cancellationToken)
    {
        await using var conn = new OracleConnection(_connections.Owner);
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
        foreach (var sql in new[]
                 {
                     "TRUNCATE TABLE orders",
                     "TRUNCATE TABLE outbox",
                     """
                     DECLARE
                         po DBMS_AQADM.AQ$_PURGE_OPTIONS_T;
                     BEGIN
                         po.block := TRUE;
                         DBMS_AQADM.PURGE_QUEUE_TABLE(queue_table => 'outbox_qt', purge_condition => NULL, purge_options => po);
                     END;
                     """,
                 })
        {
            await using var cmd = new OracleCommand(sql, conn);
            await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    public async Task<IOutboxProducer> CreateProducerAsync(RelayOptions options, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(options);
        var conn = new OracleConnection(_connections.App);
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
        return new OraProducer(conn, Kind, options.PayloadSize);
    }

    public IOutboxRelay CreateRelay(RelayOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        return Kind == OraStrategyKind.SkipLocked
            ? new OraSkipLockedRelay(_connections.Relay, options)
            : new OraAqRelay(_connections.Relay, options);
    }
}
