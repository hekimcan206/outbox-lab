using Npgsql;
using OutboxLab.Core.Model;

namespace OutboxLab.Postgres;

/// <summary>The PostgreSQL outbox strategies the lab compares.</summary>
public enum PgStrategyKind
{
    /// <summary>Workers poll with <c>FOR UPDATE SKIP LOCKED</c> and sleep for the poll interval when idle.</summary>
    SkipLocked,

    /// <summary>As <see cref="SkipLocked"/>, but producers <c>NOTIFY</c> and idle workers wait on <c>LISTEN</c>.</summary>
    Notify,

    /// <summary>One reader takes only rows whose transaction is older than the snapshot xmin, in (txid, id) order.</summary>
    Xmin,

    /// <summary>One reader follows a high-water mark on the identity column. Included because it loses messages.</summary>
    IdCursor,

    /// <summary>Producers write a transactional logical decoding message; the relay streams it with pgoutput.</summary>
    Logical,
}

/// <summary>Connection strings for the three roles of <c>deploy/postgres/init/01-outbox-lab.sh</c>.</summary>
/// <param name="Admin">Superuser, used only to reset state between runs.</param>
/// <param name="App">outbox_app: the business service that writes orders and outbox messages.</param>
/// <param name="Relay">outbox_relay: reads and deletes outbox rows; REPLICATION for logical decoding.</param>
public sealed record PgConnections(string Admin, string App, string Relay);

/// <summary>One PostgreSQL outbox strategy: how producers write and how the relay reads.</summary>
public sealed class PgStrategy : IOutboxStrategy, IAsyncDisposable
{
    internal const string NotifyChannel = "outbox_lab";
    internal const string SlotName = "outbox_lab_slot";
    internal const string PublicationName = "outbox_lab_pub";
    internal const string MessagePrefix = "outbox";

    private readonly NpgsqlDataSource _admin;
    private readonly NpgsqlDataSource _app;
    private readonly NpgsqlDataSource _relay;
    private readonly PgConnections _connections;

    public PgStrategy(PgStrategyKind kind, PgConnections connections)
    {
        ArgumentNullException.ThrowIfNull(connections);
        Kind = kind;
        _connections = connections;
        _admin = NpgsqlDataSource.Create(connections.Admin);
        _app = NpgsqlDataSource.Create(connections.App);
        _relay = NpgsqlDataSource.Create(connections.Relay);
    }

    public PgStrategyKind Kind { get; }

    public string Id => Kind switch
    {
        PgStrategyKind.SkipLocked => "pg-skip-locked",
        PgStrategyKind.Notify => "pg-notify",
        PgStrategyKind.Xmin => "pg-xmin",
        PgStrategyKind.IdCursor => "pg-id-cursor",
        PgStrategyKind.Logical => "pg-logical",
        _ => throw new InvalidOperationException($"Unknown strategy {Kind}."),
    };

    public string Engine => "PostgreSQL";

    public string OrderingModel => Kind switch
    {
        PgStrategyKind.SkipLocked or PgStrategyKind.Notify => "per worker batch only; workers race each other",
        PgStrategyKind.Xmin => "commit-horizon order (txid, id); one reader",
        PgStrategyKind.IdCursor => "id order; skips rows that commit behind the cursor",
        PgStrategyKind.Logical => "commit order (WAL); one stream",
        _ => "unknown",
    };

    public bool SupportsParallelWorkers => Kind is PgStrategyKind.SkipLocked or PgStrategyKind.Notify;

    /// <summary>Empties the business table and the outbox, and recreates the replication slot only when it is needed.</summary>
    /// <remarks>
    /// An unused slot pins WAL forever, so every non-logical reset drops it. A logical reset recreates it, which puts
    /// its start position after everything earlier runs wrote.
    /// </remarks>
    public async Task ResetAsync(CancellationToken cancellationToken)
    {
        await using var conn = await _admin.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using (var drop = new NpgsqlCommand(
            """
            SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = $1 AND active_pid IS NOT NULL;
            """, conn))
        {
            drop.Parameters.AddWithValue(SlotName);
            await drop.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        // A terminated walsender releases the slot asynchronously; retry the drop briefly.
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                await using var drop = new NpgsqlCommand(
                    "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1", conn);
                drop.Parameters.AddWithValue(SlotName);
                await drop.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                break;
            }
            catch (PostgresException e) when (e.SqlState == PostgresErrorCodes.ObjectInUse && attempt < 50)
            {
                await Task.Delay(100, cancellationToken).ConfigureAwait(false);
            }
        }

        await using (var truncate = new NpgsqlCommand("TRUNCATE outbox_lab.orders, outbox_lab.outbox RESTART IDENTITY", conn))
        {
            await truncate.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        if (Kind == PgStrategyKind.Logical)
        {
            await using var create = new NpgsqlCommand("SELECT pg_create_logical_replication_slot($1, 'pgoutput')", conn);
            create.Parameters.AddWithValue(SlotName);
            await create.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    public async Task<IOutboxProducer> CreateProducerAsync(RelayOptions options, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(options);
        var conn = await _app.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        return new PgProducer(conn, Kind, options.PayloadSize);
    }

    public IOutboxRelay CreateRelay(RelayOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        return Kind switch
        {
            PgStrategyKind.SkipLocked => new PgSkipLockedRelay(_relay, options, useNotify: false),
            PgStrategyKind.Notify => new PgSkipLockedRelay(_relay, options, useNotify: true),
            PgStrategyKind.Xmin => new PgXminRelay(_relay, options),
            PgStrategyKind.IdCursor => new PgIdCursorRelay(_relay, options),
            PgStrategyKind.Logical => new PgLogicalRelay(_connections.Relay),
            _ => throw new InvalidOperationException($"Unknown strategy {Kind}."),
        };
    }

    public async ValueTask DisposeAsync()
    {
        await _admin.DisposeAsync().ConfigureAwait(false);
        await _app.DisposeAsync().ConfigureAwait(false);
        await _relay.DisposeAsync().ConfigureAwait(false);
    }
}
