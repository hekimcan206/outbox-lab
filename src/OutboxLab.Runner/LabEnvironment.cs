using Microsoft.Extensions.Logging;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using OutboxLab.Core.Model;
using OutboxLab.Oracle;
using OutboxLab.Postgres;

namespace OutboxLab.Runner;

/// <summary>
/// Where the databases are, read from the environment. The defaults match <c>docker compose up</c> on the host's
/// loopback ports, with the development-only passwords from <c>.env.example</c>.
/// </summary>
internal sealed class LabEnvironment
{
    private LabEnvironment(PgConnections postgres, OraConnections oracle)
    {
        Postgres = postgres;
        Oracle = oracle;
    }

    public PgConnections Postgres { get; }

    public OraConnections Oracle { get; }

    public static LabEnvironment FromEnvironment()
    {
        var pgHost = Env("OUTBOX_PG_HOST", "127.0.0.1");
        var pgPort = Env("OUTBOX_PG_PORT", "57432");
        var pgBase = $"Host={pgHost};Port={pgPort};Database=outbox;Timeout=30;Command Timeout=120;Maximum Pool Size=200";
        var postgres = new PgConnections(
            $"{pgBase};Username=postgres;Password={Env("POSTGRES_PASSWORD", "postgres-dev-only")}",
            $"{pgBase};Username=outbox_app;Password={Env("APP_PASSWORD", "app-dev-only")}",
            $"{pgBase};Username=outbox_relay;Password={Env("RELAY_PASSWORD", "relay-dev-only")}");

        var oraHost = Env("OUTBOX_ORA_HOST", "127.0.0.1");
        var oraPort = Env("OUTBOX_ORA_PORT", "57521");
        var dataSource = $"//{oraHost}:{oraPort}/FREEPDB1";
        var oracle = new OraConnections(
            Ora(dataSource, "outbox_owner", Env("ORA_OWNER_PASSWORD", "OwnerDev1")),
            Ora(dataSource, "outbox_app", Env("ORA_APP_PASSWORD", "AppDev1")),
            Ora(dataSource, "outbox_relay", Env("ORA_RELAY_PASSWORD", "RelayDev1")));

        return new LabEnvironment(postgres, oracle);
    }

    public static IReadOnlyList<string> AllStrategyIds { get; } =
        ["pg-skip-locked", "pg-notify", "pg-xmin", "pg-id-cursor", "pg-logical", "ora-skip-locked", "ora-aq"];

    public IOutboxStrategy Create(string id) => id switch
    {
        "pg-skip-locked" => new PgStrategy(PgStrategyKind.SkipLocked, Postgres),
        "pg-notify" => new PgStrategy(PgStrategyKind.Notify, Postgres),
        "pg-xmin" => new PgStrategy(PgStrategyKind.Xmin, Postgres),
        "pg-id-cursor" => new PgStrategy(PgStrategyKind.IdCursor, Postgres),
        "pg-logical" => new PgStrategy(PgStrategyKind.Logical, Postgres),
        "ora-skip-locked" => new OraStrategy(OraStrategyKind.SkipLocked, Oracle),
        "ora-aq" => new OraStrategy(OraStrategyKind.AdvancedQueuing, Oracle),
        _ => throw new ArgumentException($"Unknown strategy '{id}'. Known: {string.Join(", ", AllStrategyIds)}.", nameof(id)),
    };

    /// <summary>
    /// Waits until the lab users can log in. docker compose reports Oracle healthy when the database is open, which can
    /// be before the init script has created the users and the queue, so readiness is checked as those users.
    /// </summary>
    public async Task WaitUntilReadyAsync(bool postgres, bool oracle, ILogger logger, CancellationToken cancellationToken)
    {
        var deadline = DateTime.UtcNow.AddMinutes(10);
        if (postgres)
        {
            await RetryAsync("PostgreSQL", async () =>
            {
                await using var conn = new NpgsqlConnection(Postgres.Relay);
                await conn.OpenAsync(cancellationToken);
                await using var cmd = new NpgsqlCommand("SELECT count(*) FROM outbox_lab.outbox", conn);
                await cmd.ExecuteScalarAsync(cancellationToken);
            }, deadline, logger, cancellationToken);
        }

        if (oracle)
        {
            await RetryAsync("Oracle", async () =>
            {
                await using var conn = new OracleConnection(Oracle.Relay);
                await conn.OpenAsync(cancellationToken);
                await using var cmd = new OracleCommand("SELECT COUNT(*) FROM outbox_owner.outbox", conn);
                await cmd.ExecuteScalarAsync(cancellationToken);
            }, deadline, logger, cancellationToken);
        }
    }

    private static async Task RetryAsync(string name, Func<Task> probe, DateTime deadline, ILogger logger, CancellationToken cancellationToken)
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await probe();
                Log.Ready(logger, name);
                return;
            }
            catch (Exception e) when ((e is NpgsqlException or OracleException or InvalidOperationException) && DateTime.UtcNow < deadline)
            {
                if (attempt % 10 == 1)
                {
                    Log.Waiting(logger, name, e.Message);
                }

                await Task.Delay(TimeSpan.FromSeconds(3), cancellationToken);
            }
        }
    }

    private static string Ora(string dataSource, string user, string password) =>
        new OracleConnectionStringBuilder
        {
            DataSource = dataSource,
            UserID = user,
            Password = password,
            ConnectionTimeout = 60,
            MaxPoolSize = 200,
        }.ConnectionString;

    private static string Env(string name, string fallback) =>
        Environment.GetEnvironmentVariable(name) is { Length: > 0 } value ? value : fallback;
}
