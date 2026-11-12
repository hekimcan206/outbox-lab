using Npgsql;
using OutboxLab.Core.Measurement;
using OutboxLab.Core.Model;
using OutboxLab.Postgres;
using Testcontainers.PostgreSql;

[assembly: AssemblyFixture(typeof(OutboxLab.PostgresTests.PostgresFixture))]

namespace OutboxLab.PostgresTests;

/// <summary>
/// One PostgreSQL 17 container per test run, initialised by the same script docker compose mounts, with
/// <c>wal_level=logical</c> for the logical decoding strategy.
/// </summary>
public sealed class PostgresFixture : IAsyncLifetime
{
    // Test-only credentials for a throwaway container.
    private const string AppPassword = "app-test-only";
    private const string RelayPassword = "relay-test-only";

    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder("postgres:17-alpine")
        .WithDatabase("outbox")
        .WithCommand("-c", "wal_level=logical", "-c", "max_replication_slots=4", "-c", "max_wal_senders=4")
        .WithEnvironment("APP_PASSWORD", AppPassword)
        .WithEnvironment("RELAY_PASSWORD", RelayPassword)
        .WithResourceMapping(
            File.ReadAllBytes(Path.Combine(AppContext.BaseDirectory, "postgres-init", "01-outbox-lab.sh")),
            "/docker-entrypoint-initdb.d/01-outbox-lab.sh")
        .WithLabel("project", "outbox-lab-tests")
        .Build();

    public PgConnections Connections { get; private set; } = null!;

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();
        var admin = new NpgsqlConnectionStringBuilder(_container.GetConnectionString()) { Timeout = 30, CommandTimeout = 60 };
        Connections = new PgConnections(
            admin.ConnectionString,
            WithCredentials(admin, "outbox_app", AppPassword),
            WithCredentials(admin, "outbox_relay", RelayPassword));
    }

    public async ValueTask DisposeAsync() => await _container.DisposeAsync();

    public PgStrategy Strategy(PgStrategyKind kind) => new(kind, Connections);

    private static string WithCredentials(NpgsqlConnectionStringBuilder admin, string user, string password) =>
        new NpgsqlConnectionStringBuilder(admin.ConnectionString) { Username = user, Password = password }.ConnectionString;
}

/// <summary>Runs a relay in the background for a test and stops it cleanly.</summary>
internal sealed class RelayHost : IAsyncDisposable
{
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _task;

    private RelayHost(IOutboxRelay relay, IDeliverySink sink)
    {
        _task = Task.Run(() => relay.RunAsync(sink, _cts.Token));
    }

    public Task Completion => _task;

    public static RelayHost Start(IOutboxStrategy strategy, RelayOptions options, IDeliverySink sink) =>
        new(strategy.CreateRelay(options), sink);

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();
        try
        {
            await _task;
        }
        catch (OperationCanceledException)
        {
            // Expected: relays run until cancelled.
        }
        catch (InvalidOperationException) when (_cts.IsCancellationRequested)
        {
            // Npgsql may report a connection broken by the cancellation itself.
        }
        catch (NpgsqlException) when (_cts.IsCancellationRequested)
        {
            // Same: the cancellation tore down the connection mid-command.
        }

        _cts.Dispose();
    }
}

/// <summary>Records deliveries and can fail on demand, to simulate a relay crash before acknowledging.</summary>
internal sealed class FailingSink(int failOnDelivery) : IDeliverySink
{
    private int _calls;

    public DeliveryRecorder Recorder { get; } = new();

    public void Deliver(OutboxEvent message, int worker)
    {
        if (Interlocked.Increment(ref _calls) == failOnDelivery)
        {
            throw new InvalidOperationException("simulated crash before commit");
        }

        Recorder.Deliver(message, worker);
    }
}
