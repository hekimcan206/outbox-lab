using DotNet.Testcontainers.Configurations;
using Oracle.ManagedDataAccess.Client;
using OutboxLab.Core.Measurement;
using OutboxLab.Core.Model;
using OutboxLab.Oracle;
using Testcontainers.Oracle;

[assembly: AssemblyFixture(typeof(OutboxLab.OracleTests.OracleFixture))]

namespace OutboxLab.OracleTests;

/// <summary>
/// One Oracle Database 23ai Free container per test run, initialised by the same script docker compose mounts.
/// Opt-in with <c>OUTBOX_ORACLE_TESTS=1</c>: the image is 6.5 GB and needs about 2.5 GB of memory, so the default CI
/// job skips it and a dedicated job runs it.
/// </summary>
public sealed class OracleFixture : IAsyncLifetime
{
    // Test-only credentials for a throwaway container.
    private const string OwnerPassword = "OwnerTest1";
    private const string AppPassword = "AppTest1";
    private const string RelayPassword = "RelayTest1";

    private OracleContainer? _container;

    public static bool Enabled => Environment.GetEnvironmentVariable("OUTBOX_ORACLE_TESTS") == "1";

    public static string SkipReason => "Set OUTBOX_ORACLE_TESTS=1 to run the Oracle tests (gvenzl/oracle-free, about 2.5 GB of memory).";

    public OraConnections Connections { get; private set; } = null!;

    public async ValueTask InitializeAsync()
    {
        if (!Enabled)
        {
            return;
        }

        _container = new OracleBuilder("gvenzl/oracle-free:23-slim-faststart")
            .WithEnvironment("OWNER_PASSWORD", OwnerPassword)
            .WithEnvironment("APP_PASSWORD", AppPassword)
            .WithEnvironment("RELAY_PASSWORD", RelayPassword)
            .WithResourceMapping(
                File.ReadAllBytes(Path.Combine(AppContext.BaseDirectory, "oracle-init", "01-outbox-lab.sh")),
                "/container-entrypoint-initdb.d/01-outbox-lab.sh",
                fileMode: UnixFileModes.UserRead | UnixFileModes.UserWrite | UnixFileModes.UserExecute | UnixFileModes.GroupRead | UnixFileModes.OtherRead)
            .WithLabel("project", "outbox-lab-tests")
            .Build();
        await _container.StartAsync();
        Connections = new OraConnections(
            WithCredentials("outbox_owner", OwnerPassword),
            WithCredentials("outbox_app", AppPassword),
            WithCredentials("outbox_relay", RelayPassword));
    }

    public async ValueTask DisposeAsync()
    {
        if (_container is not null)
        {
            await _container.DisposeAsync();
        }
    }

    public OraStrategy Strategy(OraStrategyKind kind) => new(kind, Connections);

    private string WithCredentials(string user, string password) =>
        new OracleConnectionStringBuilder(_container!.GetConnectionString())
        {
            UserID = user,
            Password = password,
            ConnectionTimeout = 60,
        }.ConnectionString;
}

/// <summary>Runs a relay in the background for a test and stops it cleanly.</summary>
internal sealed class RelayHost : IAsyncDisposable
{
    private readonly CancellationTokenSource _cts = new();

    private RelayHost(IOutboxRelay relay, IDeliverySink sink)
    {
        Completion = Task.Run(() => relay.RunAsync(sink, _cts.Token));
    }

    public Task Completion { get; }

    public static RelayHost Start(IOutboxStrategy strategy, RelayOptions options, IDeliverySink sink) =>
        new(strategy.CreateRelay(options), sink);

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();
        try
        {
            await Completion;
        }
        catch (OperationCanceledException)
        {
            // Expected: relays run until cancelled.
        }
        catch (OracleException) when (_cts.IsCancellationRequested)
        {
            // The cancellation can interrupt a call in flight.
        }
        catch (InvalidOperationException) when (_cts.IsCancellationRequested)
        {
            // A relay a test crashed on purpose; the test has already asserted the failure.
        }

        _cts.Dispose();
    }
}

/// <summary>Records deliveries and fails on the n-th one, to simulate a relay crash before acknowledging.</summary>
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
