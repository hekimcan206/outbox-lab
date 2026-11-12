using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Logging;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using OutboxLab.Core.Lab;
using OutboxLab.Runner;

// outbox-lab demo                       quick run of every strategy with pass/fail checks (docker compose demo)
// outbox-lab bench <scenario|all>       the benchmark matrix; raw runs and medians go to --out
//   --engines pg,ora   --runs 3   --out benchmark-results   --strategies pg-xmin,ora-aq (filter)
var command = args.FirstOrDefault() ?? "demo";
var options = Options.Parse(args.Skip(1).ToArray());

using var loggerFactory = LoggerFactory.Create(b =>
{
    if (Environment.GetEnvironmentVariable("OUTBOX_LOG_FORMAT") == "json")
    {
        b.AddJsonConsole(o => o.TimestampFormat = "O");
    }
    else
    {
        b.AddSimpleConsole(o =>
        {
            o.SingleLine = true;
            o.TimestampFormat = "HH:mm:ss ";
        });
    }
});
var logger = loggerFactory.CreateLogger("outbox-lab");

// Traces and metrics leave the process only when an OTLP endpoint is configured (OTEL_EXPORTER_OTLP_ENDPOINT).
var otlp = !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("OTEL_EXPORTER_OTLP_ENDPOINT"));
var resource = ResourceBuilder.CreateDefault().AddService("outbox-lab");
using var tracing = otlp ? Sdk.CreateTracerProviderBuilder().SetResourceBuilder(resource).AddSource(LabTelemetry.Name).AddOtlpExporter().Build() : null;
using var metrics = otlp ? Sdk.CreateMeterProviderBuilder().SetResourceBuilder(resource).AddMeter(LabTelemetry.Name).AddOtlpExporter().Build() : null;

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

var steps = command switch
{
    "demo" => LabPlans.Demo(),
    "bench" when options.Scenario == "all" =>
        [.. new[] { Scenarios.Publish, Scenarios.Drain, Scenarios.Ordering, Scenarios.Latency }.SelectMany(LabPlans.Bench)],
    "bench" => LabPlans.Bench(options.Scenario),
    _ => null,
};
if (steps is null)
{
    Console.Error.WriteLine("usage: outbox-lab demo | bench <drain|latency|ordering|publish|all> [--engines pg,ora] [--runs n] [--out dir] [--strategies a,b]");
    return 2;
}

steps = [.. steps.Where(s => options.Includes(s.StrategyId))];
var environment = LabEnvironment.FromEnvironment();
await environment.WaitUntilReadyAsync(
    steps.Any(s => s.StrategyId.StartsWith("pg-", StringComparison.Ordinal)),
    steps.Any(s => s.StrategyId.StartsWith("ora-", StringComparison.Ordinal)),
    logger, cts.Token);

Log.Starting(logger, command, steps.Count, options.Runs);
var runner = new ScenarioRunner(new ScenarioSettings(), message => Log.Progress(logger, message));
var medians = new List<ScenarioResult>();
var raw = new List<RunRecord>();

foreach (var step in steps)
{
    var strategy = environment.Create(step.StrategyId);
    try
    {
        var runs = new List<ScenarioResult>();
        for (var run = 1; run <= options.Runs; run++)
        {
            var result = await step.Run(runner, strategy, cts.Token);
            runs.Add(result);
            raw.Add(new RunRecord(run, result));
            Log.RunFinished(logger, result.Scenario, result.Strategy, result.Workers, run, result.ThroughputPerSecond, result.Latency?.P50 ?? double.NaN,
                result.Ordering?.Missing, result.Ordering?.Inversions);
        }

        medians.Add(runs.OrderBy(step.MedianKey).ElementAt((runs.Count - 1) / 2));
    }
    finally
    {
        if (strategy is IAsyncDisposable disposable)
        {
            await disposable.DisposeAsync();
        }
    }
}

var report = ReportWriter.ToMarkdown(medians);
Console.WriteLine();
Console.WriteLine(report);

if (options.Out is { } outDir)
{
    Directory.CreateDirectory(outDir);
    var name = command == "demo" ? "demo" : options.Scenario;
    var json = new JsonSerializerOptions
    {
        WriteIndented = true,
        NumberHandling = JsonNumberHandling.AllowNamedFloatingPointLiterals,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };
    await File.WriteAllTextAsync(Path.Combine(outDir, $"{name}.json"),
        JsonSerializer.Serialize(new { generatedUtc = DateTime.UtcNow, runsPerConfiguration = options.Runs, medians, runs = raw }, json), cts.Token);
    await File.WriteAllTextAsync(Path.Combine(outDir, $"{name}.md"), report, cts.Token);
    Log.Wrote(logger, outDir, name);
}

if (command != "demo")
{
    return 0;
}

// The demo's checks: what each strategy promises must hold on this run.
var failures = new List<string>();
foreach (var r in medians)
{
    if (r.Strategy != "pg-id-cursor" && r.Ordering is { Missing: > 0 } lost)
    {
        failures.Add($"{r.Scenario}/{r.Strategy}: {lost.Missing} messages lost");
    }

    if (r.Scenario == Scenarios.Ordering && LabPlans.OrderedInDemo.Contains(r.Strategy) && r.Ordering is { Inversions: > 0 } inverted)
    {
        failures.Add($"{r.Scenario}/{r.Strategy}: {inverted.Inversions} per-aggregate inversions");
    }
}

var idCursorLoss = medians.FirstOrDefault(r => r.Strategy == "pg-id-cursor")?.Ordering?.Missing;
Console.WriteLine(JsonSerializer.Serialize(new { ok = failures.Count == 0, failures, idCursorLost = idCursorLoss }));
return failures.Count == 0 ? 0 : 1;

internal sealed record RunRecord(int Run, ScenarioResult Result);

internal sealed record Options(string Scenario, int Runs, string? Out, HashSet<string>? Engines, HashSet<string>? Strategies)
{
    public static Options Parse(string[] args)
    {
        var scenario = args.FirstOrDefault(a => !a.StartsWith("--", StringComparison.Ordinal)) ?? "all";
        string? Value(string name)
        {
            var i = Array.IndexOf(args, name);
            return i >= 0 && i + 1 < args.Length ? args[i + 1] : null;
        }

        static HashSet<string>? Set(string? csv) => csv is null ? null : [.. csv.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)];

        return new Options(
            scenario,
            int.TryParse(Value("--runs"), out var runs) && runs > 0 ? runs : 1,
            Value("--out"),
            Set(Value("--engines")),
            Set(Value("--strategies")));
    }

    public bool Includes(string strategyId) =>
        (Engines is null || Engines.Contains(strategyId.Split('-')[0])) &&
        (Strategies is null || Strategies.Contains(strategyId));
}
