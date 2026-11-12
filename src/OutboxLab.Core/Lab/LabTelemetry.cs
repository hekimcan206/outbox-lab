using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace OutboxLab.Core.Lab;

/// <summary>
/// Traces and metrics of the lab, built on System.Diagnostics so the core has no telemetry dependency. The runner
/// exports them over OTLP when <c>OTEL_EXPORTER_OTLP_ENDPOINT</c> is set.
/// </summary>
public static class LabTelemetry
{
    public const string Name = "OutboxLab";

    public static readonly ActivitySource Source = new(Name);

    private static readonly Meter Meter = new(Name);

    /// <summary>Messages handed to the sink, tagged with the strategy.</summary>
    public static readonly Counter<long> Delivered = Meter.CreateCounter<long>("outbox.delivered", unit: "{message}");

    /// <summary>Commit-to-delivery latency, tagged with the strategy.</summary>
    public static readonly Histogram<double> DeliveryLatency = Meter.CreateHistogram<double>("outbox.delivery.latency", unit: "ms");

    /// <summary>Business transactions committed by producers, tagged with the strategy.</summary>
    public static readonly Counter<long> Published = Meter.CreateCounter<long>("outbox.published", unit: "{transaction}");
}
