using OutboxLab.Core.Lab;
using OutboxLab.Core.Measurement;

namespace OutboxLab.UnitTests.Lab;

public sealed class ReportWriterTests
{
    private static readonly ScenarioResult[] Sample =
    [
        new(Scenarios.Drain, "pg-skip-locked", "PostgreSQL 17", 4, 4, 20_000, 1.25, 16_000, Ordering: new OrderingReport(20_000, 20_000, 0, 0, 312, 40)),
        new(Scenarios.Latency, "ora-aq", "Oracle 23ai Free", 1, 4, 3_000, 11, 272.7, 250, new LatencySummary(2_750, 3.21, 7.5, 12.04, 30.9, 3.9), new OrderingReport(3_000, 3_000, 0, 0, 0, 0), "poll/wait 100 ms"),
        new(Scenarios.Ordering, "pg-id-cursor", "PostgreSQL 17", 1, 4, 2_000, 8.4, 230.1, Ordering: new OrderingReport(1_987, 1_987, 0, 13, 0, 0), Notes: "hold 0-5 ms"),
        new(Scenarios.Publish, "pg-notify", "PostgreSQL 17", 0, 32, 41_234, 10.0, 4_123.4, Latency: new LatencySummary(41_234, 7.456, 0, 21.333, 50, 7.7)),
        new(Scenarios.Drain, "pg-logical", "PostgreSQL 17", 1, 4, 20_000, double.NaN, double.NaN),
    ];

    [Fact]
    public void Markdown_matches_the_golden_file()
    {
        var expected = File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Golden", "report.md")).ReplaceLineEndings("\n");

        ReportWriter.ToMarkdown(Sample).Should().Be(expected);
    }

    [Fact]
    public void Empty_input_renders_nothing()
    {
        ReportWriter.ToMarkdown([]).Should().BeEmpty();
    }
}
