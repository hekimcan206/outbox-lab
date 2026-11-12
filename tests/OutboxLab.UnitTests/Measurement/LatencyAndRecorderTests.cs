using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using OutboxLab.Core.Measurement;
using OutboxLab.Core.Model;

namespace OutboxLab.UnitTests.Measurement;

public sealed class LatencySummaryTests
{
    [Fact]
    public void Nearest_rank_percentiles_on_one_to_hundred()
    {
        var summary = LatencySummary.From(Enumerable.Range(1, 100).Select(i => (double)i));

        summary.Should().Be(new LatencySummary(100, 50, 95, 99, 100, 50.5));
    }

    [Fact]
    public void A_single_sample_is_every_percentile()
    {
        var summary = LatencySummary.From([4.2]);

        summary.P50.Should().Be(4.2);
        summary.P99.Should().Be(4.2);
        summary.Max.Should().Be(4.2);
    }

    [Fact]
    public void No_samples_yields_not_a_number_rather_than_zero()
    {
        var summary = LatencySummary.From([]);

        summary.Count.Should().Be(0);
        double.IsNaN(summary.P50).Should().BeTrue();
    }

    [Property(MaxTest = 300)]
    public Property Percentiles_are_ordered_and_are_real_samples() =>
        Prop.ForAll(Gen.NonEmptyListOf(Gen.Choose(0, 100_000).Select(i => i / 10.0)).ToArbitrary(), samples =>
        {
            var s = LatencySummary.From(samples);
            return s.P50 <= s.P95 && s.P95 <= s.P99 && s.P99 <= s.Max
                   && samples.Contains(s.P50) && samples.Contains(s.P99) && s.Max == samples.Max();
        });
}

public sealed class DeliveryRecorderTests
{
    [Fact]
    public async Task Concurrent_deliveries_are_all_recorded_with_unique_indexes()
    {
        var recorder = new DeliveryRecorder("test");

        await Task.WhenAll(Enumerable.Range(0, 8).Select(w => Task.Run(() =>
        {
            for (var i = 1; i <= 1000; i++)
            {
                recorder.Deliver(new OutboxEvent(w, i, LabClock.Now()), w);
            }
        }, TestContext.Current.CancellationToken)));

        var snapshot = recorder.Snapshot();
        snapshot.Should().HaveCount(8000);
        snapshot.Select(d => d.Index).Should().OnlyHaveUniqueItems();
        recorder.UniqueCount.Should().Be(8000);
        recorder.LastDeliveredAt.Should().BeGreaterThanOrEqualTo(recorder.FirstDeliveredAt);
    }

    [Fact]
    public async Task Waiting_completes_when_enough_distinct_messages_arrive_and_ignores_duplicates()
    {
        var recorder = new DeliveryRecorder();
        var ct = TestContext.Current.CancellationToken;
        var wait = recorder.WaitForUniqueAsync(2, TimeSpan.FromSeconds(5), ct);

        recorder.Deliver(new OutboxEvent(1, 1, 0), 0);
        recorder.Deliver(new OutboxEvent(1, 1, 0), 0);
        wait.IsCompleted.Should().BeFalse("a duplicate is not a new message");

        recorder.Deliver(new OutboxEvent(1, 2, 0), 0);
        (await wait).Should().BeTrue();
    }

    [Fact]
    public async Task Waiting_times_out_with_false()
    {
        var recorder = new DeliveryRecorder();

        var reached = await recorder.WaitForUniqueAsync(1, TimeSpan.FromMilliseconds(50), TestContext.Current.CancellationToken);

        reached.Should().BeFalse();
    }

    [Fact]
    public async Task Quiet_wait_returns_once_deliveries_stop()
    {
        var recorder = new DeliveryRecorder();
        recorder.Deliver(new OutboxEvent(1, 1, 0), 0);
        var started = System.Diagnostics.Stopwatch.StartNew();

        await recorder.WaitForQuietAsync(TimeSpan.FromMilliseconds(200), TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        started.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(4));
    }
}
