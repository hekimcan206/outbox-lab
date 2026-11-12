using System.Diagnostics;

namespace OutboxLab.Core.Model;

/// <summary>
/// The single clock every latency is measured on. Producers and relays run in the same process, so a monotonic
/// <see cref="Stopwatch"/> timestamp is comparable across them without any clock-synchronisation error.
/// </summary>
public static class LabClock
{
    public static long Now() => Stopwatch.GetTimestamp();

    public static double ToMilliseconds(long elapsedTicks) => elapsedTicks * 1000.0 / Stopwatch.Frequency;

    public static double MillisecondsBetween(long from, long to) => ToMilliseconds(to - from);
}
