using System;
using System.Diagnostics;

namespace Lotse.Net;

/// <summary>
/// A monotonic clock in nanoseconds, so tests can move session times without waiting. Only differences mean anything,
/// and they may overflow: compare with subtraction.
/// </summary>
public delegate long Ticker();

/// <summary>The clocks connections use.</summary>
public static class Tickers
{
    /// <summary>The system's monotonic clock.</summary>
    public static readonly Ticker System = () => (long)((Int128)Stopwatch.GetTimestamp() * 1_000_000_000 / Stopwatch.Frequency);
}
