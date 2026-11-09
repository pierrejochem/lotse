using Lotse.Proto;

namespace Lotse.Client;

/// <summary>
/// One node's answer to <c>DescribeMetrics</c>, from <see cref="LotseClient.NodeMetricsAsync"/>: its metrics, or why
/// there are none.
/// </summary>
/// <param name="NodeId">The node.</param>
/// <param name="Metrics">What it answered, or <c>null</c>.</param>
/// <param name="Error">Why it did not answer, or <c>null</c>.</param>
public sealed record NodeMetrics(uint NodeId, Response.DescribeMetrics? Metrics, ClientException? Error);
