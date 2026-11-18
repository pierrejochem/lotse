using Lotse.Proto;

namespace Lotse.Client;

/// <summary>
/// One node's answer to <c>DescribeSessions</c>, from <see cref="LotseClient.NodeSessionsAsync"/>: its open sessions,
/// or why there are none (ADR 0121).
/// </summary>
/// <param name="NodeId">The node.</param>
/// <param name="Sessions">What it answered, or <c>null</c>.</param>
/// <param name="Error">Why it did not answer, or <c>null</c>.</param>
public sealed record NodeSessions(uint NodeId, Response.DescribeSessions? Sessions, ClientException? Error);
