package dev.lotse.client;

import dev.lotse.proto.Response;

/**
 * One node's answer to {@code DescribeSessions}, from {@link LotseClient#nodeSessions}: its open sessions, or why
 * there are none (ADR 0121).
 *
 * @param nodeId the node
 * @param sessions what it answered, or {@code null}
 * @param error why it did not answer, or {@code null}
 */
public record NodeSessions(long nodeId, Response.DescribeSessions sessions, ClientException error) {}
