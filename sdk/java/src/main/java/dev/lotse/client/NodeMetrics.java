package dev.lotse.client;

import dev.lotse.proto.Response;

/**
 * One node's answer to {@code DescribeMetrics}, from {@link LotseClient#nodeMetrics}: its metrics, or why there are
 * none.
 *
 * @param nodeId the node
 * @param metrics what it answered, or {@code null}
 * @param error why it did not answer, or {@code null}
 */
public record NodeMetrics(long nodeId, Response.DescribeMetrics metrics, ClientException error) {}
