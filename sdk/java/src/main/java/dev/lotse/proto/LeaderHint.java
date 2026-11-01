package dev.lotse.proto;

/**
 * Where a partition's leader is, sent with {@link ErrorCode#NOT_LEADER}. On the wire it is tagged field 0 of the
 * structure it belongs to: {@code leader u32 | term u64}.
 *
 * @param leader the leader's node id, a {@code u32}
 * @param term its term, a {@code u64}
 */
public record LeaderHint(long leader, long term) {}
