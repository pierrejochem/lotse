package dev.lotse.proto;

/**
 * A position in one Raft group's log: the group and the index applied or wanted there. Frame headers carry lists of
 * them as tag 0: {@code read_at_least} in a request, {@code applied} in a response.
 *
 * @param group the group, a {@code u64}: 1 for the metadata group, 2 for the coordinator group
 * @param index a log index in it, a {@code u64}
 */
public record GroupIndex(long group, long index) {
    /** The metadata group's id. */
    public static final long METADATA = 1;

    /** The coordinator group's id. */
    public static final long COORDINATOR = 2;
}
