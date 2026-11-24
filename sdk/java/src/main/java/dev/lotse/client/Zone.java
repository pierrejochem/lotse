package dev.lotse.client;

import dev.lotse.proto.ErrorCode;
import dev.lotse.proto.GroupIndex;
import dev.lotse.proto.LeaderHint;
import dev.lotse.proto.Messages;
import dev.lotse.proto.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Reads in this client's zone (the zone reads design): the indexes of the metadata and coordinator groups this client
 * has seen, which a node in the zone must have applied before it answers a read, and the choice of that node.
 */
final class Zone {
    private Zone() {}

    /**
     * The highest index of each group named in an answer's {@code applied}, shared by a client and its connections.
     * An index only rises: a follower further behind than an answer seen before must not make a later read accept
     * older state. Thread-safe.
     */
    static final class Indexes {
        private final TreeMap<Long, Long> known = new TreeMap<>(Long::compareUnsigned);

        /**
         * Both groups at 0: a read names them even before any answer did, so a node can tell a client that saw
         * nothing from one that sends no tag.
         */
        Indexes() {
            known.put(GroupIndex.METADATA, 0L);
            known.put(GroupIndex.COORDINATOR, 0L);
        }

        /** Takes in what an answer says its node had applied. */
        synchronized void raise(List<GroupIndex> applied) {
            for (GroupIndex e : applied) {
                known.merge(e.group(), e.index(), (a, b) -> Long.compareUnsigned(a, b) >= 0 ? a : b);
            }
        }

        /** What a zone read asks for: every group's index, in group order. */
        synchronized List<GroupIndex> snapshot() {
            List<GroupIndex> out = new ArrayList<>();
            known.forEach((group, index) -> out.add(new GroupIndex(group, index)));
            return out;
        }
    }

    /** The lowest-id node of {@code candidates} in {@code zone}, as {@code zones} has it, or {@code null}. */
    static Long zoneNode(String zone, Iterable<Long> candidates, Map<Long, String> zones) {
        Long best = null;
        for (long n : candidates) {
            if (zone.equals(zones.get(n)) && (best == null || Long.compareUnsigned(n, best) < 0)) {
                best = n;
            }
        }
        return best;
    }

    /**
     * Whether a zone node's answer to a zone read is no answer: an error, of the whole read or of a part. The leader
     * is asked instead, since a zone node refuses what it has not caught up on, and an older node refuses leader-only
     * reads, with the same {@code NOT_LEADER} the leader never gives.
     */
    static boolean refused(Response resp) {
        ErrorCode error;
        if (resp instanceof Response.DescribeConfigs d) {
            return !d.error().isOk() || d.resources().stream().anyMatch(r -> !r.error().isOk());
        } else if (resp instanceof Response.OffsetFetch o) {
            for (Messages.OffsetFetchResource r : o.resources()) {
                for (Messages.OffsetFetchPartition p : r.partitions()) {
                    if (!p.error().isOk()) {
                        return true;
                    }
                }
            }
            return false;
        } else if (resp instanceof Response.DescribeNodes d) {
            error = d.error();
        } else if (resp instanceof Response.DescribeUsers d) {
            error = d.error();
        } else if (resp instanceof Response.DescribeTokens d) {
            error = d.error();
        } else if (resp instanceof Response.DescribeCertificates d) {
            error = d.error();
        } else if (resp instanceof Response.DescribeCertificateRequests d) {
            error = d.error();
        } else if (resp instanceof Response.DescribeUpgrade d) {
            error = d.error();
        } else if (resp instanceof Response.DescribeTenants d) {
            error = d.error();
        } else if (resp instanceof Response.DescribeRoleBindings d) {
            error = d.error();
        } else {
            return true;
        }
        return !error.isOk();
    }

    /** The metadata leader a refused admin read names, or {@code null}. */
    static LeaderHint leaderHint(Response resp) {
        if (resp instanceof Response.DescribeConfigs d) {
            return d.leaderHint();
        } else if (resp instanceof Response.DescribeNodes d) {
            return d.leaderHint();
        } else if (resp instanceof Response.DescribeUsers d) {
            return d.leaderHint();
        } else if (resp instanceof Response.DescribeTokens d) {
            return d.leaderHint();
        } else if (resp instanceof Response.DescribeCertificates d) {
            return d.leaderHint();
        } else if (resp instanceof Response.DescribeCertificateRequests d) {
            return d.leaderHint();
        } else if (resp instanceof Response.DescribeUpgrade d) {
            return d.leaderHint();
        } else if (resp instanceof Response.DescribeTenants d) {
            return d.leaderHint();
        } else if (resp instanceof Response.DescribeRoleBindings d) {
            return d.leaderHint();
        }
        return null;
    }
}
