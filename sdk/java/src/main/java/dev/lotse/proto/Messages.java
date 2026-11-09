package dev.lotse.proto;

import dev.lotse.record.ByteString;
import java.util.List;
import java.util.Objects;

/**
 * The structures that requests and responses share or contain, and the constants their fields take. Each
 * structure has a {@code write} and a {@code read} for its fields in the layout the broker uses. The messages
 * themselves are the nested types of {@link Request} and {@link Response}.
 */
public final class Messages {
    private Messages() {}

    /** The offset of "no committed offset": {@code u64::MAX}. Compare with {@code ==}; as a signed value it is -1. */
    public static final long NO_OFFSET = -1L;
    /** Buckets of the commit latency histogram: bucket {@code i} counts produces acknowledged in under {@code 2^i} ms, the last all slower ones. */
    public static final int LATENCY_BUCKETS = 17;

    /** Tag number of the {@link LeaderHint} in a response. */
    static final long TAG_LEADER_HINT = 0;
    /** Tag number of the reason in a frame-level error. */
    static final long TAG_REASON = 0;

    static <T> List<T> copy(List<T> list) {
        return list == null ? null : List.copyOf(list);
    }

    // ---- constants ----

    /** Kinds of resources in a {@link ResourceMetadata}. */
    public static final class ResourceKind {
        /** A stream with partitions. */
        public static final int STREAM = 0;
        /** A queue: one partition, 0. */
        public static final int QUEUE = 1;
        /** An exchange: routes into queues, has no partitions. */
        public static final int EXCHANGE = 2;

        private ResourceKind() {}
    }

    /** Types of exchanges. */
    public static final class ExchangeType {
        /** The binding key equals the routing key. */
        public static final int DIRECT = 0;
        /** {@code .}-separated words, {@code *} one word, {@code #} zero or more. */
        public static final int TOPIC = 1;
        /** Every bound queue. */
        public static final int FANOUT = 2;
        /** By message headers. */
        public static final int HEADERS = 3;

        private ExchangeType() {}
    }

    /** Isolation levels for a fetch. */
    public static final class Isolation {
        /** Everything up to the high watermark. */
        public static final int UNCOMMITTED = 0;
        /** Up to the last stable offset; aborted transactions are listed. */
        public static final int COMMITTED = 1;

        private Isolation() {}
    }

    /** What a {@code FindCoordinator} key names. */
    public static final class CoordinatorType {
        /** A consumer group. */
        public static final int GROUP = 0;
        /** A producer (idempotent or transactional); the key is the transactional id, or empty. */
        public static final int PRODUCER = 1;

        private CoordinatorType() {}
    }

    /** Where a setting, user, tenant or binding comes from. */
    public static final class ConfigSource {
        /** Not set: the default. */
        public static final int DEFAULT = 0;
        /** Set at runtime. */
        public static final int DYNAMIC = 1;
        /** From the node configuration; cannot be changed at runtime. */
        public static final int STATIC = 2;

        private ConfigSource() {}
    }

    /** Kinds of subjects in a {@link RoleBinding}. */
    public static final class SubjectKind {
        /** A principal by name. */
        public static final int USER = 0;
        /** A group at the identity provider. */
        public static final int GROUP = 1;

        private SubjectKind() {}
    }

    /** Roles in a {@link RoleBinding}. */
    public static final class Role {
        /** Everything, everywhere. */
        public static final int CLUSTER_ADMIN = 0;
        /** Creating, changing, deleting and binding resources in a tenant, and what the roles below may. */
        public static final int TENANT_ADMIN = 1;
        /** Writing to resources. */
        public static final int PRODUCER = 2;
        /** Reading from resources. */
        public static final int CONSUMER = 3;
        /** Seeing resources. */
        public static final int VIEWER = 4;
        /** Seeing everything, everywhere, and nothing else. */
        public static final int CLUSTER_VIEWER = 5;

        private Role() {}
    }

    /** Kinds of principals in a {@link Response.WhoAmI}. */
    public static final class PrincipalKind {
        /** Not authenticated, on a node that does not require it: may do everything. */
        public static final int ANONYMOUS = 0;
        /** A broker node. */
        public static final int NODE = 1;
        /** A user or service. */
        public static final int USER = 2;

        private PrincipalKind() {}
    }

    /** States of a node in a {@link NodeEntry}. */
    public static final class NodeState {
        /** Holds replicas; new ones may be placed on it. */
        public static final int ACTIVE = 0;
        /** Its replicas are moving to other nodes; then it leaves. */
        public static final int DRAINING = 1;

        private NodeState() {}
    }

    // ---- leader hint: a block of tagged fields ----

    /** Writes a leader hint as a block of tagged fields: tag 0 holds it, no hint is an empty block. */
    static void putHint(Writer w, LeaderHint hint) {
        if (hint == null) {
            w.noTags();
            return;
        }
        Writer v = new Writer();
        v.u32(hint.leader());
        v.u64(hint.term());
        w.tags(List.of(new Tag(TAG_LEADER_HINT, ByteString.copyOf(v.toByteArray()))));
    }

    /** Reads a block of tagged fields and picks the leader hint out of it; other tags are skipped. */
    static LeaderHint getHint(Reader r) {
        LeaderHint hint = null;
        for (Tag t : r.tags()) {
            if (t.tag() == TAG_LEADER_HINT) {
                byte[] v = t.value().toByteArray();
                Reader vr = new Reader(v);
                hint = new LeaderHint(vr.u32(), vr.u64());
                vr.finish();
            }
        }
        return hint;
    }

    // ---- tenant of an id: a block of tagged fields ----

    /** The tenant that always exists, and the one a request means when it names none. */
    public static final String DEFAULT_TENANT = "default";

    /** Tag of the tenant in a request that names a transactional or group id (ADR 0062). */
    static final long TAG_TENANT = 1;

    /** Tag of a group member's instance id (ADR 0072). */
    static final long TAG_INSTANCE = 2;

    /** Tag of a group member's subscription pattern (ADR 0073). */
    static final long TAG_PATTERN = 3;

    /** Tag of a subscription that asks to hear it became the single active consumer (ADR 0076). */
    static final long TAG_NOTIFY_ACTIVE = 1;

    /** Tag of a subscription that waits as a standby, next to the leader hint (ADR 0076). */
    static final long TAG_STANDBY = 1;

    /** Tag of a transactional ack's producer id and epoch (ADR 0075). */
    static final long TAG_PRODUCER = 2;

    /** A tag holding one string. */
    static Tag stringTag(long tag, String value) {
        Writer v = new Writer();
        v.string(value);
        return new Tag(tag, ByteString.copyOf(v.toByteArray()));
    }

    static String tenantOrDefault(String tenant) {
        return tenant == null ? DEFAULT_TENANT : tenant;
    }

    /** Writes a request's tags: none in the default tenant, otherwise tag 1 holding the tenant. */
    static void putTenant(Writer w, String tenant) {
        if (DEFAULT_TENANT.equals(tenant)) {
            w.noTags();
            return;
        }
        Writer v = new Writer();
        v.string(tenant);
        w.tags(List.of(new Tag(TAG_TENANT, ByteString.copyOf(v.toByteArray()))));
    }

    /** Reads a request's tags and picks the tenant out of them; other tags are skipped. */
    static String getTenant(Reader r) {
        String tenant = DEFAULT_TENANT;
        for (Tag t : r.tags()) {
            if (t.tag() == TAG_TENANT) {
                Reader vr = new Reader(t.value().toByteArray());
                tenant = vr.string();
                vr.finish();
            }
        }
        return tenant;
    }

    // ---- small shared structures ----

    /**
     * A pair of strings, for settings.
     *
     * @param key the setting
     * @param value its value
     */
    public record Config(String key, String value) {
        /** Creates a pair. */
        public Config {
            Objects.requireNonNull(key);
            Objects.requireNonNull(value);
        }

        static void write(Writer w, Config c) {
            w.string(c.key).string(c.value);
        }

        static Config read(Reader r) {
            return new Config(r.string(), r.string());
        }
    }

    static void putConfigs(Writer w, List<Config> configs) {
        w.array(configs, Config::write);
    }

    static List<Config> getConfigs(Reader r) {
        return r.array(4, Config::read);
    }

    /**
     * An offset range {@code [from, to)}, both {@code u64}.
     *
     * @param from first offset
     * @param to offset after the last
     */
    public record Range(long from, long to) {
        static void write(Writer w, Range x) {
            w.u64(x.from).u64(x.to);
        }

        static Range read(Reader r) {
            return new Range(r.u64(), r.u64());
        }
    }

    static void putRanges(Writer w, List<Range> ranges) {
        w.array(ranges, Range::write);
    }

    static List<Range> getRanges(Reader r) {
        return r.array(16, Range::read);
    }

    /**
     * A match on one header of a binding.
     *
     * @param key the header's name
     * @param value the value to match, or {@code null} to match a header whose value is null
     */
    public record HeaderMatch(ByteString key, ByteString value) {
        /** Creates a match. */
        public HeaderMatch {
            Objects.requireNonNull(key);
        }

        static void write(Writer w, HeaderMatch h) {
            w.bytes(h.key);
            w.bool(h.value != null);
            if (h.value != null) {
                w.bytes(h.value);
            }
        }

        static HeaderMatch read(Reader r) {
            ByteString k = r.bytes();
            return new HeaderMatch(k, r.bool() ? r.bytes() : null);
        }
    }

    // ---- partitions in requests and responses ----

    /**
     * Partitions of one resource.
     *
     * @param resourceId the resource, a {@code u64}
     * @param partitions partition numbers, each a {@code u32}
     */
    public record TxnResource(long resourceId, List<Long> partitions) {
        /** Creates the structure. */
        public TxnResource {
            partitions = List.copyOf(partitions);
        }

        static void write(Writer w, TxnResource t) {
            w.u64(t.resourceId);
            w.array(t.partitions, Writer::u32);
            w.noTags();
        }

        static TxnResource read(Reader r) {
            TxnResource t = new TxnResource(r.u64(), r.array(4, Reader::u32));
            r.tags();
            return t;
        }
    }

    static void putTxnResources(Writer w, List<TxnResource> resources) {
        w.array(resources, TxnResource::write);
    }

    static List<TxnResource> getTxnResources(Reader r) {
        return r.array(10, TxnResource::read);
    }

    static void putOptResources(Writer w, List<TxnResource> resources) {
        w.bool(resources != null);
        if (resources != null) {
            putTxnResources(w, resources);
        }
    }

    static List<TxnResource> getOptResources(Reader r) {
        return r.bool() ? getTxnResources(r) : null;
    }

    /**
     * The outcome for one partition of a transaction or commit request.
     *
     * @param partition partition number, a {@code u32}
     * @param error the outcome
     */
    public record TxnPartitionResult(long partition, ErrorCode error) {
        /** Creates the structure. */
        public TxnPartitionResult {
            Objects.requireNonNull(error);
        }
    }

    /**
     * Outcomes for one resource.
     *
     * @param resourceId the resource, a {@code u64}
     * @param partitions per partition
     */
    public record TxnResourceResult(long resourceId, List<TxnPartitionResult> partitions) {
        /** Creates the structure. */
        public TxnResourceResult {
            partitions = List.copyOf(partitions);
        }

        static void write(Writer w, TxnResourceResult t) {
            w.u64(t.resourceId);
            w.array(t.partitions, (w2, p) -> {
                w2.u32(p.partition());
                w2.u16(p.error().code());
                w2.noTags();
            });
            w.noTags();
        }

        static TxnResourceResult read(Reader r) {
            long id = r.u64();
            List<TxnPartitionResult> partitions = r.array(7, r2 -> {
                TxnPartitionResult p = new TxnPartitionResult(r2.u32(), new ErrorCode(r2.u16()));
                r2.tags();
                return p;
            });
            r.tags();
            return new TxnResourceResult(id, partitions);
        }
    }

    static void putTxnResults(Writer w, List<TxnResourceResult> results) {
        w.array(results, TxnResourceResult::write);
    }

    static List<TxnResourceResult> getTxnResults(Reader r) {
        return r.array(10, TxnResourceResult::read);
    }

    // ---- metadata ----

    /**
     * A broker node.
     *
     * @param nodeId node id, a {@code u32}
     * @param host host clients connect to
     * @param port port clients connect to, a {@code u16}
     * @param rack rack, or empty
     */
    public record Broker(long nodeId, String host, int port, String rack) {}

    /**
     * One partition of a resource.
     *
     * @param partition partition number, a {@code u32}
     * @param leader leader node, a {@code u32}; {@code 0} if none is known
     * @param leaderTerm the leader's term, a {@code u64}
     * @param voters voters of the partition's Raft group, each a {@code u32}
     */
    public record PartitionMetadata(long partition, long leader, long leaderTerm, List<Long> voters) {
        /** Creates the structure. */
        public PartitionMetadata {
            voters = List.copyOf(voters);
        }
    }

    /**
     * A queue bound to an exchange.
     *
     * @param queueId the queue, a {@code u64}
     * @param bindingKey matched against the routing key (direct, topic)
     * @param headerMatch headers to match (headers exchange)
     * @param matchAll all headers must match ({@code true}) or any one
     */
    public record BindingMetadata(long queueId, String bindingKey, List<HeaderMatch> headerMatch, boolean matchAll) {
        /** Creates the structure. */
        public BindingMetadata {
            headerMatch = List.copyOf(headerMatch);
        }
    }

    /**
     * An exchange's type and bindings: clients route with them.
     *
     * @param exchangeType see {@link ExchangeType}
     * @param bindings the bound queues
     */
    public record ExchangeMetadata(int exchangeType, List<BindingMetadata> bindings) {
        /** Creates the structure. */
        public ExchangeMetadata {
            bindings = List.copyOf(bindings);
        }
    }

    /**
     * A resource.
     *
     * @param error {@link ErrorCode#UNKNOWN_RESOURCE} for a name that does not exist
     * @param name the name
     * @param tenant the tenant it belongs to; empty for a name that does not exist
     * @param resourceId the id used by data-path requests, a {@code u64}
     * @param kind see {@link ResourceKind}
     * @param partitions the partitions
     * @param exchange for an exchange: its type and bindings, otherwise {@code null}
     */
    public record ResourceMetadata(
            ErrorCode error,
            String name,
            String tenant,
            long resourceId,
            int kind,
            List<PartitionMetadata> partitions,
            ExchangeMetadata exchange) {
        /** Creates the structure. */
        public ResourceMetadata {
            Objects.requireNonNull(error);
            partitions = List.copyOf(partitions);
        }
    }

    // ---- produce and fetch ----

    /**
     * A batch for one partition.
     *
     * @param partition partition number, a {@code u32}
     * @param batch one record batch, as built by the producer
     */
    public record ProducePartition(long partition, ByteString batch) {}

    /**
     * Batches for one resource.
     *
     * @param resourceId the resource, a {@code u64}
     * @param partitions its partitions
     */
    public record ProduceResource(long resourceId, List<ProducePartition> partitions) {
        /** Creates the structure. */
        public ProduceResource {
            partitions = List.copyOf(partitions);
        }
    }

    /**
     * The outcome for one partition of a produce.
     *
     * @param partition partition number, a {@code u32}
     * @param error the outcome
     * @param baseOffset offset of the batch's first record, a {@code u64}
     * @param appendTs append time in milliseconds since the epoch, or {@code -1}
     * @param errorMsg details for humans, or {@code null}
     * @param leaderHint the leader, with {@link ErrorCode#NOT_LEADER}, or {@code null}
     */
    public record ProducePartitionResponse(
            long partition, ErrorCode error, long baseOffset, long appendTs, String errorMsg, LeaderHint leaderHint) {}

    /**
     * Outcomes for one resource.
     *
     * @param resourceId the resource, a {@code u64}
     * @param partitions its partitions
     */
    public record ProduceResourceResponse(long resourceId, List<ProducePartitionResponse> partitions) {
        /** Creates the structure. */
        public ProduceResourceResponse {
            partitions = List.copyOf(partitions);
        }
    }

    /**
     * Where to read in one partition.
     *
     * @param partition partition number, a {@code u32}
     * @param fetchOffset first offset wanted, a {@code u64}
     * @param leaderTerm the leader term the client knows, a {@code u64}; {@code 0} not to check
     * @param maxBytes byte limit for this partition (at least one batch is returned), a {@code u32}
     */
    public record FetchPartition(long partition, long fetchOffset, long leaderTerm, long maxBytes) {}

    /**
     * Partitions to read of one resource.
     *
     * @param resourceId the resource, a {@code u64}
     * @param partitions its partitions
     */
    public record FetchResource(long resourceId, List<FetchPartition> partitions) {
        /** Creates the structure. */
        public FetchResource {
            partitions = List.copyOf(partitions);
        }
    }

    /**
     * A transaction aborted inside the returned range.
     *
     * @param producerId its producer, a {@code u64}
     * @param firstOffset its first offset, a {@code u64}
     */
    public record AbortedTxn(long producerId, long firstOffset) {}

    /**
     * One partition to look up with {@code ListOffsets}.
     *
     * @param partition partition number, a {@code u32}
     * @param timestamp milliseconds since the epoch, or {@link ListOffsets#LATEST} or {@link ListOffsets#EARLIEST}
     */
    public record ListOffsetsPartition(long partition, long timestamp) {}

    /**
     * The partitions of one stream to look up.
     *
     * @param resourceId the stream, a {@code u64}
     * @param partitions its partitions
     */
    public record ListOffsetsResource(long resourceId, List<ListOffsetsPartition> partitions) {
        /** Creates the structure. */
        public ListOffsetsResource {
            partitions = List.copyOf(partitions);
        }
    }

    /**
     * The answer for one partition.
     *
     * @param partition partition number, a {@code u32}
     * @param error outcome
     * @param offset for a time, the first offset of the first data batch with a record at or after it, or the latest
     *     offset if there is none; a {@code u64}
     * @param timestamp for a time lookup that found a batch, its newest record's timestamp; -1 otherwise
     * @param leaderHint the leader, with {@link ErrorCode#NOT_LEADER}; {@code null} if none
     */
    public record ListOffsetsPartitionResponse(
            long partition, ErrorCode error, long offset, long timestamp, LeaderHint leaderHint) {}

    /**
     * The answers for one stream.
     *
     * @param resourceId the stream, a {@code u64}
     * @param partitions its partitions
     */
    public record ListOffsetsResourceResponse(long resourceId, List<ListOffsetsPartitionResponse> partitions) {
        /** Creates the structure. */
        public ListOffsetsResourceResponse {
            partitions = List.copyOf(partitions);
        }
    }

    /** What {@link ListOffsetsPartition#timestamp} may ask for besides a time. */
    public static final class ListOffsets {
        private ListOffsets() {}

        /** The offset after the last one a reader may see: the high watermark, or the last stable offset. */
        public static final long LATEST = -1;
        /** The first offset still in the log. */
        public static final long EARLIEST = -2;
    }

    /**
     * What was read from one partition.
     *
     * @param partition partition number, a {@code u32}
     * @param error the outcome
     * @param highWatermark offsets below this are committed, a {@code u64}
     * @param lastStableOffset offsets below this are committed and not in an open transaction, a {@code u64}
     * @param logStartOffset first offset still in the log, a {@code u64}
     * @param abortedTxns aborted transactions in {@code records}, for read-committed
     * @param records whole record batches as stored, control batches included
     * @param leaderHint the leader, with {@link ErrorCode#NOT_LEADER}, or {@code null}
     */
    public record FetchPartitionResponse(
            long partition,
            ErrorCode error,
            long highWatermark,
            long lastStableOffset,
            long logStartOffset,
            List<AbortedTxn> abortedTxns,
            ByteString records,
            LeaderHint leaderHint) {
        /** Creates the structure. */
        public FetchPartitionResponse {
            abortedTxns = List.copyOf(abortedTxns);
        }
    }

    /**
     * What was read from one resource.
     *
     * @param resourceId the resource, a {@code u64}
     * @param partitions its partitions
     */
    public record FetchResourceResponse(long resourceId, List<FetchPartitionResponse> partitions) {
        /** Creates the structure. */
        public FetchResourceResponse {
            partitions = List.copyOf(partitions);
        }
    }

    // ---- queues ----

    /**
     * One message of a delivered batch, for this subscription.
     *
     * @param offset the message's offset, a {@code u64}
     * @param deliveryCount how often it has been delivered, this time included, a {@code u16}
     */
    public record DeliveredOffset(long offset, int deliveryCount) {}

    // ---- publishing (ADR 0079) ----

    /**
     * One message to publish.
     *
     * @param exchangeId the exchange, a {@code u64}
     * @param routingKey the routing key
     * @param mandatory refuse the message with {@code UNROUTABLE} if it routes to no queue, rather than drop it
     * @param batch a batch of exactly one record, the message; its headers route it through a headers exchange
     */
    public record PublishEntry(long exchangeId, String routingKey, boolean mandatory, ByteString batch) {}

    /**
     * What became of one published message.
     *
     * @param error {@code NONE} once routed (each queue's outcome is in {@code routed}); otherwise why nothing was
     *     written
     * @param routed the queues it routes to, by id
     */
    public record PublishResult(ErrorCode error, List<RoutedMessage> routed) {
        /** Creates the result. */
        public PublishResult {
            routed = List.copyOf(routed);
        }
    }

    /**
     * One queue's copy of a published message.
     *
     * @param queueId the queue, a {@code u64}
     * @param name the queue's name; empty when the node no longer knows the queue
     * @param error {@code NONE}: the queue holds it at {@code baseOffset}; {@code REQUEST_TIMED_OUT}: unknown;
     *     anything else: it does not
     * @param baseOffset where, with {@code NONE}, a {@code u64}
     */
    public record RoutedMessage(long queueId, String name, ErrorCode error, long baseOffset) {}

    // ---- consumer groups ----

    /**
     * An offset to commit for one partition.
     *
     * @param partition partition number, a {@code u32}
     * @param offset the next offset to read, a {@code u64}
     * @param leaderTerm the leader term the offset was read under, a {@code u64}, or 0
     * @param metadata free text kept with the offset, or {@code null}
     */
    public record OffsetCommitPartition(long partition, long offset, long leaderTerm, String metadata) {}

    /**
     * Offsets to commit for one resource.
     *
     * @param resourceId the resource, a {@code u64}
     * @param partitions its partitions
     */
    public record OffsetCommitResource(long resourceId, List<OffsetCommitPartition> partitions) {
        /** Creates the structure. */
        public OffsetCommitResource {
            partitions = List.copyOf(partitions);
        }
    }

    static void putCommits(Writer w, List<OffsetCommitResource> resources) {
        w.array(resources, (w1, r) -> {
            w1.u64(r.resourceId());
            w1.array(r.partitions(), (w2, p) -> {
                w2.u32(p.partition());
                w2.u64(p.offset());
                w2.u64(p.leaderTerm());
                w2.optString(p.metadata());
                w2.noTags();
            });
            w1.noTags();
        });
    }

    static List<OffsetCommitResource> getCommits(Reader r) {
        return r.array(10, r1 -> {
            long id = r1.u64();
            List<OffsetCommitPartition> partitions = r1.array(23, r2 -> {
                OffsetCommitPartition p = new OffsetCommitPartition(r2.u32(), r2.u64(), r2.u64(), r2.optString());
                r2.tags();
                return p;
            });
            r1.tags();
            return new OffsetCommitResource(id, partitions);
        });
    }

    /**
     * One partition's committed offset.
     *
     * @param partition partition number, a {@code u32}
     * @param error the outcome
     * @param offset the next offset to read, a {@code u64}, or {@link #NO_OFFSET}
     * @param leaderTerm the leader term it was committed with, a {@code u64}
     * @param metadata what was kept with it, or {@code null}
     */
    public record OffsetFetchPartition(long partition, ErrorCode error, long offset, long leaderTerm, String metadata) {}

    /**
     * One resource's committed offsets.
     *
     * @param resourceId the resource, a {@code u64}
     * @param partitions its partitions
     */
    public record OffsetFetchResource(long resourceId, List<OffsetFetchPartition> partitions) {
        /** Creates the structure. */
        public OffsetFetchResource {
            partitions = List.copyOf(partitions);
        }
    }

    // ---- configs ----

    /**
     * One setting of a resource.
     *
     * @param key its key
     * @param value its value
     * @param source see {@link ConfigSource}
     */
    public record ConfigEntry(String key, String value, int source) {}

    /**
     * One resource's settings, or why not.
     *
     * @param resourceId the resource, a {@code u64}
     * @param error {@link ErrorCode#UNKNOWN_RESOURCE} if there is no such resource
     * @param configs its settings
     */
    public record ResourceConfigs(long resourceId, ErrorCode error, List<ConfigEntry> configs) {
        /** Creates the structure. */
        public ResourceConfigs {
            configs = List.copyOf(configs);
        }
    }

    // ---- metrics ----

    /**
     * The machine a node runs on; a {@code null} field is unknown.
     *
     * @param cpuPermille CPU use, per mille, a {@code u16}
     * @param memoryBytes memory the node uses, a {@code u64}
     * @param memoryLimitBytes memory available to it, a {@code u64}
     * @param diskFreeBytes free bytes on the data volume, a {@code u64}
     * @param diskTotalBytes size of the data volume, a {@code u64}
     */
    public record HostMetrics(
            Integer cpuPermille, Long memoryBytes, Long memoryLimitBytes, Long diskFreeBytes, Long diskTotalBytes) {}

    /**
     * How far a replica is.
     *
     * @param nodeId the replica's node, a {@code u32}
     * @param logEnd offset after the last one it holds, a {@code u64}
     */
    public record ReplicaProgress(long nodeId, long logEnd) {}

    /**
     * One Raft group hosted on the node. All counters are {@code u64}.
     *
     * @param group the group
     * @param resourceId the stream or queue it belongs to; 0 for the node's own groups
     * @param partition partition (0 for a queue), a {@code u32}
     * @param leader whether this node leads the group
     * @param term Raft term here
     * @param highWatermark committed offsets end here
     * @param lastStable below the first open transaction
     * @param logStart first offset still stored
     * @param logEnd offset after the last one in the log, committed or not
     * @param sizeBytes bytes of the log on this node
     * @param produceBytes acknowledged produce bytes, while this node led the group
     * @param produceRecords acknowledged produce records, likewise
     * @param fetchBytes bytes fetched, or for a queue delivered, likewise
     * @param replicas on the leader: the last offset each replica holds (plus one)
     */
    public record PartitionMetrics(
            long group,
            long resourceId,
            long partition,
            boolean leader,
            long term,
            long highWatermark,
            long lastStable,
            long logStart,
            long logEnd,
            long sizeBytes,
            long produceBytes,
            long produceRecords,
            long fetchBytes,
            List<ReplicaProgress> replicas) {
        /** Creates the structure. */
        public PartitionMetrics {
            replicas = List.copyOf(replicas);
        }
    }

    /**
     * A queue subscription.
     *
     * @param subscriptionId the subscription, a {@code u32}
     * @param peer the client's address
     * @param inFlight messages delivered to it and not acknowledged, a {@code u32}
     * @param credit messages it may still be sent, a {@code u32}
     */
    public record ConsumerMetrics(long subscriptionId, String peer, long inFlight, long credit) {}

    /**
     * A queue the node leads. All counters are {@code u64}.
     *
     * @param resourceId the queue
     * @param ready messages ready for delivery
     * @param inFlight delivered and not yet acknowledged
     * @param inOpenTxns waiting for their transaction to end
     * @param delivered messages delivered, while this node led the queue
     * @param acked messages acknowledged, likewise
     * @param consumers subscriptions
     */
    public record QueueMetrics(
            long resourceId,
            long ready,
            long inFlight,
            long inOpenTxns,
            long delivered,
            long acked,
            List<ConsumerMetrics> consumers) {
        /** Creates the structure. */
        public QueueMetrics {
            consumers = List.copyOf(consumers);
        }
    }

    /**
     * A member of a consumer group.
     *
     * @param memberId the member
     * @param owned partitions it owns
     */
    public record GroupMemberMetrics(String memberId, List<TxnResource> owned) {
        /** Creates the structure. */
        public GroupMemberMetrics {
            owned = List.copyOf(owned);
        }
    }

    /**
     * A group's committed offset for one partition.
     *
     * @param resourceId the stream, a {@code u64}
     * @param partition the partition, a {@code u32}
     * @param offset the next offset to read, a {@code u64}
     */
    public record CommittedOffset(long resourceId, long partition, long offset) {}

    /**
     * A consumer group, from the coordinator's leader.
     *
     * @param groupId the group
     * @param tenant the tenant its id lives in (ADR 0062)
     * @param epoch assignment epoch, an {@code i32}
     * @param members members and what they own
     * @param offsets committed offsets
     */
    public record GroupMetrics(
            String groupId, String tenant, int epoch, List<GroupMemberMetrics> members, List<CommittedOffset> offsets) {
        /** Creates the structure. */
        public GroupMetrics {
            members = List.copyOf(members);
            offsets = List.copyOf(offsets);
        }
    }

    // ---- authentication, users, tenants, roles ----

    /**
     * A user's SCRAM-SHA-512 credential as the broker keeps it: never the password.
     *
     * @param iterations PBKDF2 iterations, a {@code u32}, at least 4096
     * @param salt the salt
     * @param storedKey {@code H(ClientKey)}
     * @param serverKey {@code HMAC(SaltedPassword, "Server Key")}
     */
    public record ScramCredentialData(long iterations, ByteString salt, ByteString storedKey, ByteString serverKey) {}

    /**
     * One user with a credential.
     *
     * @param name the user name
     * @param mechanism {@code SCRAM-SHA-512}
     * @param iterations the credential's iterations, a {@code u32}
     * @param source see {@link ConfigSource}
     */
    public record UserEntry(String name, String mechanism, long iterations, int source) {}

    /**
     * One tenant.
     *
     * @param name the name
     * @param source see {@link ConfigSource}
     * @param resources how many resources it holds, a {@code u32}
     */
    public record TenantEntry(String name, int source, long resources) {}

    /**
     * A role granted to a user or group, in a tenant, for the resources whose names start with {@code prefix}.
     *
     * @param subjectKind see {@link SubjectKind}
     * @param subject the user or group
     * @param role see {@link Role}
     * @param tenant the tenant; empty for the two cluster-wide roles
     * @param prefix only resources whose names start with this; empty for all
     */
    public record RoleBinding(int subjectKind, String subject, int role, String tenant, String prefix) {
        static void write(Writer w, RoleBinding b) {
            w.u8(b.subjectKind).string(b.subject).u8(b.role).string(b.tenant).string(b.prefix);
        }

        static RoleBinding read(Reader r) {
            return new RoleBinding(r.u8(), r.string(), r.u8(), r.string(), r.string());
        }
    }

    /** The smallest {@link RoleBinding}: two flags and three empty strings. */
    static final int BINDING_MIN = 1 + 2 + 1 + 2 + 2;

    /**
     * A role binding and where it comes from.
     *
     * @param binding the binding
     * @param source see {@link ConfigSource}
     */
    public record SourcedBinding(RoleBinding binding, int source) {}

    /**
     * One node of the cluster.
     *
     * @param nodeId its id, a {@code u64}
     * @param peer where the other nodes reach it
     * @param client where clients reach it
     * @param state see {@link NodeState}
     * @param replicas groups it is to be a voter of, a {@code u32}
     * @param moving of those, groups whose voters are not yet in place, a {@code u32}
     */
    public record NodeEntry(long nodeId, String peer, String client, int state, long replicas, long moving) {}
}
