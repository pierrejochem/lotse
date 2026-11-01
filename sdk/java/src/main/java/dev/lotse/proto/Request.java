package dev.lotse.proto;

import dev.lotse.proto.Messages.*;
import dev.lotse.record.ByteString;
import java.util.List;
import java.util.Objects;

/**
 * A request, version 0. Each nested type is one API key; {@link #write} writes the body that follows the
 * request header, {@code read} on the nested type reads it. Use {@link Frames} for whole frames.
 *
 * <p>The node-to-node keys ({@code SettleVoters}, {@code DeadLetter}, {@code WriteTxnMarkers}) have no type here.
 */
public sealed interface Request {
    /** The key of this request. */
    ApiKey apiKey();

    /** Writes the request body, tagged fields included. */
    void write(Writer w);

    /** Asks for the versions and features of the node. The body is an empty block of tagged fields. */
    record ApiVersions() implements Request {
        @Override
        public ApiKey apiKey() {
            return ApiKey.API_VERSIONS;
        }

        @Override
        public void write(Writer w) {
            w.noTags();
        }

        static ApiVersions read(Reader r) {
            r.tags();
            return new ApiVersions();
        }
    }

    /**
     * Asks for nodes and resources.
     *
     * @param names resource names; empty asks for all
     */
    record Metadata(List<String> names) implements Request {
        /** Creates the request. */
        public Metadata {
            names = List.copyOf(names);
        }

        @Override
        public ApiKey apiKey() {
            return ApiKey.METADATA;
        }

        @Override
        public void write(Writer w) {
            w.array(names, Writer::string);
            w.noTags();
        }

        static Metadata read(Reader r) {
            Metadata m = new Metadata(r.array(2, Reader::string));
            r.tags();
            return m;
        }
    }

    /**
     * Asks which node coordinates a group or producer.
     *
     * @param keyType see {@link CoordinatorType}
     * @param key group name or transactional id
     */
    record FindCoordinator(int keyType, String key) implements Request {
        @Override
        public ApiKey apiKey() {
            return ApiKey.FIND_COORDINATOR;
        }

        @Override
        public void write(Writer w) {
            w.u8(keyType).string(key).noTags();
        }

        static FindCoordinator read(Reader r) {
            FindCoordinator x = new FindCoordinator(r.u8(), r.string());
            r.tags();
            return x;
        }
    }

    /**
     * Appends batches to stream partitions.
     *
     * @param txnId the transaction, if the batches belong to one, otherwise {@code null}
     * @param timeoutMs how long to wait for the quorum, a {@code u32}
     * @param resources the batches
     */
    record Produce(String txnId, long timeoutMs, List<ProduceResource> resources) implements Request {
        /** Creates the request. */
        public Produce {
            resources = List.copyOf(resources);
        }

        @Override
        public ApiKey apiKey() {
            return ApiKey.PRODUCE;
        }

        @Override
        public void write(Writer w) {
            w.optString(txnId).u32(timeoutMs);
            w.array(resources, (w1, res) -> {
                w1.u64(res.resourceId());
                w1.array(res.partitions(), (w2, p) -> {
                    w2.u32(p.partition());
                    w2.bytes(p.batch());
                    w2.noTags();
                });
                w1.noTags();
            });
            w.noTags();
        }

        static Produce read(Reader r) {
            String txnId = r.optString();
            long timeoutMs = r.u32();
            List<ProduceResource> resources = r.array(10, r1 -> {
                long id = r1.u64();
                List<ProducePartition> partitions = r1.array(9, r2 -> {
                    ProducePartition p = new ProducePartition(r2.u32(), r2.bytes());
                    r2.tags();
                    return p;
                });
                r1.tags();
                return new ProduceResource(id, partitions);
            });
            r.tags();
            return new Produce(txnId, timeoutMs, resources);
        }
    }

    /**
     * Reads batches from stream partitions, waiting up to {@code maxWaitMs} for at least {@code minBytes}.
     *
     * @param maxWaitMs longest wait for {@code minBytes}, a {@code u32}
     * @param minBytes bytes to wait for, a {@code u32}
     * @param maxBytes byte limit for the whole response, a {@code u32}
     * @param isolation see {@link Isolation}
     * @param resources what to read
     */
    record Fetch(long maxWaitMs, long minBytes, long maxBytes, int isolation, List<FetchResource> resources)
            implements Request {
        /** Creates the request. */
        public Fetch {
            resources = List.copyOf(resources);
        }

        @Override
        public ApiKey apiKey() {
            return ApiKey.FETCH;
        }

        @Override
        public void write(Writer w) {
            w.u32(maxWaitMs).u32(minBytes).u32(maxBytes).u8(isolation);
            w.array(resources, (w1, res) -> {
                w1.u64(res.resourceId());
                w1.array(res.partitions(), (w2, p) -> {
                    w2.u32(p.partition());
                    w2.u64(p.fetchOffset());
                    w2.u64(p.leaderTerm());
                    w2.u32(p.maxBytes());
                    w2.noTags();
                });
                w1.noTags();
            });
            w.noTags();
        }

        static Fetch read(Reader r) {
            long maxWait = r.u32();
            long minBytes = r.u32();
            long maxBytes = r.u32();
            int isolation = r.u8();
            List<FetchResource> resources = r.array(10, r1 -> {
                long id = r1.u64();
                List<FetchPartition> partitions = r1.array(25, r2 -> {
                    FetchPartition p = new FetchPartition(r2.u32(), r2.u64(), r2.u64(), r2.u32());
                    r2.tags();
                    return p;
                });
                r1.tags();
                return new FetchResource(id, partitions);
            });
            r.tags();
            return new Fetch(maxWait, minBytes, maxBytes, isolation, resources);
        }
    }

    /**
     * Asks the coordinator for a producer id and epoch.
     *
     * @param txnId the transactional id; {@code null} for an idempotent producer
     * @param txnTimeoutMs transaction timeout, a {@code u32}
     */
    record InitProducer(String txnId, long txnTimeoutMs) implements Request {
        @Override
        public ApiKey apiKey() {
            return ApiKey.INIT_PRODUCER;
        }

        @Override
        public void write(Writer w) {
            w.optString(txnId).u32(txnTimeoutMs).noTags();
        }

        static InitProducer read(Reader r) {
            InitProducer x = new InitProducer(r.optString(), r.u32());
            r.tags();
            return x;
        }
    }

    /**
     * Adds partitions to the producer's transaction.
     *
     * @param txnId transactional id
     * @param producerId producer id from {@code InitProducer}, a {@code u64}
     * @param producerEpoch its epoch, a {@code u16}
     * @param resources the partitions
     */
    record AddPartitionsToTxn(String txnId, long producerId, int producerEpoch, List<TxnResource> resources)
            implements Request {
        /** Creates the request. */
        public AddPartitionsToTxn {
            resources = List.copyOf(resources);
        }

        @Override
        public ApiKey apiKey() {
            return ApiKey.ADD_PARTITIONS_TO_TXN;
        }

        @Override
        public void write(Writer w) {
            w.string(txnId).u64(producerId).u16(producerEpoch);
            Messages.putTxnResources(w, resources);
            w.noTags();
        }

        static AddPartitionsToTxn read(Reader r) {
            AddPartitionsToTxn x =
                    new AddPartitionsToTxn(r.string(), r.u64(), r.u16(), Messages.getTxnResources(r));
            r.tags();
            return x;
        }
    }

    /**
     * Commits or aborts the producer's transaction.
     *
     * @param txnId transactional id
     * @param producerId producer id, a {@code u64}
     * @param producerEpoch its epoch, a {@code u16}
     * @param commit commit ({@code true}) or abort
     */
    record EndTxn(String txnId, long producerId, int producerEpoch, boolean commit) implements Request {
        @Override
        public ApiKey apiKey() {
            return ApiKey.END_TXN;
        }

        @Override
        public void write(Writer w) {
            w.string(txnId).u64(producerId).u16(producerEpoch).bool(commit).noTags();
        }

        static EndTxn read(Reader r) {
            EndTxn x = new EndTxn(r.string(), r.u64(), r.u16(), r.bool());
            r.tags();
            return x;
        }
    }

    /**
     * Subscribes to a queue; deliveries follow as pushes on this connection.
     *
     * @param queueId the queue, a {@code u64}
     * @param consumerTag the consumer's name for itself
     * @param creditMsgs initial credit in messages, a {@code u32}
     * @param creditBytes initial credit in bytes, a {@code u32}
     * @param exclusive single active consumer (not yet supported: refused)
     */
    record Subscribe(long queueId, String consumerTag, long creditMsgs, long creditBytes, boolean exclusive)
            implements Request {
        @Override
        public ApiKey apiKey() {
            return ApiKey.SUBSCRIBE;
        }

        @Override
        public void write(Writer w) {
            w.u64(queueId).string(consumerTag).u32(creditMsgs).u32(creditBytes).bool(exclusive).noTags();
        }

        static Subscribe read(Reader r) {
            Subscribe x = new Subscribe(r.u64(), r.string(), r.u32(), r.u32(), r.bool());
            r.tags();
            return x;
        }
    }

    /**
     * Adds credit to a subscription. Never answered.
     *
     * @param subscriptionId the subscription, a {@code u32}
     * @param addMsgs messages to add, a {@code u32}
     * @param addBytes bytes to add, a {@code u32}
     */
    record Credit(long subscriptionId, long addMsgs, long addBytes) implements Request {
        @Override
        public ApiKey apiKey() {
            return ApiKey.CREDIT;
        }

        @Override
        public void write(Writer w) {
            w.u32(subscriptionId).u32(addMsgs).u32(addBytes).noTags();
        }

        static Credit read(Reader r) {
            Credit x = new Credit(r.u32(), r.u32(), r.u32());
            r.tags();
            return x;
        }
    }

    /**
     * Acknowledges messages {@code [from, to)} of a queue.
     *
     * @param queueId the queue, a {@code u64}
     * @param txnId a transaction, for an ack that takes effect with its commit (not yet supported), or {@code null}
     * @param ranges offset ranges {@code [from, to)}
     */
    record Ack(long queueId, String txnId, List<Range> ranges) implements Request {
        /** Creates the request. */
        public Ack {
            ranges = List.copyOf(ranges);
        }

        @Override
        public ApiKey apiKey() {
            return ApiKey.ACK;
        }

        @Override
        public void write(Writer w) {
            w.u64(queueId).optString(txnId);
            Messages.putRanges(w, ranges);
            w.noTags();
        }

        static Ack read(Reader r) {
            Ack x = new Ack(r.u64(), r.optString(), Messages.getRanges(r));
            r.tags();
            return x;
        }
    }

    /**
     * Rejects messages {@code [from, to)}: back to the queue, or discarded.
     *
     * @param queueId the queue, a {@code u64}
     * @param ranges offset ranges {@code [from, to)}
     * @param requeue deliver again ({@code true}) or discard
     * @param delayMs wait before delivering again (not yet supported: must be 0), a {@code u32}
     */
    record Nack(long queueId, List<Range> ranges, boolean requeue, long delayMs) implements Request {
        /** Creates the request. */
        public Nack {
            ranges = List.copyOf(ranges);
        }

        @Override
        public ApiKey apiKey() {
            return ApiKey.NACK;
        }

        @Override
        public void write(Writer w) {
            w.u64(queueId);
            Messages.putRanges(w, ranges);
            w.bool(requeue).u32(delayMs).noTags();
        }

        static Nack read(Reader r) {
            Nack x = new Nack(r.u64(), Messages.getRanges(r), r.bool(), r.u32());
            r.tags();
            return x;
        }
    }

    /**
     * Ends a subscription; its messages in flight stay with it until acked or the connection closes.
     *
     * @param subscriptionId the subscription, a {@code u32}
     */
    record Unsubscribe(long subscriptionId) implements Request {
        @Override
        public ApiKey apiKey() {
            return ApiKey.UNSUBSCRIBE;
        }

        @Override
        public void write(Writer w) {
            w.u32(subscriptionId).noTags();
        }

        static Unsubscribe read(Reader r) {
            Unsubscribe x = new Unsubscribe(r.u32());
            r.tags();
            return x;
        }
    }

    /**
     * Joins, stays in or leaves a consumer group, and reconciles the member's partitions.
     *
     * @param groupId the group
     * @param memberId empty to join; the coordinator names the member
     * @param memberEpoch 0 to join, -1 to leave, otherwise the member's current epoch
     * @param rebalanceTimeoutMs how long the member may take to release a revoked partition, a {@code u32}
     * @param subscribed the streams it reads (each a {@code u64}); only when joining or when they change, otherwise {@code null}
     * @param owned the partitions it still owns; only when that changes, otherwise {@code null}
     */
    record GroupHeartbeat(
            String groupId,
            String memberId,
            int memberEpoch,
            long rebalanceTimeoutMs,
            List<Long> subscribed,
            List<TxnResource> owned)
            implements Request {
        /** Creates the request. */
        public GroupHeartbeat {
            subscribed = Messages.copy(subscribed);
            owned = Messages.copy(owned);
        }

        @Override
        public ApiKey apiKey() {
            return ApiKey.GROUP_HEARTBEAT;
        }

        @Override
        public void write(Writer w) {
            w.string(groupId).string(memberId).i32(memberEpoch).u32(rebalanceTimeoutMs);
            w.bool(subscribed != null);
            if (subscribed != null) {
                w.array(subscribed, Writer::u64);
            }
            Messages.putOptResources(w, owned);
            w.noTags();
        }

        static GroupHeartbeat read(Reader r) {
            String groupId = r.string();
            String memberId = r.string();
            int epoch = r.i32();
            long timeout = r.u32();
            List<Long> subscribed = r.bool() ? r.array(8, Reader::u64) : null;
            GroupHeartbeat x = new GroupHeartbeat(groupId, memberId, epoch, timeout, subscribed, Messages.getOptResources(r));
            r.tags();
            return x;
        }
    }

    /**
     * Commits a member's offsets for its group.
     *
     * @param groupId the group
     * @param memberId the member committing
     * @param memberEpoch its epoch
     * @param resources the offsets
     */
    record CommitOffsets(String groupId, String memberId, int memberEpoch, List<OffsetCommitResource> resources)
            implements Request {
        /** Creates the request. */
        public CommitOffsets {
            resources = List.copyOf(resources);
        }

        @Override
        public ApiKey apiKey() {
            return ApiKey.COMMIT_OFFSETS;
        }

        @Override
        public void write(Writer w) {
            w.string(groupId).string(memberId).i32(memberEpoch);
            Messages.putCommits(w, resources);
            w.noTags();
        }

        static CommitOffsets read(Reader r) {
            CommitOffsets x = new CommitOffsets(r.string(), r.string(), r.i32(), Messages.getCommits(r));
            r.tags();
            return x;
        }
    }

    /**
     * Reads a group's committed offsets.
     *
     * @param groupId the group
     * @param requireStable refuse with {@code UNSTABLE_OFFSET_COMMIT} while a transaction holds an uncommitted offset for a partition
     * @param resources the partitions
     */
    record OffsetFetch(String groupId, boolean requireStable, List<TxnResource> resources) implements Request {
        /** Creates the request. */
        public OffsetFetch {
            resources = List.copyOf(resources);
        }

        @Override
        public ApiKey apiKey() {
            return ApiKey.OFFSET_FETCH;
        }

        @Override
        public void write(Writer w) {
            w.string(groupId).bool(requireStable);
            Messages.putTxnResources(w, resources);
            w.noTags();
        }

        static OffsetFetch read(Reader r) {
            OffsetFetch x = new OffsetFetch(r.string(), r.bool(), Messages.getTxnResources(r));
            r.tags();
            return x;
        }
    }

    /**
     * Commits a group's offsets within a transaction: they take effect when it commits and are dropped when it
     * aborts. Answered like {@link CommitOffsets}.
     *
     * @param txnId transactional id
     * @param producerId producer id from {@code InitProducer}, a {@code u64}
     * @param producerEpoch its epoch, a {@code u16}
     * @param groupId the group
     * @param memberId the member whose partitions these are; a member that no longer owns them is fenced
     * @param memberEpoch its epoch
     * @param resources the offsets
     */
    record AddOffsetsToTxn(
            String txnId,
            long producerId,
            int producerEpoch,
            String groupId,
            String memberId,
            int memberEpoch,
            List<OffsetCommitResource> resources)
            implements Request {
        /** Creates the request. */
        public AddOffsetsToTxn {
            resources = List.copyOf(resources);
        }

        @Override
        public ApiKey apiKey() {
            return ApiKey.ADD_OFFSETS_TO_TXN;
        }

        @Override
        public void write(Writer w) {
            w.string(txnId).u64(producerId).u16(producerEpoch).string(groupId).string(memberId).i32(memberEpoch);
            Messages.putCommits(w, resources);
            w.noTags();
        }

        static AddOffsetsToTxn read(Reader r) {
            AddOffsetsToTxn x = new AddOffsetsToTxn(
                    r.string(), r.u64(), r.u16(), r.string(), r.string(), r.i32(), Messages.getCommits(r));
            r.tags();
            return x;
        }
    }

    /**
     * Creates a stream (admin).
     *
     * @param name its name; unique among all resources
     * @param tenant the tenant it belongs to, which must exist
     * @param partitions number of partitions, a {@code u32}
     * @param replication replicas per partition, a {@code u8}
     * @param configs settings
     */
    record CreateStream(String name, String tenant, long partitions, int replication, List<Config> configs)
            implements Request {
        /** Creates the request. */
        public CreateStream {
            configs = List.copyOf(configs);
        }

        @Override
        public ApiKey apiKey() {
            return ApiKey.CREATE_STREAM;
        }

        @Override
        public void write(Writer w) {
            w.string(name).string(tenant).u32(partitions).u8(replication);
            Messages.putConfigs(w, configs);
            w.noTags();
        }

        static CreateStream read(Reader r) {
            CreateStream x = new CreateStream(r.string(), r.string(), r.u32(), r.u8(), Messages.getConfigs(r));
            r.tags();
            return x;
        }
    }

    /**
     * Creates a queue (admin).
     *
     * @param name its name; unique among all resources
     * @param tenant the tenant it belongs to, which must exist
     * @param priorityLevels priority levels, 1 to 10 (only 1 is supported so far), a {@code u8}
     * @param singleActive single active consumer (not supported so far)
     * @param dlq the dead-letter queue, or {@code null}
     * @param replication replicas, a {@code u8}
     * @param configs settings
     */
    record CreateQueue(
            String name,
            String tenant,
            int priorityLevels,
            boolean singleActive,
            String dlq,
            int replication,
            List<Config> configs)
            implements Request {
        /** Creates the request. */
        public CreateQueue {
            configs = List.copyOf(configs);
        }

        @Override
        public ApiKey apiKey() {
            return ApiKey.CREATE_QUEUE;
        }

        @Override
        public void write(Writer w) {
            w.string(name).string(tenant).u8(priorityLevels).bool(singleActive).optString(dlq).u8(replication);
            Messages.putConfigs(w, configs);
            w.noTags();
        }

        static CreateQueue read(Reader r) {
            CreateQueue x = new CreateQueue(
                    r.string(), r.string(), r.u8(), r.bool(), r.optString(), r.u8(), Messages.getConfigs(r));
            r.tags();
            return x;
        }
    }

    /**
     * Declares an exchange (admin).
     *
     * @param name its name; unique among all resources
     * @param tenant the tenant it belongs to, which must exist
     * @param exchangeType see {@link ExchangeType}
     * @param configs settings
     */
    record DeclareExchange(String name, String tenant, int exchangeType, List<Config> configs) implements Request {
        /** Creates the request. */
        public DeclareExchange {
            configs = List.copyOf(configs);
        }

        @Override
        public ApiKey apiKey() {
            return ApiKey.DECLARE_EXCHANGE;
        }

        @Override
        public void write(Writer w) {
            w.string(name).string(tenant).u8(exchangeType);
            Messages.putConfigs(w, configs);
            w.noTags();
        }

        static DeclareExchange read(Reader r) {
            DeclareExchange x = new DeclareExchange(r.string(), r.string(), r.u8(), Messages.getConfigs(r));
            r.tags();
            return x;
        }
    }

    /**
     * Binds a queue to an exchange (admin). Answered with an outcome only.
     *
     * @param exchangeId the exchange, a {@code u64}
     * @param queueId the queue, a {@code u64}
     * @param bindingKey matched against routing keys (direct, topic)
     * @param headerMatch headers to match (headers exchange)
     * @param matchAll all headers must match ({@code true}) or any one
     */
    record Bind(long exchangeId, long queueId, String bindingKey, List<HeaderMatch> headerMatch, boolean matchAll)
            implements Request {
        /** Creates the request. */
        public Bind {
            headerMatch = List.copyOf(headerMatch);
        }

        @Override
        public ApiKey apiKey() {
            return ApiKey.BIND;
        }

        @Override
        public void write(Writer w) {
            Request.writeBinding(w, exchangeId, queueId, bindingKey, headerMatch, matchAll);
        }

        static Bind read(Reader r) {
            Bind x = new Bind(r.u64(), r.u64(), r.string(), Request.readHeaderMatches(r), r.bool());
            r.tags();
            return x;
        }
    }

    /**
     * Removes a binding (admin); the fields are those of {@link Bind}.
     *
     * @param exchangeId the exchange, a {@code u64}
     * @param queueId the queue, a {@code u64}
     * @param bindingKey matched against routing keys (direct, topic)
     * @param headerMatch headers to match (headers exchange)
     * @param matchAll all headers must match ({@code true}) or any one
     */
    record Unbind(long exchangeId, long queueId, String bindingKey, List<HeaderMatch> headerMatch, boolean matchAll)
            implements Request {
        /** Creates the request. */
        public Unbind {
            headerMatch = List.copyOf(headerMatch);
        }

        @Override
        public ApiKey apiKey() {
            return ApiKey.UNBIND;
        }

        @Override
        public void write(Writer w) {
            Request.writeBinding(w, exchangeId, queueId, bindingKey, headerMatch, matchAll);
        }

        static Unbind read(Reader r) {
            Unbind x = new Unbind(r.u64(), r.u64(), r.string(), Request.readHeaderMatches(r), r.bool());
            r.tags();
            return x;
        }
    }

    private static void writeBinding(
            Writer w, long exchangeId, long queueId, String key, List<HeaderMatch> headers, boolean matchAll) {
        w.u64(exchangeId).u64(queueId).string(key);
        w.array(headers, HeaderMatch::write);
        w.bool(matchAll).noTags();
    }

    private static List<HeaderMatch> readHeaderMatches(Reader r) {
        return r.array(5, HeaderMatch::read);
    }

    /**
     * Deletes a stream, queue or exchange created at runtime (admin).
     *
     * @param resourceId the resource, a {@code u64}
     * @param ifEmpty only if it holds no messages (not supported so far)
     */
    record DeleteResource(long resourceId, boolean ifEmpty) implements Request {
        @Override
        public ApiKey apiKey() {
            return ApiKey.DELETE_RESOURCE;
        }

        @Override
        public void write(Writer w) {
            w.u64(resourceId).bool(ifEmpty).noTags();
        }

        static DeleteResource read(Reader r) {
            DeleteResource x = new DeleteResource(r.u64(), r.bool());
            r.tags();
            return x;
        }
    }

    /**
     * Reads resources' settings (admin).
     *
     * @param resourceIds the resources, each a {@code u64}
     */
    record DescribeConfigs(List<Long> resourceIds) implements Request {
        /** Creates the request. */
        public DescribeConfigs {
            resourceIds = List.copyOf(resourceIds);
        }

        @Override
        public ApiKey apiKey() {
            return ApiKey.DESCRIBE_CONFIGS;
        }

        @Override
        public void write(Writer w) {
            w.array(resourceIds, Writer::u64).noTags();
        }

        static DescribeConfigs read(Reader r) {
            DescribeConfigs x = new DescribeConfigs(r.array(8, Reader::u64));
            r.tags();
            return x;
        }
    }

    /**
     * Changes a resource's settings (admin).
     *
     * @param resourceId the resource, a {@code u64}
     * @param set settings to set
     * @param delete settings to return to their default
     */
    record AlterConfigs(long resourceId, List<Config> set, List<String> delete) implements Request {
        /** Creates the request. */
        public AlterConfigs {
            set = List.copyOf(set);
            delete = List.copyOf(delete);
        }

        @Override
        public ApiKey apiKey() {
            return ApiKey.ALTER_CONFIGS;
        }

        @Override
        public void write(Writer w) {
            w.u64(resourceId);
            Messages.putConfigs(w, set);
            w.array(delete, Writer::string).noTags();
        }

        static AlterConfigs read(Reader r) {
            AlterConfigs x = new AlterConfigs(r.u64(), Messages.getConfigs(r), r.array(2, Reader::string));
            r.tags();
            return x;
        }
    }

    /** Asks one node for its metrics (admin). The body is an empty block of tagged fields. */
    record DescribeMetrics() implements Request {
        @Override
        public ApiKey apiKey() {
            return ApiKey.DESCRIBE_METRICS;
        }

        @Override
        public void write(Writer w) {
            w.noTags();
        }

        static DescribeMetrics read(Reader r) {
            r.tags();
            return new DescribeMetrics();
        }
    }

    /**
     * Starts authenticating with a SASL mechanism, or starts over before the session ends.
     *
     * @param mechanism {@code SCRAM-SHA-512} or {@code OAUTHBEARER}
     */
    record SaslHandshake(String mechanism) implements Request {
        @Override
        public ApiKey apiKey() {
            return ApiKey.SASL_HANDSHAKE;
        }

        @Override
        public void write(Writer w) {
            w.string(mechanism).noTags();
        }

        static SaslHandshake read(Reader r) {
            SaslHandshake x = new SaslHandshake(r.string());
            r.tags();
            return x;
        }
    }

    /**
     * One step of the mechanism chosen with {@link SaslHandshake}.
     *
     * @param authBytes the mechanism's message
     */
    record SaslAuthenticate(ByteString authBytes) implements Request {
        /** Creates the request. */
        public SaslAuthenticate {
            Objects.requireNonNull(authBytes);
        }

        @Override
        public ApiKey apiKey() {
            return ApiKey.SASL_AUTHENTICATE;
        }

        @Override
        public void write(Writer w) {
            w.bytes(authBytes).noTags();
        }

        static SaslAuthenticate read(Reader r) {
            SaslAuthenticate x = new SaslAuthenticate(r.bytes());
            r.tags();
            return x;
        }
    }

    /**
     * Sets or removes a user's SCRAM credential (admin).
     *
     * @param user the user name
     * @param credential the new credential; {@code null} removes the user's
     */
    record AlterUser(String user, ScramCredentialData credential) implements Request {
        @Override
        public ApiKey apiKey() {
            return ApiKey.ALTER_USER;
        }

        @Override
        public void write(Writer w) {
            w.string(user);
            if (credential == null) {
                w.u8(0);
            } else {
                w.u8(1).u32(credential.iterations());
                w.bytes(credential.salt()).bytes(credential.storedKey()).bytes(credential.serverKey());
            }
            w.noTags();
        }

        static AlterUser read(Reader r) {
            String user = r.string();
            ScramCredentialData credential;
            switch (r.u8()) {
                case 0 -> credential = null;
                case 1 -> credential = new ScramCredentialData(r.u32(), r.bytes(), r.bytes(), r.bytes());
                default -> throw new DecodeException(
                        DecodeException.Kind.MALFORMED, "malformed frame: credential flag is neither 0 nor 1");
            }
            r.tags();
            return new AlterUser(user, credential);
        }
    }

    /** Lists the users with a SCRAM credential (admin). The body is an empty block of tagged fields. */
    record DescribeUsers() implements Request {
        @Override
        public ApiKey apiKey() {
            return ApiKey.DESCRIBE_USERS;
        }

        @Override
        public void write(Writer w) {
            w.noTags();
        }

        static DescribeUsers read(Reader r) {
            r.tags();
            return new DescribeUsers();
        }
    }

    /**
     * Creates a tenant (cluster admin).
     *
     * @param name its name
     */
    record CreateTenant(String name) implements Request {
        @Override
        public ApiKey apiKey() {
            return ApiKey.CREATE_TENANT;
        }

        @Override
        public void write(Writer w) {
            w.string(name).noTags();
        }

        static CreateTenant read(Reader r) {
            CreateTenant x = new CreateTenant(r.string());
            r.tags();
            return x;
        }
    }

    /**
     * Deletes a tenant that holds no resources (cluster admin), with its role bindings.
     *
     * @param name its name
     */
    record DeleteTenant(String name) implements Request {
        @Override
        public ApiKey apiKey() {
            return ApiKey.DELETE_TENANT;
        }

        @Override
        public void write(Writer w) {
            w.string(name).noTags();
        }

        static DeleteTenant read(Reader r) {
            DeleteTenant x = new DeleteTenant(r.string());
            r.tags();
            return x;
        }
    }

    /** Lists the tenants the caller has a role in (every tenant for a cluster admin). Empty block of tagged fields. */
    record DescribeTenants() implements Request {
        @Override
        public ApiKey apiKey() {
            return ApiKey.DESCRIBE_TENANTS;
        }

        @Override
        public void write(Writer w) {
            w.noTags();
        }

        static DescribeTenants read(Reader r) {
            r.tags();
            return new DescribeTenants();
        }
    }

    /**
     * Adds and removes role bindings (cluster admin). Removals apply before additions.
     *
     * @param add bindings to add
     * @param remove bindings to remove
     */
    record AlterRoleBindings(List<RoleBinding> add, List<RoleBinding> remove) implements Request {
        /** Creates the request. */
        public AlterRoleBindings {
            add = List.copyOf(add);
            remove = List.copyOf(remove);
        }

        @Override
        public ApiKey apiKey() {
            return ApiKey.ALTER_ROLE_BINDINGS;
        }

        @Override
        public void write(Writer w) {
            w.array(add, RoleBinding::write).array(remove, RoleBinding::write).noTags();
        }

        static AlterRoleBindings read(Reader r) {
            AlterRoleBindings x = new AlterRoleBindings(
                    r.array(Messages.BINDING_MIN, RoleBinding::read), r.array(Messages.BINDING_MIN, RoleBinding::read));
            r.tags();
            return x;
        }
    }

    /** Lists every role binding (cluster admin). The body is an empty block of tagged fields. */
    record DescribeRoleBindings() implements Request {
        @Override
        public ApiKey apiKey() {
            return ApiKey.DESCRIBE_ROLE_BINDINGS;
        }

        @Override
        public void write(Writer w) {
            w.noTags();
        }

        static DescribeRoleBindings read(Reader r) {
            r.tags();
            return new DescribeRoleBindings();
        }
    }

    /** Asks who the connection acts as, and with which roles. The body is an empty block of tagged fields. */
    record WhoAmI() implements Request {
        @Override
        public ApiKey apiKey() {
            return ApiKey.WHO_AM_I;
        }

        @Override
        public void write(Writer w) {
            w.noTags();
        }

        static WhoAmI read(Reader r) {
            r.tags();
            return new WhoAmI();
        }
    }

    /**
     * Adds a node to the cluster (cluster admin).
     *
     * @param nodeId its id, a {@code u64}: never used in this cluster before
     * @param peer where the other nodes reach it ({@code host:port})
     * @param client where clients reach it ({@code host:port})
     */
    record AddNode(long nodeId, String peer, String client) implements Request {
        @Override
        public ApiKey apiKey() {
            return ApiKey.ADD_NODE;
        }

        @Override
        public void write(Writer w) {
            w.u64(nodeId).string(peer).string(client).noTags();
        }

        static AddNode read(Reader r) {
            AddNode x = new AddNode(r.u64(), r.string(), r.string());
            r.tags();
            return x;
        }
    }

    /**
     * Starts draining a node: its replicas move to other nodes, then it leaves the cluster (cluster admin).
     *
     * @param nodeId the node, a {@code u64}
     */
    record RemoveNode(long nodeId) implements Request {
        @Override
        public ApiKey apiKey() {
            return ApiKey.REMOVE_NODE;
        }

        @Override
        public void write(Writer w) {
            w.u64(nodeId).noTags();
        }

        static RemoveNode read(Reader r) {
            RemoveNode x = new RemoveNode(r.u64());
            r.tags();
            return x;
        }
    }

    /** Lists the cluster's nodes. The body is an empty block of tagged fields. */
    record DescribeNodes() implements Request {
        @Override
        public ApiKey apiKey() {
            return ApiKey.DESCRIBE_NODES;
        }

        @Override
        public void write(Writer w) {
            w.noTags();
        }

        static DescribeNodes read(Reader r) {
            r.tags();
            return new DescribeNodes();
        }
    }
}
