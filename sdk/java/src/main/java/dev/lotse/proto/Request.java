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
     * @param tenant the tenant the transactional id lives in (ADR 0062); written as a tag, left out for {@code default}
     */
    record Produce(String txnId, long timeoutMs, List<ProduceResource> resources, String tenant) implements Request {
        /** Creates the request. */
        public Produce {
            tenant = Messages.tenantOrDefault(tenant);
            resources = List.copyOf(resources);
        }

        /** The request in the default tenant. */
        public Produce(String txnId, long timeoutMs, List<ProduceResource> resources) {
            this(txnId, timeoutMs, resources, Messages.DEFAULT_TENANT);
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
            Messages.putTenant(w, tenant);
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
            return new Produce(txnId, timeoutMs, resources, Messages.getTenant(r));
        }
    }

    /**
     * Looks up offsets in stream partitions by time, or at either end.
     *
     * @param isolation see {@link Isolation}: what latest means and where a time lookup stops
     * @param resources what to look up
     */
    record ListOffsets(int isolation, List<ListOffsetsResource> resources) implements Request {
        /** Creates the request. */
        public ListOffsets {
            resources = List.copyOf(resources);
        }

        @Override
        public ApiKey apiKey() {
            return ApiKey.LIST_OFFSETS;
        }

        @Override
        public void write(Writer w) {
            w.u8(isolation);
            w.array(resources, (w1, res) -> {
                w1.u64(res.resourceId());
                w1.array(res.partitions(), (w2, p) -> {
                    w2.u32(p.partition());
                    w2.i64(p.timestamp());
                    w2.noTags();
                });
                w1.noTags();
            });
            w.noTags();
        }

        static ListOffsets read(Reader r) {
            int isolation = r.u8();
            List<ListOffsetsResource> resources = r.array(10, r1 -> {
                long id = r1.u64();
                List<ListOffsetsPartition> partitions = r1.array(13, r2 -> {
                    ListOffsetsPartition p = new ListOffsetsPartition(r2.u32(), r2.i64());
                    r2.tags();
                    return p;
                });
                r1.tags();
                return new ListOffsetsResource(id, partitions);
            });
            r.tags();
            return new ListOffsets(isolation, resources);
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
     * @param tenant the tenant the transactional id lives in (ADR 0062); written as a tag, left out for {@code default}
     */
    record InitProducer(String txnId, long txnTimeoutMs, String tenant) implements Request {
        /** Creates the request. */
        public InitProducer {
            tenant = Messages.tenantOrDefault(tenant);
        }

        /** The request in the default tenant. */
        public InitProducer(String txnId, long txnTimeoutMs) {
            this(txnId, txnTimeoutMs, Messages.DEFAULT_TENANT);
        }

        @Override
        public ApiKey apiKey() {
            return ApiKey.INIT_PRODUCER;
        }

        @Override
        public void write(Writer w) {
            w.optString(txnId).u32(txnTimeoutMs);
            Messages.putTenant(w, tenant);
        }

        static InitProducer read(Reader r) {
            InitProducer x = new InitProducer(r.optString(), r.u32(), Messages.getTenant(r));
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
     * @param tenant the tenant the transactional id lives in (ADR 0062); written as a tag, left out for {@code default}
     */
    record AddPartitionsToTxn(
            String txnId, long producerId, int producerEpoch, List<TxnResource> resources, String tenant)
            implements Request {
        /** Creates the request. */
        public AddPartitionsToTxn {
            tenant = Messages.tenantOrDefault(tenant);
            resources = List.copyOf(resources);
        }

        /** The request in the default tenant. */
        public AddPartitionsToTxn(String txnId, long producerId, int producerEpoch, List<TxnResource> resources) {
            this(txnId, producerId, producerEpoch, resources, Messages.DEFAULT_TENANT);
        }

        @Override
        public ApiKey apiKey() {
            return ApiKey.ADD_PARTITIONS_TO_TXN;
        }

        @Override
        public void write(Writer w) {
            w.string(txnId).u64(producerId).u16(producerEpoch);
            Messages.putTxnResources(w, resources);
            Messages.putTenant(w, tenant);
        }

        static AddPartitionsToTxn read(Reader r) {
            AddPartitionsToTxn x =
                    new AddPartitionsToTxn(
                            r.string(), r.u64(), r.u16(), Messages.getTxnResources(r), Messages.getTenant(r));
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
     * @param tenant the tenant the transactional id lives in (ADR 0062); written as a tag, left out for {@code default}
     */
    record EndTxn(String txnId, long producerId, int producerEpoch, boolean commit, String tenant) implements Request {
        /** Creates the request. */
        public EndTxn {
            tenant = Messages.tenantOrDefault(tenant);
        }

        /** The request in the default tenant. */
        public EndTxn(String txnId, long producerId, int producerEpoch, boolean commit) {
            this(txnId, producerId, producerEpoch, commit, Messages.DEFAULT_TENANT);
        }

        @Override
        public ApiKey apiKey() {
            return ApiKey.END_TXN;
        }

        @Override
        public void write(Writer w) {
            w.string(txnId).u64(producerId).u16(producerEpoch).bool(commit);
            Messages.putTenant(w, tenant);
        }

        static EndTxn read(Reader r) {
            EndTxn x = new EndTxn(r.string(), r.u64(), r.u16(), r.bool(), Messages.getTenant(r));
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
     * @param exclusive an exclusive consumer, the only one the queue may have: not built, refused; a single active
     *     consumer is the queue's setting {@code single.active.consumer} (ADR 0076)
     * @param notifyActive asks for a push when the subscription becomes the queue's single active consumer; a tag
     */
    record Subscribe(
            long queueId, String consumerTag, long creditMsgs, long creditBytes, boolean exclusive, boolean notifyActive)
            implements Request {
        /** Without asking to hear about becoming active. */
        public Subscribe(long queueId, String consumerTag, long creditMsgs, long creditBytes, boolean exclusive) {
            this(queueId, consumerTag, creditMsgs, creditBytes, exclusive, false);
        }

        @Override
        public ApiKey apiKey() {
            return ApiKey.SUBSCRIBE;
        }

        @Override
        public void write(Writer w) {
            w.u64(queueId).string(consumerTag).u32(creditMsgs).u32(creditBytes).bool(exclusive);
            if (notifyActive) {
                w.tags(List.of(new Tag(Messages.TAG_NOTIFY_ACTIVE, ByteString.copyOf(new byte[] {1}))));
            } else {
                w.noTags();
            }
        }

        static Subscribe read(Reader r) {
            long queueId = r.u64();
            String tag = r.string();
            long msgs = r.u32();
            long bytes = r.u32();
            boolean exclusive = r.bool();
            boolean notify = false;
            for (Tag t : r.tags()) {
                if (t.tag() == Messages.TAG_NOTIFY_ACTIVE) {
                    Reader vr = new Reader(t.value().toByteArray());
                    notify = vr.u8() != 0;
                    vr.finish();
                }
            }
            return new Subscribe(queueId, tag, msgs, bytes, exclusive, notify);
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
     * <p>With a transactional id (ADR 0075) the ack takes effect with the transaction's commit: it goes on the
     * connection the messages were delivered on, after the queue was added to the transaction, and carries the
     * transaction's producer id and epoch.
     *
     * @param queueId the queue, a {@code u64}
     * @param txnId a transaction, or {@code null} for a plain ack
     * @param ranges offset ranges {@code [from, to)}
     * @param tenant the tenant the transactional id lives in; sent as a tag, absent for {@code default}
     * @param producerId the transaction's producer id, a {@code u64}; sent as a tag with {@code producerEpoch} when
     *     {@code txnId} is set, ignored otherwise
     * @param producerEpoch the transaction's producer epoch, a {@code u16}
     */
    record Ack(long queueId, String txnId, List<Range> ranges, String tenant, long producerId, int producerEpoch)
            implements Request {
        /** Creates the request. */
        public Ack {
            ranges = List.copyOf(ranges);
            tenant = tenant == null ? Messages.DEFAULT_TENANT : tenant;
        }

        /** A plain ack, outside any transaction. */
        public Ack(long queueId, List<Range> ranges) {
            this(queueId, null, ranges, Messages.DEFAULT_TENANT, 0, 0);
        }

        @Override
        public ApiKey apiKey() {
            return ApiKey.ACK;
        }

        @Override
        public void write(Writer w) {
            w.u64(queueId).optString(txnId);
            Messages.putRanges(w, ranges);
            List<Tag> tags = new java.util.ArrayList<>();
            if (!Messages.DEFAULT_TENANT.equals(tenant)) {
                tags.add(Messages.stringTag(Messages.TAG_TENANT, tenant));
            }
            if (txnId != null) {
                Writer v = new Writer();
                v.u64(producerId).u16(producerEpoch);
                tags.add(new Tag(Messages.TAG_PRODUCER, ByteString.copyOf(v.toByteArray())));
            }
            if (tags.isEmpty()) {
                w.noTags();
            } else {
                w.tags(tags);
            }
        }

        static Ack read(Reader r) {
            long queueId = r.u64();
            String txnId = r.optString();
            List<Range> ranges = Messages.getRanges(r);
            String tenant = Messages.DEFAULT_TENANT;
            long producerId = 0;
            int epoch = 0;
            for (Tag t : r.tags()) {
                Reader vr = new Reader(t.value().toByteArray());
                if (t.tag() == Messages.TAG_TENANT) {
                    tenant = vr.string();
                } else if (t.tag() == Messages.TAG_PRODUCER) {
                    producerId = vr.u64();
                    epoch = vr.u16();
                } else {
                    continue; // from a newer version: skipped
                }
                vr.finish();
            }
            return new Ack(queueId, txnId, ranges, tenant, producerId, epoch);
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
     * @param tenant the tenant the group id lives in (ADR 0062); written as a tag, left out for {@code default}
     * @param instanceId a stable id for the member across restarts (ADR 0072), tag 2; {@code null} for none
     * @param subscribedPattern subscribe to the streams whose whole name matches this regular expression (ADR 0073),
     *     tag 3, sent when joining; {@code null} to subscribe by {@code subscribed}
     */
    record GroupHeartbeat(
            String groupId,
            String memberId,
            int memberEpoch,
            long rebalanceTimeoutMs,
            List<Long> subscribed,
            List<TxnResource> owned,
            String tenant,
            String instanceId,
            String subscribedPattern)
            implements Request {
        /** Creates the request. */
        public GroupHeartbeat {
            tenant = Messages.tenantOrDefault(tenant);
            subscribed = Messages.copy(subscribed);
            owned = Messages.copy(owned);
        }

        /** The request without an instance id. */
        public GroupHeartbeat(
                String groupId,
                String memberId,
                int memberEpoch,
                long rebalanceTimeoutMs,
                List<Long> subscribed,
                List<TxnResource> owned,
                String tenant) {
            this(groupId, memberId, memberEpoch, rebalanceTimeoutMs, subscribed, owned, tenant, null, null);
        }

        /** The request in the default tenant. */
        public GroupHeartbeat(
                String groupId,
                String memberId,
                int memberEpoch,
                long rebalanceTimeoutMs,
                List<Long> subscribed,
                List<TxnResource> owned) {
            this(groupId, memberId, memberEpoch, rebalanceTimeoutMs, subscribed, owned, Messages.DEFAULT_TENANT, null, null);
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
            List<Tag> tags = new java.util.ArrayList<>();
            if (!Messages.DEFAULT_TENANT.equals(tenant)) {
                tags.add(Messages.stringTag(Messages.TAG_TENANT, tenant));
            }
            if (instanceId != null) {
                tags.add(Messages.stringTag(Messages.TAG_INSTANCE, instanceId));
            }
            if (subscribedPattern != null) {
                tags.add(Messages.stringTag(Messages.TAG_PATTERN, subscribedPattern));
            }
            if (tags.isEmpty()) {
                w.noTags();
            } else {
                w.tags(tags);
            }
        }

        static GroupHeartbeat read(Reader r) {
            String groupId = r.string();
            String memberId = r.string();
            int epoch = r.i32();
            long timeout = r.u32();
            List<Long> subscribed = r.bool() ? r.array(8, Reader::u64) : null;
            List<TxnResource> owned = Messages.getOptResources(r);
            String tenant = Messages.DEFAULT_TENANT;
            String instanceId = null;
            String pattern = null;
            for (Tag t : r.tags()) {
                if (t.tag() == Messages.TAG_TENANT || t.tag() == Messages.TAG_INSTANCE || t.tag() == Messages.TAG_PATTERN) {
                    Reader vr = new Reader(t.value().toByteArray());
                    String v = vr.string();
                    vr.finish();
                    if (t.tag() == Messages.TAG_TENANT) {
                        tenant = v;
                    } else if (t.tag() == Messages.TAG_INSTANCE) {
                        instanceId = v;
                    } else {
                        pattern = v;
                    }
                }
            }
            return new GroupHeartbeat(
                    groupId, memberId, epoch, timeout, subscribed, owned, tenant, instanceId, pattern);
        }
    }

    /**
     * Commits a member's offsets for its group.
     *
     * @param groupId the group
     * @param memberId the member committing
     * @param memberEpoch its epoch
     * @param resources the offsets
     * @param tenant the tenant the group id lives in (ADR 0062); written as a tag, left out for {@code default}
     */
    record CommitOffsets(
            String groupId, String memberId, int memberEpoch, List<OffsetCommitResource> resources, String tenant)
            implements Request {
        /** Creates the request. */
        public CommitOffsets {
            tenant = Messages.tenantOrDefault(tenant);
            resources = List.copyOf(resources);
        }

        /** The request in the default tenant. */
        public CommitOffsets(String groupId, String memberId, int memberEpoch, List<OffsetCommitResource> resources) {
            this(groupId, memberId, memberEpoch, resources, Messages.DEFAULT_TENANT);
        }

        @Override
        public ApiKey apiKey() {
            return ApiKey.COMMIT_OFFSETS;
        }

        @Override
        public void write(Writer w) {
            w.string(groupId).string(memberId).i32(memberEpoch);
            Messages.putCommits(w, resources);
            Messages.putTenant(w, tenant);
        }

        static CommitOffsets read(Reader r) {
            CommitOffsets x =
                    new CommitOffsets(r.string(), r.string(), r.i32(), Messages.getCommits(r), Messages.getTenant(r));
            return x;
        }
    }

    /**
     * Reads a group's committed offsets.
     *
     * @param groupId the group
     * @param requireStable refuse with {@code UNSTABLE_OFFSET_COMMIT} while a transaction holds an uncommitted offset for a partition
     * @param resources the partitions
     * @param tenant the tenant the group id lives in (ADR 0062); written as a tag, left out for {@code default}
     */
    record OffsetFetch(String groupId, boolean requireStable, List<TxnResource> resources, String tenant)
            implements Request {
        /** Creates the request. */
        public OffsetFetch {
            tenant = Messages.tenantOrDefault(tenant);
            resources = List.copyOf(resources);
        }

        /** The request in the default tenant. */
        public OffsetFetch(String groupId, boolean requireStable, List<TxnResource> resources) {
            this(groupId, requireStable, resources, Messages.DEFAULT_TENANT);
        }

        @Override
        public ApiKey apiKey() {
            return ApiKey.OFFSET_FETCH;
        }

        @Override
        public void write(Writer w) {
            w.string(groupId).bool(requireStable);
            Messages.putTxnResources(w, resources);
            Messages.putTenant(w, tenant);
        }

        static OffsetFetch read(Reader r) {
            OffsetFetch x = new OffsetFetch(r.string(), r.bool(), Messages.getTxnResources(r), Messages.getTenant(r));
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
     * @param tenant the tenant the transactional id and the group id live in (ADR 0062); written as a tag, left out
     *     for {@code default}
     */
    record AddOffsetsToTxn(
            String txnId,
            long producerId,
            int producerEpoch,
            String groupId,
            String memberId,
            int memberEpoch,
            List<OffsetCommitResource> resources,
            String tenant)
            implements Request {
        /** Creates the request. */
        public AddOffsetsToTxn {
            tenant = Messages.tenantOrDefault(tenant);
            resources = List.copyOf(resources);
        }

        /** The request in the default tenant. */
        public AddOffsetsToTxn(
                String txnId,
                long producerId,
                int producerEpoch,
                String groupId,
                String memberId,
                int memberEpoch,
                List<OffsetCommitResource> resources) {
            this(txnId, producerId, producerEpoch, groupId, memberId, memberEpoch, resources, Messages.DEFAULT_TENANT);
        }

        @Override
        public ApiKey apiKey() {
            return ApiKey.ADD_OFFSETS_TO_TXN;
        }

        @Override
        public void write(Writer w) {
            w.string(txnId).u64(producerId).u16(producerEpoch).string(groupId).string(memberId).i32(memberEpoch);
            Messages.putCommits(w, resources);
            Messages.putTenant(w, tenant);
        }

        static AddOffsetsToTxn read(Reader r) {
            AddOffsetsToTxn x = new AddOffsetsToTxn(
                    r.string(),
                    r.u64(),
                    r.u16(),
                    r.string(),
                    r.string(),
                    r.i32(),
                    Messages.getCommits(r),
                    Messages.getTenant(r));
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
     * @param ifEmpty only if it holds nothing: no unsettled message in a queue, no record in a stream (ADR 0063)
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
     * Pins the leader of a partition of a stream or queue created at runtime to one of its voters, or unpins it
     * (cluster admin, ADR 0065). Answered with {@link Response.Admin}.
     *
     * @param resourceId the stream or queue, a {@code u64}
     * @param partition the partition, a {@code u32} (0 for a queue)
     * @param nodeId the node to lead it, a {@code u64}; 0 unpins it
     */
    record MoveLeader(long resourceId, long partition, long nodeId) implements Request {
        @Override
        public ApiKey apiKey() {
            return ApiKey.MOVE_LEADER;
        }

        @Override
        public void write(Writer w) {
            w.u64(resourceId).u32(partition).u64(nodeId).noTags();
        }

        static MoveLeader read(Reader r) {
            MoveLeader x = new MoveLeader(r.u64(), r.u32(), r.u64());
            r.tags();
            return x;
        }
    }

    /**
     * Publishes messages through exchanges (ADR 0079): the node that gets it routes each message by the exchange's
     * bindings as it knows them and writes it to every queue it routes to, if the publisher may write each of them.
     * Not atomic across queues.
     *
     * @param txnId a transaction: refused; a transaction publishes by producing to each queue itself
     * @param timeoutMs how long the node may take, a {@code u32}
     * @param entries the messages
     */
    record Publish(String txnId, long timeoutMs, List<Messages.PublishEntry> entries) implements Request {
        /** Creates the request. */
        public Publish {
            entries = List.copyOf(entries);
        }

        @Override
        public ApiKey apiKey() {
            return ApiKey.PUBLISH;
        }

        @Override
        public void write(Writer w) {
            w.optString(txnId).u32(timeoutMs);
            w.array(entries, (w1, e) -> {
                w1.u64(e.exchangeId()).string(e.routingKey()).bool(e.mandatory()).bytes(e.batch());
                w1.noTags();
            });
            w.noTags();
        }

        static Publish read(Reader r) {
            String txnId = r.optString();
            long timeout = r.u32();
            List<Messages.PublishEntry> entries = r.array(15, r1 -> {
                Messages.PublishEntry e = new Messages.PublishEntry(r1.u64(), r1.string(), r1.bool(), r1.bytes());
                r1.tags();
                return e;
            });
            r.tags();
            return new Publish(txnId, timeout, entries);
        }
    }

    /**
     * Raises the partition count of a stream created at runtime (tenant admin, ADR 0074); never lowers it.
     * Answered with {@link Response.Admin}.
     *
     * @param resourceId the stream, a {@code u64}
     * @param count the partition count it is to have, a {@code u32}
     */
    record CreatePartitions(long resourceId, long count) implements Request {
        @Override
        public ApiKey apiKey() {
            return ApiKey.CREATE_PARTITIONS;
        }

        @Override
        public void write(Writer w) {
            w.u64(resourceId).u32(count).noTags();
        }

        static CreatePartitions read(Reader r) {
            CreatePartitions x = new CreatePartitions(r.u64(), r.u32());
            r.tags();
            return x;
        }
    }

    /**
     * Revokes a user's or an IdP group's sessions (cluster admin, ADR 0070): every node ends the sessions that
     * authenticated before {@code beforeMs} and refuses tokens issued before it. Answered with
     * {@link Response.Admin}.
     *
     * @param subjectKind 0 for a user, 1 for a group
     * @param subject the user or group
     * @param beforeMs milliseconds since the Unix epoch, a {@code u64}; 0 for now, by the metadata leader's clock
     */
    record RevokeSessions(int subjectKind, String subject, long beforeMs) implements Request {
        @Override
        public ApiKey apiKey() {
            return ApiKey.REVOKE_SESSIONS;
        }

        @Override
        public void write(Writer w) {
            w.u8(subjectKind).string(subject).u64(beforeMs).noTags();
        }

        static RevokeSessions read(Reader r) {
            RevokeSessions x = new RevokeSessions(r.u8(), r.string(), r.u64());
            r.tags();
            return x;
        }
    }

    /**
     * Adds a certificate to the cluster's revocation list (cluster admin): every node refuses it from then on and
     * ends its connections. Answered with {@link Response.Admin}.
     *
     * @param issuer SHA-256 of the issuer's distinguished name in DER form, 32 bytes
     * @param serial the serial number, big-endian, as the certificate has it
     * @param notAfterMs milliseconds since the Unix epoch at which the certificate stops being valid, a
     *     {@code u64}; the entry is dropped after that
     * @param reason why, for the people who read the list later; may be empty
     */
    record RevokeCertificate(ByteString issuer, ByteString serial, long notAfterMs, String reason)
            implements Request {
        @Override
        public ApiKey apiKey() {
            return ApiKey.REVOKE_CERTIFICATE;
        }

        @Override
        public void write(Writer w) {
            w.bytes(issuer).bytes(serial).u64(notAfterMs).string(reason).noTags();
        }

        static RevokeCertificate read(Reader r) {
            RevokeCertificate x = new RevokeCertificate(r.bytes(), r.bytes(), r.u64(), r.string());
            r.tags();
            return x;
        }
    }

    /**
     * {@code PurgeQueue} (47) or {@code Requeue} (48): an action on a queue's ready messages, served by its leader
     * (ADR 0064). Each request acts on a bounded number of messages; send it again with the answer's {@code upTo}
     * while the answer says {@code more}.
     *
     * @param requeue {@code true} for {@code Requeue}, which moves a dead-letter queue's messages back to their
     *     source; not on the wire, it picks the key
     * @param queueId the queue, a {@code u64}
     * @param upTo act only below this offset, a {@code u64}; 0 for the queue's end now
     */
    record QueueAction(boolean requeue, long queueId, long upTo) implements Request {
        @Override
        public ApiKey apiKey() {
            return requeue ? ApiKey.REQUEUE : ApiKey.PURGE_QUEUE;
        }

        @Override
        public void write(Writer w) {
            w.u64(queueId).u64(upTo).noTags();
        }

        static QueueAction read(Reader r, boolean requeue) {
            QueueAction x = new QueueAction(requeue, r.u64(), r.u64());
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
