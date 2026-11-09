package dev.lotse.proto;

import dev.lotse.proto.Messages.*;
import dev.lotse.record.ByteString;
import java.util.List;
import java.util.Objects;

/**
 * A response. Which type follows from the request it answers, not from the bytes: a response carries no API key,
 * so {@link Frames#decodeResponse} needs the key of the request.
 *
 * <p>Where the tagged-field block sits differs per message: most end with it, several end with the leader hint
 * <em>as</em> that block, and a few have none at all. Each type's {@code write} is the layout.
 */
public sealed interface Response {
    /** Writes the response body, which follows the response header. */
    void write(Writer w);

    /**
     * The versions and features of a node.
     *
     * @param versions the version range per API key
     * @param features feature names
     */
    record ApiVersions(List<ApiVersionRange> versions, List<String> features) implements Response {
        /** Creates the response. */
        public ApiVersions {
            versions = List.copyOf(versions);
            features = List.copyOf(features);
        }

        @Override
        public void write(Writer w) {
            w.array(versions, (w1, v) -> w1.u16(v.apiKey().id()).u16(v.min()).u16(v.max()));
            w.array(features, Writer::string);
            w.noTags();
        }

        static ApiVersions read(Reader r) {
            List<ApiVersionRange> versions =
                    r.array(6, r1 -> new ApiVersionRange(new ApiKey(r1.u16()), r1.u16(), r1.u16()));
            List<String> features = r.array(2, Reader::string);
            r.tags();
            return new ApiVersions(versions, features);
        }
    }

    /**
     * Nodes and resources.
     *
     * @param brokers every broker node
     * @param resources the resources asked for
     */
    record Metadata(List<Broker> brokers, List<ResourceMetadata> resources) implements Response {
        /** Creates the response. */
        public Metadata {
            brokers = List.copyOf(brokers);
            resources = List.copyOf(resources);
        }

        @Override
        public void write(Writer w) {
            w.array(brokers, (w1, b) -> {
                w1.u32(b.nodeId()).string(b.host()).u16(b.port()).string(b.rack());
                w1.noTags();
            });
            w.array(resources, (w1, res) -> {
                w1.u16(res.error().code()).string(res.name()).string(res.tenant());
                w1.u64(res.resourceId()).u8(res.kind());
                w1.array(res.partitions(), (w2, p) -> {
                    w2.u32(p.partition()).u32(p.leader()).u64(p.leaderTerm());
                    w2.array(p.voters(), Writer::u32);
                    w2.noTags();
                });
                w1.bool(res.exchange() != null);
                if (res.exchange() != null) {
                    w1.u8(res.exchange().exchangeType());
                    w1.array(res.exchange().bindings(), (w2, b) -> {
                        w2.u64(b.queueId()).string(b.bindingKey());
                        w2.array(b.headerMatch(), HeaderMatch::write);
                        w2.bool(b.matchAll());
                        w2.noTags();
                    });
                }
                w1.noTags();
            });
            w.noTags();
        }

        static Metadata read(Reader r) {
            // node id, host, port, rack, no tags
            List<Broker> brokers = r.array(11, r1 -> {
                Broker b = new Broker(r1.u32(), r1.string(), r1.u16(), r1.string());
                r1.tags();
                return b;
            });
            // error, name, tenant, id, kind, no partitions, no exchange, no tags
            List<ResourceMetadata> resources = r.array(18, r1 -> {
                ErrorCode error = new ErrorCode(r1.u16());
                String name = r1.string();
                String tenant = r1.string();
                long id = r1.u64();
                int kind = r1.u8();
                List<PartitionMetadata> partitions = r1.array(18, r2 -> {
                    PartitionMetadata p = new PartitionMetadata(r2.u32(), r2.u32(), r2.u64(), r2.array(4, Reader::u32));
                    r2.tags();
                    return p;
                });
                ExchangeMetadata exchange = null;
                if (r1.bool()) {
                    exchange = new ExchangeMetadata(r1.u8(), r1.array(13, r2 -> {
                        BindingMetadata b = new BindingMetadata(
                                r2.u64(), r2.string(), r2.array(5, HeaderMatch::read), r2.bool());
                        r2.tags();
                        return b;
                    }));
                }
                r1.tags();
                return new ResourceMetadata(error, name, tenant, id, kind, partitions, exchange);
            });
            r.tags();
            return new Metadata(brokers, resources);
        }
    }

    /**
     * The node coordinating a group or producers.
     *
     * @param error {@link ErrorCode#COORDINATOR_NOT_AVAILABLE} while there is none
     * @param nodeId its node id, a {@code u32}
     * @param host host clients connect to
     * @param port port clients connect to, a {@code u16}
     * @param errorMsg details for humans, or {@code null}
     */
    record FindCoordinator(ErrorCode error, long nodeId, String host, int port, String errorMsg) implements Response {
        @Override
        public void write(Writer w) {
            w.u16(error.code()).u32(nodeId).string(host).u16(port).optString(errorMsg).noTags();
        }

        static FindCoordinator read(Reader r) {
            FindCoordinator x =
                    new FindCoordinator(new ErrorCode(r.u16()), r.u32(), r.string(), r.u16(), r.optString());
            r.tags();
            return x;
        }
    }

    /**
     * A producer id and epoch. The leader hint is the structure's block of tagged fields.
     *
     * @param error the outcome
     * @param producerId the id, a {@code u64}; never 0
     * @param producerEpoch the epoch, a {@code u16}
     * @param leaderHint the coordinator, with {@link ErrorCode#NOT_LEADER}, or {@code null}
     */
    record InitProducer(ErrorCode error, long producerId, int producerEpoch, LeaderHint leaderHint)
            implements Response {
        @Override
        public void write(Writer w) {
            w.u16(error.code()).u64(producerId).u16(producerEpoch);
            Messages.putHint(w, leaderHint);
        }

        static InitProducer read(Reader r) {
            return new InitProducer(new ErrorCode(r.u16()), r.u64(), r.u16(), Messages.getHint(r));
        }
    }

    /**
     * Per partition, whether it was added.
     *
     * @param resources per resource
     */
    record AddPartitionsToTxn(List<TxnResourceResult> resources) implements Response {
        /** Creates the response. */
        public AddPartitionsToTxn {
            resources = List.copyOf(resources);
        }

        @Override
        public void write(Writer w) {
            Messages.putTxnResults(w, resources);
            w.noTags();
        }

        static AddPartitionsToTxn read(Reader r) {
            AddPartitionsToTxn x = new AddPartitionsToTxn(Messages.getTxnResults(r));
            r.tags();
            return x;
        }
    }

    /**
     * The decision is durable; markers follow. Ending a transaction raises the producer epoch: the next
     * transaction uses the one returned.
     *
     * @param error the outcome
     * @param producerId producer id for the next transaction, a {@code u64}
     * @param producerEpoch epoch for the next transaction, a {@code u16}
     */
    record EndTxn(ErrorCode error, long producerId, int producerEpoch) implements Response {
        @Override
        public void write(Writer w) {
            w.u16(error.code()).u64(producerId).u16(producerEpoch).noTags();
        }

        static EndTxn read(Reader r) {
            EndTxn x = new EndTxn(new ErrorCode(r.u16()), r.u64(), r.u16());
            r.tags();
            return x;
        }
    }

    /**
     * The subscription, or why not. The leader hint is the structure's block of tagged fields.
     *
     * @param error the outcome
     * @param subscriptionId names the subscription in pushes, {@code Credit} and {@code Unsubscribe}, a {@code u32}
     * @param leaderHint the queue's leader, with {@link ErrorCode#NOT_LEADER}, or {@code null}
     * @param standby the queue has a single active consumer and this subscription waits for its turn (ADR 0076); a
     *     tag in the hint's block
     */
    record Subscribe(ErrorCode error, long subscriptionId, LeaderHint leaderHint, boolean standby)
            implements Response {
        /** Not a standby. */
        public Subscribe(ErrorCode error, long subscriptionId, LeaderHint leaderHint) {
            this(error, subscriptionId, leaderHint, false);
        }

        @Override
        public void write(Writer w) {
            w.u16(error.code()).u32(subscriptionId);
            List<Tag> tags = new java.util.ArrayList<>();
            if (leaderHint != null) {
                Writer v = new Writer();
                v.u32(leaderHint.leader()).u64(leaderHint.term());
                tags.add(new Tag(Messages.TAG_LEADER_HINT, ByteString.copyOf(v.toByteArray())));
            }
            if (standby) {
                tags.add(new Tag(Messages.TAG_STANDBY, ByteString.copyOf(new byte[] {1})));
            }
            if (tags.isEmpty()) {
                w.noTags();
            } else {
                w.tags(tags);
            }
        }

        static Subscribe read(Reader r) {
            ErrorCode error = new ErrorCode(r.u16());
            long id = r.u32();
            LeaderHint hint = null;
            boolean standby = false;
            for (Tag t : r.tags()) {
                Reader vr = new Reader(t.value().toByteArray());
                if (t.tag() == Messages.TAG_LEADER_HINT) {
                    hint = new LeaderHint(vr.u32(), vr.u64());
                } else if (t.tag() == Messages.TAG_STANDBY) {
                    standby = vr.u8() != 0;
                } else {
                    continue; // from a newer version: skipped
                }
                vr.finish();
            }
            return new Subscribe(error, id, hint, standby);
        }
    }

    /**
     * What became of each published message, in the request's order (ADR 0079).
     *
     * @param throttleMs how long the client should wait before its next request, a {@code u32}
     * @param entries per message
     */
    record Publish(long throttleMs, List<PublishResult> entries) implements Response {
        /** Creates the response. */
        public Publish {
            entries = List.copyOf(entries);
        }

        @Override
        public void write(Writer w) {
            w.u32(throttleMs);
            w.array(entries, (w1, e) -> {
                w1.u16(e.error().code());
                w1.array(e.routed(), (w2, q) -> w2.u64(q.queueId()).string(q.name()).u16(q.error().code()).u64(q.baseOffset()));
                w1.noTags();
            });
            w.noTags();
        }

        static Publish read(Reader r) {
            long throttle = r.u32();
            List<PublishResult> entries = r.array(4, r1 -> {
                ErrorCode error = new ErrorCode(r1.u16());
                List<RoutedMessage> routed =
                        r1.array(20, r2 -> new RoutedMessage(r2.u64(), r2.string(), new ErrorCode(r2.u16()), r2.u64()));
                r1.tags();
                return new PublishResult(error, routed);
            });
            r.tags();
            return new Publish(throttle, entries);
        }
    }

    /**
     * Only an outcome, for {@code Ack}, {@code Nack}, {@code Unsubscribe}, {@code Bind} and {@code Unbind}. The
     * leader hint is the structure's block of tagged fields.
     *
     * @param error the outcome
     * @param leaderHint the leader, with {@link ErrorCode#NOT_LEADER}, or {@code null}
     */
    record Outcome(ErrorCode error, LeaderHint leaderHint) implements Response {
        @Override
        public void write(Writer w) {
            w.u16(error.code());
            Messages.putHint(w, leaderHint);
        }

        static Outcome read(Reader r) {
            return new Outcome(new ErrorCode(r.u16()), Messages.getHint(r));
        }
    }

    /**
     * The member's state after a heartbeat.
     *
     * @param throttleMs throttle time, a {@code u32}
     * @param error the outcome
     * @param memberId the member's id (new after a join)
     * @param memberEpoch the member's epoch now
     * @param heartbeatIntervalMs when to send the next heartbeat, a {@code u32}
     * @param assignment the member's full assignment, whenever its epoch changed, otherwise {@code null}
     */
    record GroupHeartbeat(
            long throttleMs,
            ErrorCode error,
            String memberId,
            int memberEpoch,
            long heartbeatIntervalMs,
            List<TxnResource> assignment)
            implements Response {
        /** Creates the response. */
        public GroupHeartbeat {
            assignment = Messages.copy(assignment);
        }

        @Override
        public void write(Writer w) {
            w.u32(throttleMs).u16(error.code()).string(memberId).i32(memberEpoch).u32(heartbeatIntervalMs);
            Messages.putOptResources(w, assignment);
            w.noTags();
        }

        static GroupHeartbeat read(Reader r) {
            GroupHeartbeat x = new GroupHeartbeat(
                    r.u32(), new ErrorCode(r.u16()), r.string(), r.i32(), r.u32(), Messages.getOptResources(r));
            r.tags();
            return x;
        }
    }

    /**
     * The outcome per partition of {@code CommitOffsets} or {@code AddOffsetsToTxn}.
     *
     * @param resources per resource and partition
     */
    record CommitOffsets(List<TxnResourceResult> resources) implements Response {
        /** Creates the response. */
        public CommitOffsets {
            resources = List.copyOf(resources);
        }

        @Override
        public void write(Writer w) {
            Messages.putTxnResults(w, resources);
            w.noTags();
        }

        static CommitOffsets read(Reader r) {
            CommitOffsets x = new CommitOffsets(Messages.getTxnResults(r));
            r.tags();
            return x;
        }
    }

    /**
     * The committed offsets.
     *
     * @param resources per resource and partition
     */
    record OffsetFetch(List<OffsetFetchResource> resources) implements Response {
        /** Creates the response. */
        public OffsetFetch {
            resources = List.copyOf(resources);
        }

        @Override
        public void write(Writer w) {
            w.array(resources, (w1, r) -> {
                w1.u64(r.resourceId());
                w1.array(r.partitions(), (w2, p) -> {
                    w2.u32(p.partition()).u16(p.error().code()).u64(p.offset()).u64(p.leaderTerm());
                    w2.optString(p.metadata());
                    w2.noTags();
                });
                w1.noTags();
            });
            w.noTags();
        }

        static OffsetFetch read(Reader r) {
            List<OffsetFetchResource> resources = r.array(10, r1 -> {
                long id = r1.u64();
                List<OffsetFetchPartition> partitions = r1.array(25, r2 -> {
                    OffsetFetchPartition p = new OffsetFetchPartition(
                            r2.u32(), new ErrorCode(r2.u16()), r2.u64(), r2.u64(), r2.optString());
                    r2.tags();
                    return p;
                });
                r1.tags();
                return new OffsetFetchResource(id, partitions);
            });
            r.tags();
            return new OffsetFetch(resources);
        }
    }

    /**
     * The resource created, or why not, for {@code CreateStream}, {@code CreateQueue} and
     * {@code DeclareExchange}. The leader hint is the structure's block of tagged fields.
     *
     * @param error the outcome; {@link ErrorCode#RESOURCE_EXISTS} names the existing one
     * @param resourceId the resource's id, a {@code u64}
     * @param errorMsg why, for a refusal, or {@code null}
     * @param leaderHint the metadata group's leader, with {@link ErrorCode#NOT_LEADER}, or {@code null}
     */
    record Create(ErrorCode error, long resourceId, String errorMsg, LeaderHint leaderHint) implements Response {
        @Override
        public void write(Writer w) {
            w.u16(error.code()).u64(resourceId).optString(errorMsg);
            Messages.putHint(w, leaderHint);
        }

        static Create read(Reader r) {
            return new Create(new ErrorCode(r.u16()), r.u64(), r.optString(), Messages.getHint(r));
        }
    }

    /**
     * The outcome of an admin change that creates nothing. The leader hint is the structure's block of tagged
     * fields.
     *
     * @param error the outcome
     * @param errorMsg why, for a refusal, or {@code null}
     * @param leaderHint the metadata group's leader, with {@link ErrorCode#NOT_LEADER}, or {@code null}
     */
    record Admin(ErrorCode error, String errorMsg, LeaderHint leaderHint) implements Response {
        @Override
        public void write(Writer w) {
            w.u16(error.code()).optString(errorMsg);
            Messages.putHint(w, leaderHint);
        }

        static Admin read(Reader r) {
            return new Admin(new ErrorCode(r.u16()), r.optString(), Messages.getHint(r));
        }
    }

    /**
     * The answer to {@code PurgeQueue} and {@code Requeue} (ADR 0064).
     *
     * @param error the outcome
     * @param errorMsg why, for a refusal, or {@code null}
     * @param count messages purged or requeued by this request, a {@code u64}
     * @param upTo the bound it acted below, a {@code u64}; send it again to go on
     * @param more whether ready messages below {@code upTo} are left
     * @param leaderHint the queue's leader, with {@link ErrorCode#NOT_LEADER}, or {@code null}
     */
    record QueueAction(ErrorCode error, String errorMsg, long count, long upTo, boolean more, LeaderHint leaderHint)
            implements Response {
        @Override
        public void write(Writer w) {
            w.u16(error.code()).optString(errorMsg).u64(count).u64(upTo).bool(more);
            Messages.putHint(w, leaderHint);
        }

        static QueueAction read(Reader r) {
            return new QueueAction(
                    new ErrorCode(r.u16()), r.optString(), r.u64(), r.u64(), r.bool(), Messages.getHint(r));
        }
    }

    /**
     * The settings asked for. The leader hint sits <em>before</em> the resources and there is no trailing block
     * of tagged fields.
     *
     * @param error whether the request could be served here at all
     * @param leaderHint the metadata group's leader, with {@link ErrorCode#NOT_LEADER}, or {@code null}
     * @param resources per resource, in the order asked
     */
    record DescribeConfigs(ErrorCode error, LeaderHint leaderHint, List<ResourceConfigs> resources)
            implements Response {
        /** Creates the response. */
        public DescribeConfigs {
            resources = List.copyOf(resources);
        }

        @Override
        public void write(Writer w) {
            w.u16(error.code());
            Messages.putHint(w, leaderHint);
            w.array(resources, (w1, r) -> {
                w1.u64(r.resourceId()).u16(r.error().code());
                w1.array(r.configs(), (w2, c) -> w2.string(c.key()).string(c.value()).u8(c.source()));
            });
        }

        static DescribeConfigs read(Reader r) {
            ErrorCode error = new ErrorCode(r.u16());
            LeaderHint hint = Messages.getHint(r);
            // resource id, error, an empty settings array: 8 + 2 + 1 bytes
            List<ResourceConfigs> resources = r.array(11, r1 -> new ResourceConfigs(
                    r1.u64(),
                    new ErrorCode(r1.u16()),
                    r1.array(5, r2 -> new ConfigEntry(r2.string(), r2.string(), r2.u8()))));
            return new DescribeConfigs(error, hint, resources);
        }
    }

    /**
     * One node's metrics. Counters run since the node started; rates are the difference of two answers.
     *
     * @param error whether the node answered at all
     * @param nodeId the node, a {@code u32}
     * @param version its version
     * @param uptimeMs milliseconds since it started, a {@code u64}
     * @param metadataGroup the metadata group, 0 if there is none, a {@code u64}
     * @param coordinatorGroup the coordinator group, 0 if there is none, a {@code u64}
     * @param connections open client connections, a {@code u32} (sent as a {@code u64})
     * @param clientInBytes bytes received from clients, a {@code u64}
     * @param clientOutBytes bytes sent to clients, a {@code u64}
     * @param peerInBytes bytes received from other nodes, a {@code u64}
     * @param peerOutBytes bytes sent to other nodes, a {@code u64}
     * @param commitLatency acknowledged produces by latency: see {@link Messages#LATENCY_BUCKETS}, each a {@code u64}
     * @param host the machine
     * @param partitions every Raft group hosted here
     * @param queues queues this node leads
     * @param groups consumer groups, if this node leads the coordinator group
     */
    record DescribeMetrics(
            ErrorCode error,
            long nodeId,
            String version,
            long uptimeMs,
            long metadataGroup,
            long coordinatorGroup,
            long connections,
            long clientInBytes,
            long clientOutBytes,
            long peerInBytes,
            long peerOutBytes,
            List<Long> commitLatency,
            HostMetrics host,
            List<PartitionMetrics> partitions,
            List<QueueMetrics> queues,
            List<GroupMetrics> groups)
            implements Response {
        /** Creates the response. */
        public DescribeMetrics {
            commitLatency = List.copyOf(commitLatency);
            partitions = List.copyOf(partitions);
            queues = List.copyOf(queues);
            groups = List.copyOf(groups);
        }

        @Override
        public void write(Writer w) {
            w.u16(error.code()).u32(nodeId).string(version);
            w.u64(uptimeMs).u64(metadataGroup).u64(coordinatorGroup).u64(connections);
            w.u64(clientInBytes).u64(clientOutBytes).u64(peerInBytes).u64(peerOutBytes);
            w.array(commitLatency, Writer::u64);
            w.bool(host.cpuPermille() != null);
            if (host.cpuPermille() != null) {
                w.u16(host.cpuPermille());
            }
            putOptU64(w, host.memoryBytes());
            putOptU64(w, host.memoryLimitBytes());
            putOptU64(w, host.diskFreeBytes());
            putOptU64(w, host.diskTotalBytes());
            w.array(partitions, (w1, p) -> {
                w1.u64(p.group()).u64(p.resourceId()).u32(p.partition()).bool(p.leader());
                w1.u64(p.term()).u64(p.highWatermark()).u64(p.lastStable()).u64(p.logStart()).u64(p.logEnd());
                w1.u64(p.sizeBytes()).u64(p.produceBytes()).u64(p.produceRecords()).u64(p.fetchBytes());
                w1.array(p.replicas(), (w2, rp) -> w2.u32(rp.nodeId()).u64(rp.logEnd()));
            });
            w.array(queues, (w1, q) -> {
                w1.u64(q.resourceId()).u64(q.ready()).u64(q.inFlight()).u64(q.inOpenTxns());
                w1.u64(q.delivered()).u64(q.acked());
                w1.array(q.consumers(), (w2, c) -> w2.u32(c.subscriptionId())
                        .string(c.peer())
                        .u32(c.inFlight())
                        .u32(c.credit()));
            });
            w.array(groups, (w1, g) -> {
                w1.string(g.groupId()).string(g.tenant()).i32(g.epoch());
                w1.array(g.members(), (w2, m) -> {
                    w2.string(m.memberId());
                    Messages.putTxnResources(w2, m.owned());
                });
                w1.array(g.offsets(), (w2, o) -> w2.u64(o.resourceId()).u32(o.partition()).u64(o.offset()));
            });
            w.noTags();
        }

        private static void putOptU64(Writer w, Long v) {
            w.bool(v != null);
            if (v != null) {
                w.u64(v);
            }
        }

        private static Long getOptU64(Reader r) {
            return r.bool() ? r.u64() : null;
        }

        static DescribeMetrics read(Reader r) {
            ErrorCode error = new ErrorCode(r.u16());
            long nodeId = r.u32();
            String version = r.string();
            long uptime = r.u64();
            long metadataGroup = r.u64();
            long coordinatorGroup = r.u64();
            long connections = r.u64();
            if (!dev.lotse.record.Unsigned.fitsU32(connections)) {
                throw DecodeException.malformed("connections");
            }
            long clientIn = r.u64();
            long clientOut = r.u64();
            long peerIn = r.u64();
            long peerOut = r.u64();
            List<Long> latency = r.array(8, Reader::u64);
            Integer cpu = r.bool() ? r.u16() : null;
            HostMetrics host = new HostMetrics(cpu, getOptU64(r), getOptU64(r), getOptU64(r), getOptU64(r));
            // group, resource, partition, leader, nine counters, an empty array
            List<PartitionMetrics> partitions = r.array(8 + 8 + 4 + 1 + 9 * 8 + 1, r1 -> {
                long group = r1.u64();
                long resource = r1.u64();
                long partition = r1.u32();
                boolean leader = r1.bool();
                long[] v = new long[9];
                for (int i = 0; i < v.length; i++) {
                    v[i] = r1.u64();
                }
                List<ReplicaProgress> replicas = r1.array(12, r2 -> new ReplicaProgress(r2.u32(), r2.u64()));
                return new PartitionMetrics(
                        group, resource, partition, leader, v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7], v[8],
                        replicas);
            });
            // six counters and an empty array
            List<QueueMetrics> queues = r.array(6 * 8 + 1, r1 -> {
                long[] v = new long[6];
                for (int i = 0; i < v.length; i++) {
                    v[i] = r1.u64();
                }
                // id, an empty peer, in flight, credit
                List<ConsumerMetrics> consumers =
                        r1.array(4 + 2 + 4 + 4, r2 -> new ConsumerMetrics(r2.u32(), r2.string(), r2.u32(), r2.u32()));
                return new QueueMetrics(v[0], v[1], v[2], v[3], v[4], v[5], consumers);
            });
            // an empty id and tenant, the epoch, two empty arrays
            List<GroupMetrics> groups = r.array(2 + 2 + 4 + 1 + 1, r1 -> {
                String groupId = r1.string();
                String tenant = r1.string();
                int epoch = r1.i32();
                // an empty id and an empty array
                List<GroupMemberMetrics> members =
                        r1.array(2 + 1, r2 -> new GroupMemberMetrics(r2.string(), Messages.getTxnResources(r2)));
                List<CommittedOffset> offsets =
                        r1.array(8 + 4 + 8, r2 -> new CommittedOffset(r2.u64(), r2.u32(), r2.u64()));
                return new GroupMetrics(groupId, tenant, epoch, members, offsets);
            });
            r.tags();
            return new DescribeMetrics(
                    error, nodeId, version, uptime, metadataGroup, coordinatorGroup, connections, clientIn,
                    clientOut, peerIn, peerOut, latency, host, partitions, queues, groups);
        }
    }

    /**
     * The answer to {@code SaslHandshake}. There is no block of tagged fields.
     *
     * @param error {@link ErrorCode#UNSUPPORTED_SASL_MECHANISM} if the mechanism is not enabled here
     * @param mechanisms the mechanisms this node takes
     */
    record SaslHandshake(ErrorCode error, List<String> mechanisms) implements Response {
        /** Creates the response. */
        public SaslHandshake {
            mechanisms = List.copyOf(mechanisms);
        }

        @Override
        public void write(Writer w) {
            w.u16(error.code()).array(mechanisms, Writer::string);
        }

        static SaslHandshake read(Reader r) {
            return new SaslHandshake(new ErrorCode(r.u16()), r.array(2, Reader::string));
        }
    }

    /**
     * The answer to one {@code SaslAuthenticate} step. There is no block of tagged fields.
     *
     * @param error {@link ErrorCode#AUTHENTICATION_FAILED} ends the attempt
     * @param errorMsg why, for a failure, or {@code null}
     * @param authBytes the mechanism's answer
     * @param sessionLifetimeMs once authenticated: how long the session lasts, in milliseconds, a {@code u64};
     *     authenticate again on the same connection before it ends
     */
    record SaslAuthenticate(ErrorCode error, String errorMsg, ByteString authBytes, long sessionLifetimeMs)
            implements Response {
        /** Creates the response. */
        public SaslAuthenticate {
            Objects.requireNonNull(authBytes);
        }

        @Override
        public void write(Writer w) {
            w.u16(error.code()).optString(errorMsg).bytes(authBytes).u64(sessionLifetimeMs);
        }

        static SaslAuthenticate read(Reader r) {
            return new SaslAuthenticate(new ErrorCode(r.u16()), r.optString(), r.bytes(), r.u64());
        }
    }

    /**
     * The users, sorted by name. The leader hint sits before the users; there is no trailing block.
     *
     * @param error the outcome
     * @param leaderHint the metadata group's leader, with {@link ErrorCode#NOT_LEADER}, or {@code null}
     * @param users the users
     */
    record DescribeUsers(ErrorCode error, LeaderHint leaderHint, List<UserEntry> users) implements Response {
        /** Creates the response. */
        public DescribeUsers {
            users = List.copyOf(users);
        }

        @Override
        public void write(Writer w) {
            w.u16(error.code());
            Messages.putHint(w, leaderHint);
            w.array(users, (w1, u) -> w1.string(u.name()).string(u.mechanism()).u32(u.iterations()).u8(u.source()));
        }

        static DescribeUsers read(Reader r) {
            ErrorCode error = new ErrorCode(r.u16());
            LeaderHint hint = Messages.getHint(r);
            // name, mechanism, iterations, source: 2 + 2 + 4 + 1 bytes
            return new DescribeUsers(
                    error, hint, r.array(9, r1 -> new UserEntry(r1.string(), r1.string(), r1.u32(), r1.u8())));
        }
    }

    /**
     * The client certificate inventory, by expiry. The leader hint sits before the time; there is no trailing block.
     *
     * @param error the outcome
     * @param leaderHint the metadata group's leader, with {@link ErrorCode#NOT_LEADER}, or {@code null}
     * @param nowMs the answering node's clock, milliseconds since the Unix epoch, a {@code u64}, which the states are
     *     computed against
     * @param certificates the certificates
     */
    record DescribeCertificates(
            ErrorCode error, LeaderHint leaderHint, long nowMs, List<CertificateEntry> certificates)
            implements Response {
        /** Creates the response. */
        public DescribeCertificates {
            certificates = List.copyOf(certificates);
        }

        @Override
        public void write(Writer w) {
            w.u16(error.code());
            Messages.putHint(w, leaderHint);
            w.u64(nowMs);
            w.array(certificates, (w1, c) -> w1.bytes(c.issuer())
                    .bytes(c.serial())
                    .string(c.principal())
                    .u64(c.notBeforeMs())
                    .u64(c.notAfterMs())
                    .u64(c.firstSeenMs())
                    .u64(c.lastSeenMs())
                    .u8(c.state())
                    .string(c.reason())
                    .u64(c.revokedAtMs()));
        }

        static DescribeCertificates read(Reader r) {
            ErrorCode error = new ErrorCode(r.u16());
            LeaderHint hint = Messages.getHint(r);
            long nowMs = r.u64();
            // issuer, serial, principal, four u64, state, reason, u64: 4 + 4 + 2 + 4 * 8 + 1 + 2 + 8 bytes at least
            return new DescribeCertificates(
                    error,
                    hint,
                    nowMs,
                    r.array(
                            53,
                            r1 -> new CertificateEntry(
                                    r1.bytes(),
                                    r1.bytes(),
                                    r1.string(),
                                    r1.u64(),
                                    r1.u64(),
                                    r1.u64(),
                                    r1.u64(),
                                    r1.u8(),
                                    r1.string(),
                                    r1.u64())));
        }
    }

    /**
     * The certificate requests, newest first. The leader hint sits before the time; there is no trailing block.
     *
     * @param error the outcome
     * @param leaderHint the metadata group's leader, with {@link ErrorCode#NOT_LEADER}, or {@code null}
     * @param nowMs the answering node's clock, milliseconds since the Unix epoch, a {@code u64}
     * @param requests the requests
     */
    record DescribeCertificateRequests(
            ErrorCode error, LeaderHint leaderHint, long nowMs, List<CertificateRequestEntry> requests)
            implements Response {
        /** Creates the response. */
        public DescribeCertificateRequests {
            requests = List.copyOf(requests);
        }

        @Override
        public void write(Writer w) {
            w.u16(error.code());
            Messages.putHint(w, leaderHint);
            w.u64(nowMs);
            w.array(requests, (w1, c) -> w1.bytes(c.id())
                    .string(c.principal())
                    .bytes(c.csr())
                    .string(c.requestedBy())
                    .u64(c.requestedAtMs())
                    .u64(c.expiresAtMs())
                    .u8(c.state())
                    .bytes(c.certificate())
                    .u64(c.signedAtMs()));
        }

        static DescribeCertificateRequests read(Reader r) {
            ErrorCode error = new ErrorCode(r.u16());
            LeaderHint hint = Messages.getHint(r);
            long nowMs = r.u64();
            // id, principal, csr, requested_by, two u64, state, certificate, u64: 4 + 2 + 4 + 2 + 2 * 8 + 1 + 4 + 8
            return new DescribeCertificateRequests(
                    error,
                    hint,
                    nowMs,
                    r.array(
                            41,
                            r1 -> new CertificateRequestEntry(
                                    r1.bytes(),
                                    r1.string(),
                                    r1.bytes(),
                                    r1.string(),
                                    r1.u64(),
                                    r1.u64(),
                                    r1.u8(),
                                    r1.bytes(),
                                    r1.u64())));
        }
    }

    /**
     * The tenants, sorted by name. The leader hint sits before the tenants; there is no trailing block.
     *
     * @param error the outcome
     * @param leaderHint the metadata group's leader, with {@link ErrorCode#NOT_LEADER}, or {@code null}
     * @param tenants the tenants
     */
    record DescribeTenants(ErrorCode error, LeaderHint leaderHint, List<TenantEntry> tenants) implements Response {
        /** Creates the response. */
        public DescribeTenants {
            tenants = List.copyOf(tenants);
        }

        @Override
        public void write(Writer w) {
            w.u16(error.code());
            Messages.putHint(w, leaderHint);
            w.array(tenants, (w1, t) -> w1.string(t.name()).u8(t.source()).u32(t.resources()));
        }

        static DescribeTenants read(Reader r) {
            ErrorCode error = new ErrorCode(r.u16());
            LeaderHint hint = Messages.getHint(r);
            // name, source, resources: 2 + 1 + 4 bytes
            return new DescribeTenants(error, hint, r.array(7, r1 -> new TenantEntry(r1.string(), r1.u8(), r1.u32())));
        }
    }

    /**
     * The role bindings, each with its source. The leader hint sits before the bindings; there is no trailing
     * block.
     *
     * @param error the outcome
     * @param leaderHint the metadata group's leader, with {@link ErrorCode#NOT_LEADER}, or {@code null}
     * @param bindings the bindings and where each comes from
     */
    record DescribeRoleBindings(ErrorCode error, LeaderHint leaderHint, List<SourcedBinding> bindings)
            implements Response {
        /** Creates the response. */
        public DescribeRoleBindings {
            bindings = List.copyOf(bindings);
        }

        @Override
        public void write(Writer w) {
            w.u16(error.code());
            Messages.putHint(w, leaderHint);
            w.array(bindings, (w1, b) -> {
                RoleBinding.write(w1, b.binding());
                w1.u8(b.source());
            });
        }

        static DescribeRoleBindings read(Reader r) {
            ErrorCode error = new ErrorCode(r.u16());
            LeaderHint hint = Messages.getHint(r);
            return new DescribeRoleBindings(
                    error, hint, r.array(Messages.BINDING_MIN + 1, r1 -> new SourcedBinding(RoleBinding.read(r1), r1.u8())));
        }
    }

    /**
     * Who the connection acts as. There is no block of tagged fields.
     *
     * @param kind see {@link PrincipalKind}
     * @param name the name (the node id for a node)
     * @param groups groups at the identity provider
     * @param bindings the bindings that apply, by name or by group
     */
    record WhoAmI(int kind, String name, List<String> groups, List<RoleBinding> bindings) implements Response {
        /** Creates the response. */
        public WhoAmI {
            groups = List.copyOf(groups);
            bindings = List.copyOf(bindings);
        }

        @Override
        public void write(Writer w) {
            w.u8(kind).string(name).array(groups, Writer::string).array(bindings, RoleBinding::write);
        }

        static WhoAmI read(Reader r) {
            return new WhoAmI(
                    r.u8(), r.string(), r.array(2, Reader::string), r.array(Messages.BINDING_MIN, RoleBinding::read));
        }
    }

    /**
     * The cluster's nodes, from the metadata group's leader. The leader hint sits before the nodes; there is no
     * trailing block.
     *
     * @param error the outcome
     * @param leaderHint the metadata group's leader, with {@link ErrorCode#NOT_LEADER}, or {@code null}
     * @param nodes the nodes, by id
     */
    record DescribeNodes(ErrorCode error, LeaderHint leaderHint, List<NodeEntry> nodes) implements Response {
        /** Creates the response. */
        public DescribeNodes {
            nodes = List.copyOf(nodes);
        }

        @Override
        public void write(Writer w) {
            w.u16(error.code());
            Messages.putHint(w, leaderHint);
            w.array(nodes, (w1, n) ->
                    w1.u64(n.nodeId()).string(n.peer()).string(n.client()).u8(n.state()).u32(n.replicas()).u32(n.moving()));
        }

        static DescribeNodes read(Reader r) {
            ErrorCode error = new ErrorCode(r.u16());
            LeaderHint hint = Messages.getHint(r);
            return new DescribeNodes(
                    error,
                    hint,
                    r.array(30, r1 -> new NodeEntry(r1.u64(), r1.string(), r1.string(), r1.u8(), r1.u32(), r1.u32())));
        }
    }

    /**
     * The node's answer to {@code Produce}.
     *
     * @param throttleMs how long the client should wait before its next request, a {@code u32}
     * @param resources per resource
     */
    record Produce(long throttleMs, List<ProduceResourceResponse> resources) implements Response {
        /** Creates the response. */
        public Produce {
            resources = List.copyOf(resources);
        }

        @Override
        public void write(Writer w) {
            w.u32(throttleMs);
            w.array(resources, (w1, res) -> {
                w1.u64(res.resourceId());
                w1.array(res.partitions(), (w2, p) -> {
                    w2.u32(p.partition()).u16(p.error().code()).u64(p.baseOffset()).i64(p.appendTs());
                    w2.optString(p.errorMsg());
                    Messages.putHint(w2, p.leaderHint());
                });
                w1.noTags();
            });
            w.noTags();
        }

        static Produce read(Reader r) {
            long throttle = r.u32();
            List<ProduceResourceResponse> resources = r.array(10, r1 -> {
                long id = r1.u64();
                List<ProducePartitionResponse> partitions = r1.array(25, r2 -> new ProducePartitionResponse(
                        r2.u32(), new ErrorCode(r2.u16()), r2.u64(), r2.i64(), r2.optString(), Messages.getHint(r2)));
                r1.tags();
                return new ProduceResourceResponse(id, partitions);
            });
            r.tags();
            return new Produce(throttle, resources);
        }
    }

    /**
     * The node's answer to {@code ListOffsets}.
     *
     * @param resources per stream, in the order asked
     */
    record ListOffsets(List<ListOffsetsResourceResponse> resources) implements Response {
        /** Creates the response. */
        public ListOffsets {
            resources = List.copyOf(resources);
        }

        @Override
        public void write(Writer w) {
            w.array(resources, (w1, res) -> {
                w1.u64(res.resourceId());
                w1.array(res.partitions(), (w2, p) -> {
                    w2.u32(p.partition()).u16(p.error().code());
                    w2.u64(p.offset()).i64(p.timestamp());
                    Messages.putHint(w2, p.leaderHint());
                });
                w1.noTags();
            });
            w.noTags();
        }

        static ListOffsets read(Reader r) {
            List<ListOffsetsResourceResponse> resources = r.array(10, r1 -> {
                long id = r1.u64();
                List<ListOffsetsPartitionResponse> partitions = r1.array(23, r2 -> new ListOffsetsPartitionResponse(
                        r2.u32(), new ErrorCode(r2.u16()), r2.u64(), r2.i64(), Messages.getHint(r2)));
                r1.tags();
                return new ListOffsetsResourceResponse(id, partitions);
            });
            r.tags();
            return new ListOffsets(resources);
        }
    }

    /**
     * The node's answer to {@code Fetch}.
     *
     * @param throttleMs how long the client should wait before its next request, a {@code u32}
     * @param resources per resource
     */
    record Fetch(long throttleMs, List<FetchResourceResponse> resources) implements Response {
        /** Creates the response. */
        public Fetch {
            resources = List.copyOf(resources);
        }

        @Override
        public void write(Writer w) {
            w.u32(throttleMs);
            w.array(resources, (w1, res) -> {
                w1.u64(res.resourceId());
                w1.array(res.partitions(), (w2, p) -> {
                    w2.u32(p.partition()).u16(p.error().code());
                    w2.u64(p.highWatermark()).u64(p.lastStableOffset()).u64(p.logStartOffset());
                    w2.array(p.abortedTxns(), (w3, a) -> w3.u64(a.producerId()).u64(a.firstOffset()));
                    w2.bytes(p.records());
                    Messages.putHint(w2, p.leaderHint());
                });
                w1.noTags();
            });
            w.noTags();
        }

        static Fetch read(Reader r) {
            long throttle = r.u32();
            List<FetchResourceResponse> resources = r.array(10, r1 -> {
                long id = r1.u64();
                List<FetchPartitionResponse> partitions = r1.array(36, r2 -> new FetchPartitionResponse(
                        r2.u32(),
                        new ErrorCode(r2.u16()),
                        r2.u64(),
                        r2.u64(),
                        r2.u64(),
                        r2.array(16, r3 -> new AbortedTxn(r3.u64(), r3.u64())),
                        r2.bytes(),
                        Messages.getHint(r2)));
                r1.tags();
                return new FetchResourceResponse(id, partitions);
            });
            r.tags();
            return new Fetch(throttle, resources);
        }
    }

    /**
     * The request could not be served at all: an unsupported version
     * ({@link ErrorCode#UNSUPPORTED_VERSION}), or a connection not authenticated or whose session ended
     * ({@link ErrorCode#UNAUTHORIZED}). Sent with the {@link Flags#ERROR} flag, its body is just the code and an
     * empty block of tagged fields.
     *
     * @param code why
     */
    record Error(ErrorCode code) implements Response {
        @Override
        public void write(Writer w) {
            w.u16(code.code()).noTags();
        }
    }

    /**
     * The request was refused as a whole, with a reason: the principal lacks a role for it
     * ({@link ErrorCode#UNAUTHORIZED}). Sent like {@link Error} with the reason, as UTF-8, in tagged field 0.
     *
     * @param code why, as a code
     * @param message why, for people
     */
    record Refused(ErrorCode code, String message) implements Response {
        @Override
        public void write(Writer w) {
            w.u16(code.code());
            w.tags(List.of(new Tag(Messages.TAG_REASON, ByteString.utf8(message))));
        }
    }
}
