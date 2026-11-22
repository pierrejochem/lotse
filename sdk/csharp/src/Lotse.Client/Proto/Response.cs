using System.Collections.Generic;
using System.Linq;
using Lotse.Record;
using static Lotse.Proto.Messages;

namespace Lotse.Proto;

/// <summary>
/// The responses a node sends. A response has no API key on the wire: <see cref="Frames.DecodeResponse"/> is told
/// which request it answers. A response with the error flag is an <see cref="Failed"/> or a <see cref="Refused"/>,
/// whatever the request was.
/// </summary>
public abstract record Response
{
    private protected Response()
    {
    }

    /// <summary>Writes the body, the part after the frame header.</summary>
    public abstract void Write(Writer w);

    /// <summary>The versions and features of the node.</summary>
    /// <param name="Versions">The versions per API key.</param>
    /// <param name="Features">Named features.</param>
    public sealed record ApiVersions(IReadOnlyList<ApiVersionRange> Versions, IReadOnlyList<string> Features) : Response
    {
        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.Array(Versions, (w1, v) => w1.U16(v.ApiKey.Id).U16(v.Min).U16(v.Max));
            w.Array(Features, (w1, f) => w1.String(f));
            w.NoTags();
        }

        internal static ApiVersions Read(Reader r)
        {
            List<ApiVersionRange> versions = r.Array(6, r1 => new ApiVersionRange(new ApiKey(r1.U16()), r1.U16(), r1.U16()));
            List<string> features = r.Array(2, r1 => r1.String());
            r.Tags();
            return new ApiVersions(versions, features);
        }
    }

    /// <summary>Brokers and resources.</summary>
    /// <param name="Brokers">The nodes.</param>
    /// <param name="Resources">The resources asked for; an unknown name is a resource with an error.</param>
    public sealed record Metadata(IReadOnlyList<Broker> Brokers, IReadOnlyList<ResourceMetadata> Resources) : Response
    {
        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.Array(Brokers, (w1, b) =>
            {
                w1.U32(b.NodeId).String(b.Host).U16(b.Port).String(b.Rack);
                w1.NoTags();
            });
            w.Array(Resources, (w1, res) =>
            {
                w1.U16(res.Error.Code).String(res.Name).String(res.Tenant);
                w1.U64(res.ResourceId).U8(res.Kind);
                w1.Array(res.Partitions, (w2, p) =>
                {
                    w2.U32(p.Partition).U32(p.Leader).U64(p.LeaderTerm);
                    w2.Array(p.Voters, (w3, v) => w3.U32(v));
                    var tags = new List<Tag>();
                    if (p.PinnedLeader != 0)
                    {
                        tags.Add(new Tag(TagPinnedLeader, ByteString.CopyOf(new Writer().U32(p.PinnedLeader).ToByteArray())));
                    }
                    if (p.Placed)
                    {
                        tags.Add(new Tag(TagPlaced, ByteString.CopyOf(new byte[0])));
                    }
                    w2.Tags(tags);
                });
                w1.Bool(res.Exchange is not null);
                if (res.Exchange is not null)
                {
                    w1.U8(res.Exchange.ExchangeType);
                    w1.Array(res.Exchange.Bindings, (w2, b) =>
                    {
                        w2.U64(b.QueueId).String(b.BindingKey);
                        w2.Array(b.HeaderMatch, HeaderMatch.Write);
                        w2.Bool(b.MatchAll);
                        w2.NoTags();
                    });
                }
                w1.NoTags();
            });
            w.NoTags();
        }

        internal static Metadata Read(Reader r)
        {
            // node id, host, port, rack, no tags
            List<Broker> brokers = r.Array(11, r1 =>
            {
                var b = new Broker(r1.U32(), r1.String(), r1.U16(), r1.String());
                r1.Tags();
                return b;
            });
            // error, name, tenant, id, kind, no partitions, no exchange, no tags
            List<ResourceMetadata> resources = r.Array(18, r1 =>
            {
                var error = new ErrorCode(r1.U16());
                string name = r1.String();
                string tenant = r1.String();
                ulong id = r1.U64();
                byte kind = r1.U8();
                List<PartitionMetadata> partitions = r1.Array(18, r2 =>
                {
                    uint partition = r2.U32();
                    uint leader = r2.U32();
                    ulong term = r2.U64();
                    List<uint> voters = r2.Array(4, r3 => r3.U32());
                    uint pinned = 0;
                    bool placed = false;
                    foreach (Tag t in r2.Tags())
                    {
                        if (t.Number == TagPinnedLeader)
                        {
                            var v = new Reader(t.Value.ToByteArray());
                            pinned = v.U32();
                            v.Finish();
                        }
                        else if (t.Number == TagPlaced)
                        {
                            placed = true;
                        }
                    }
                    return new PartitionMetadata(partition, leader, term, voters, pinned, placed);
                });
                ExchangeMetadata? exchange = null;
                if (r1.Bool())
                {
                    exchange = new ExchangeMetadata(r1.U8(), r1.Array(13, r2 =>
                    {
                        var b = new BindingMetadata(r2.U64(), r2.String(), r2.Array(5, HeaderMatch.Read), r2.Bool());
                        r2.Tags();
                        return b;
                    }));
                }
                r1.Tags();
                return new ResourceMetadata(error, name, tenant, id, kind, partitions, exchange);
            });
            r.Tags();
            return new Metadata(brokers, resources);
        }
    }

    /// <summary>The node that coordinates a group or producer.</summary>
    /// <param name="Error">The outcome.</param>
    /// <param name="NodeId">The coordinator.</param>
    /// <param name="Host">Its host.</param>
    /// <param name="Port">Its port.</param>
    /// <param name="ErrorMsg">Details for humans, or <c>null</c>.</param>
    public sealed record FindCoordinator(ErrorCode Error, uint NodeId, string Host, ushort Port, string? ErrorMsg) : Response
    {
        /// <inheritdoc/>
        public override void Write(Writer w) => w.U16(Error.Code).U32(NodeId).String(Host).U16(Port).OptString(ErrorMsg).NoTags();

        internal static FindCoordinator Read(Reader r)
        {
            var x = new FindCoordinator(new ErrorCode(r.U16()), r.U32(), r.String(), r.U16(), r.OptString());
            r.Tags();
            return x;
        }
    }

    /// <summary>A producer id and epoch.</summary>
    /// <param name="Error">The outcome.</param>
    /// <param name="ProducerId">The producer.</param>
    /// <param name="ProducerEpoch">Its epoch.</param>
    /// <param name="LeaderHint">The coordinator's leader, after <c>NOT_LEADER</c>.</param>
    public sealed record InitProducer(ErrorCode Error, ulong ProducerId, ushort ProducerEpoch, LeaderHint? LeaderHint) : Response
    {
        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.U16(Error.Code).U64(ProducerId).U16(ProducerEpoch);
            PutHint(w, LeaderHint);
        }

        internal static InitProducer Read(Reader r) => new(new ErrorCode(r.U16()), r.U64(), r.U16(), GetHint(r));
    }

    /// <summary>Outcomes of adding partitions to a transaction.</summary>
    /// <param name="Resources">Per resource and partition.</param>
    public sealed record AddPartitionsToTxn(IReadOnlyList<TxnResourceResult> Resources) : Response
    {
        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            PutTxnResults(w, Resources);
            w.NoTags();
        }

        internal static AddPartitionsToTxn Read(Reader r)
        {
            var x = new AddPartitionsToTxn(GetTxnResults(r));
            r.Tags();
            return x;
        }
    }

    /// <summary>The outcome of ending a transaction.</summary>
    /// <param name="Error">The outcome.</param>
    /// <param name="ProducerId">The producer.</param>
    /// <param name="ProducerEpoch">The epoch for the next transaction.</param>
    public sealed record EndTxn(ErrorCode Error, ulong ProducerId, ushort ProducerEpoch) : Response
    {
        /// <inheritdoc/>
        public override void Write(Writer w) => w.U16(Error.Code).U64(ProducerId).U16(ProducerEpoch).NoTags();

        internal static EndTxn Read(Reader r)
        {
            var x = new EndTxn(new ErrorCode(r.U16()), r.U64(), r.U16());
            r.Tags();
            return x;
        }
    }

    /// <summary>The outcome of subscribing to a queue.</summary>
    /// <param name="Error">The outcome.</param>
    /// <param name="SubscriptionId">The subscription, per node and connection.</param>
    /// <param name="LeaderHint">The queue's leader, after <c>NOT_LEADER</c>.</param>
    /// <param name="Standby">The subscription waits as a standby of a single active consumer (ADR 0076).</param>
    /// <param name="ConsumerId">The consumer's id, to name when it subscribes again (ADR 0109).</param>
    public sealed record Subscribe(
        ErrorCode Error, uint SubscriptionId, LeaderHint? LeaderHint, bool Standby = false, ulong ConsumerId = 0) : Response
    {
        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.U16(Error.Code).U32(SubscriptionId);
            var tags = new List<Tag>();
            if (LeaderHint is not null)
            {
                var v = new Writer();
                v.U32(LeaderHint.Leader).U64(LeaderHint.Term);
                tags.Add(new Tag(TagLeaderHint, ByteString.CopyOf(v.ToByteArray())));
            }
            if (Standby)
            {
                tags.Add(new Tag(TagStandby, ByteString.CopyOf(new byte[] { 1 })));
            }
            if (ConsumerId != 0)
            {
                var v = new Writer();
                v.U64(ConsumerId);
                tags.Add(new Tag(TagConsumer, ByteString.CopyOf(v.ToByteArray())));
            }
            if (tags.Count == 0)
            {
                w.NoTags();
            }
            else
            {
                w.Tags(tags);
            }
        }

        internal static Subscribe Read(Reader r)
        {
            var error = new ErrorCode(r.U16());
            uint id = r.U32();
            LeaderHint? hint = null;
            bool standby = false;
            ulong consumerId = 0;
            foreach (Tag t in r.Tags())
            {
                var vr = new Reader(t.Value.ToByteArray());
                if (t.Number == TagLeaderHint)
                {
                    hint = new LeaderHint(vr.U32(), vr.U64());
                }
                else if (t.Number == TagStandby)
                {
                    standby = vr.U8() != 0;
                }
                else if (t.Number == TagConsumer)
                {
                    consumerId = vr.U64();
                }
                else
                {
                    continue; // from a newer version: skipped
                }
                vr.Finish();
            }
            return new Subscribe(error, id, hint, standby, consumerId);
        }
    }

    /// <summary>What became of published messages.</summary>
    /// <param name="ThrottleMs">How long the client should hold back.</param>
    /// <param name="Entries">One result per message, in order.</param>
    public sealed record Publish(uint ThrottleMs, IReadOnlyList<PublishResult> Entries) : Response
    {
        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.U32(ThrottleMs);
            w.Array(Entries, (w1, e) =>
            {
                w1.U16(e.Error.Code);
                w1.Array(e.Routed, (w2, q) => w2.U64(q.QueueId).String(q.Name).U16(q.Error.Code).U64(q.BaseOffset));
                w1.NoTags();
            });
            w.NoTags();
        }

        internal static Publish Read(Reader r)
        {
            uint throttle = r.U32();
            List<PublishResult> entries = r.Array(4, r1 =>
            {
                var error = new ErrorCode(r1.U16());
                List<RoutedMessage> routed =
                    r1.Array(20, r2 => new RoutedMessage(r2.U64(), r2.String(), new ErrorCode(r2.U16()), r2.U64()));
                r1.Tags();
                return new PublishResult(error, routed);
            });
            r.Tags();
            return new Publish(throttle, entries);
        }
    }

    /// <summary>The answer to Ack, Nack, Unsubscribe, Bind and Unbind, and to a dead-letter forward.</summary>
    /// <param name="Error">The outcome.</param>
    /// <param name="LeaderHint">The leader, after <c>NOT_LEADER</c>.</param>
    public sealed record Outcome(ErrorCode Error, LeaderHint? LeaderHint) : Response
    {
        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.U16(Error.Code);
            PutHint(w, LeaderHint);
        }

        internal static Outcome Read(Reader r) => new(new ErrorCode(r.U16()), GetHint(r));
    }

    /// <summary>A member's place in a consumer group.</summary>
    /// <param name="ThrottleMs">How long the client should hold back.</param>
    /// <param name="Error">The outcome.</param>
    /// <param name="MemberId">The member.</param>
    /// <param name="MemberEpoch">Its epoch.</param>
    /// <param name="HeartbeatIntervalMs">How often to heartbeat.</param>
    /// <param name="Assignment">The partitions the member should own now, or <c>null</c> if unchanged.</param>
    public sealed record GroupHeartbeat(
        uint ThrottleMs,
        ErrorCode Error,
        string MemberId,
        int MemberEpoch,
        uint HeartbeatIntervalMs,
        IReadOnlyList<TxnResource>? Assignment) : Response
    {
        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.U32(ThrottleMs).U16(Error.Code).String(MemberId).I32(MemberEpoch).U32(HeartbeatIntervalMs);
            PutOptResources(w, Assignment);
            w.NoTags();
        }

        internal static GroupHeartbeat Read(Reader r)
        {
            var x = new GroupHeartbeat(
                r.U32(), new ErrorCode(r.U16()), r.String(), r.I32(), r.U32(), GetOptResources(r));
            r.Tags();
            return x;
        }
    }

    /// <summary>The answer to CommitOffsets and AddOffsetsToTxn.</summary>
    /// <param name="Resources">Per resource and partition.</param>
    public sealed record CommitOffsets(IReadOnlyList<TxnResourceResult> Resources) : Response
    {
        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            PutTxnResults(w, Resources);
            w.NoTags();
        }

        internal static CommitOffsets Read(Reader r)
        {
            var x = new CommitOffsets(GetTxnResults(r));
            r.Tags();
            return x;
        }
    }

    /// <summary>A group's committed offsets.</summary>
    /// <param name="Resources">Per resource and partition.</param>
    public sealed record OffsetFetch(IReadOnlyList<OffsetFetchResource> Resources) : Response
    {
        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.Array(Resources, (w1, r) =>
            {
                w1.U64(r.ResourceId);
                w1.Array(r.Partitions, (w2, p) =>
                {
                    w2.U32(p.Partition).U16(p.Error.Code).U64(p.Offset).U64(p.LeaderTerm);
                    w2.OptString(p.Metadata);
                    w2.NoTags();
                });
                w1.NoTags();
            });
            w.NoTags();
        }

        internal static OffsetFetch Read(Reader r)
        {
            List<OffsetFetchResource> resources = r.Array(10, r1 =>
            {
                ulong id = r1.U64();
                List<OffsetFetchPartition> partitions = r1.Array(25, r2 =>
                {
                    var p = new OffsetFetchPartition(r2.U32(), new ErrorCode(r2.U16()), r2.U64(), r2.U64(), r2.OptString());
                    r2.Tags();
                    return p;
                });
                r1.Tags();
                return new OffsetFetchResource(id, partitions);
            });
            r.Tags();
            return new OffsetFetch(resources);
        }
    }

    /// <summary>The answer to CreateStream, CreateQueue and DeclareExchange.</summary>
    /// <param name="Error">The outcome; <c>RESOURCE_EXISTS</c> names the existing resource's id.</param>
    /// <param name="ResourceId">The resource.</param>
    /// <param name="ErrorMsg">Details for humans, or <c>null</c>.</param>
    /// <param name="LeaderHint">The metadata leader, after <c>NOT_LEADER</c>.</param>
    public sealed record Create(ErrorCode Error, ulong ResourceId, string? ErrorMsg, LeaderHint? LeaderHint) : Response
    {
        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.U16(Error.Code).U64(ResourceId).OptString(ErrorMsg);
            PutHint(w, LeaderHint);
        }

        internal static Create Read(Reader r) => new(new ErrorCode(r.U16()), r.U64(), r.OptString(), GetHint(r));
    }

    /// <summary>The answer to the admin requests that return only an outcome.</summary>
    /// <param name="Error">The outcome.</param>
    /// <param name="ErrorMsg">Details for humans, or <c>null</c>.</param>
    /// <param name="LeaderHint">The metadata leader, after <c>NOT_LEADER</c>.</param>
    public sealed record Admin(ErrorCode Error, string? ErrorMsg, LeaderHint? LeaderHint) : Response
    {
        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.U16(Error.Code).OptString(ErrorMsg);
            PutHint(w, LeaderHint);
        }

        internal static Admin Read(Reader r) => new(new ErrorCode(r.U16()), r.OptString(), GetHint(r));
    }

    /// <summary>The answer to PurgeQueue and Requeue.</summary>
    /// <param name="Error">The outcome.</param>
    /// <param name="ErrorMsg">Details for humans, or <c>null</c>.</param>
    /// <param name="Count">How many messages this round moved.</param>
    /// <param name="UpTo">Where the next round continues.</param>
    /// <param name="More">Whether another round is needed.</param>
    /// <param name="LeaderHint">The queue's leader, after <c>NOT_LEADER</c>.</param>
    public sealed record QueueAction(ErrorCode Error, string? ErrorMsg, ulong Count, ulong UpTo, bool More, LeaderHint? LeaderHint)
        : Response
    {
        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.U16(Error.Code).OptString(ErrorMsg).U64(Count).U64(UpTo).Bool(More);
            PutHint(w, LeaderHint);
        }

        internal static QueueAction Read(Reader r) =>
            new(new ErrorCode(r.U16()), r.OptString(), r.U64(), r.U64(), r.Bool(), GetHint(r));
    }

    /// <summary>Resources' settings.</summary>
    /// <param name="Error">The outcome.</param>
    /// <param name="LeaderHint">The metadata leader, after <c>NOT_LEADER</c>.</param>
    /// <param name="Resources">Per resource.</param>
    public sealed record DescribeConfigs(ErrorCode Error, LeaderHint? LeaderHint, IReadOnlyList<ResourceConfigs> Resources)
        : Response
    {
        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.U16(Error.Code);
            PutHint(w, LeaderHint);
            w.Array(Resources, (w1, r) =>
            {
                w1.U64(r.ResourceId).U16(r.Error.Code);
                w1.Array(r.Configs, (w2, c) => w2.String(c.Key).String(c.Value).U8(c.Source));
            });
        }

        internal static DescribeConfigs Read(Reader r)
        {
            var error = new ErrorCode(r.U16());
            LeaderHint? hint = GetHint(r);
            // resource id, error, an empty settings array: 8 + 2 + 1 bytes
            List<ResourceConfigs> resources = r.Array(11, r1 => new ResourceConfigs(
                r1.U64(),
                new ErrorCode(r1.U16()),
                r1.Array(5, r2 => new ConfigEntry(r2.String(), r2.String(), r2.U8()))));
            return new DescribeConfigs(error, hint, resources);
        }
    }

    /// <summary>One node's metrics.</summary>
    /// <remarks><c>Disk</c> is the node's free space limit and whether it is below it, <c>null</c> while it cannot tell (ADR 0122).</remarks>
    public sealed record DescribeMetrics(
        ErrorCode Error,
        uint NodeId,
        string Version,
        ulong UptimeMs,
        ulong MetadataGroup,
        ulong CoordinatorGroup,
        ulong Connections,
        ulong ClientInBytes,
        ulong ClientOutBytes,
        ulong PeerInBytes,
        ulong PeerOutBytes,
        IReadOnlyList<ulong> CommitLatency,
        HostMetrics Host,
        IReadOnlyList<PartitionMetrics> Partitions,
        IReadOnlyList<QueueMetrics> Queues,
        IReadOnlyList<GroupMetrics> Groups,
        DiskMetrics? Disk = null) : Response
    {
        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.U16(Error.Code).U32(NodeId).String(Version);
            w.U64(UptimeMs).U64(MetadataGroup).U64(CoordinatorGroup).U64(Connections);
            w.U64(ClientInBytes).U64(ClientOutBytes).U64(PeerInBytes).U64(PeerOutBytes);
            w.Array(CommitLatency, (w1, v) => w1.U64(v));
            w.Bool(Host.CpuPermille is not null);
            if (Host.CpuPermille is not null)
            {
                w.U16(Host.CpuPermille.Value);
            }
            PutOptU64(w, Host.MemoryBytes);
            PutOptU64(w, Host.MemoryLimitBytes);
            PutOptU64(w, Host.DiskFreeBytes);
            PutOptU64(w, Host.DiskTotalBytes);
            w.Array(Partitions, (w1, p) =>
            {
                w1.U64(p.Group).U64(p.ResourceId).U32(p.Partition).Bool(p.Leader);
                w1.U64(p.Term).U64(p.HighWatermark).U64(p.LastStable).U64(p.LogStart).U64(p.LogEnd);
                w1.U64(p.SizeBytes).U64(p.ProduceBytes).U64(p.ProduceRecords).U64(p.FetchBytes);
                w1.Array(p.Replicas, (w2, rp) => w2.U32(rp.NodeId).U64(rp.LogEnd));
            });
            w.Array(Queues, (w1, q) =>
            {
                w1.U64(q.ResourceId).U64(q.Ready).U64(q.InFlight).U64(q.InOpenTxns);
                w1.U64(q.Delivered).U64(q.Acked);
                w1.Array(q.Consumers, (w2, c) => w2.U32(c.SubscriptionId).String(c.Peer).U32(c.InFlight).U32(c.Credit));
            });
            var details = Queues
                .Where(q => q.ReadyByLevel.Count > 0
                    || q.ReadyLevelUnread != 0
                    || q.Consumers.Any(c => c.State != ConsumerState.Shared))
                .ToList();
            w.Array(Groups, (w1, g) =>
            {
                w1.String(g.GroupId).String(g.Tenant).I32(g.Epoch);
                w1.Array(g.Members, (w2, m) =>
                {
                    w2.String(m.MemberId);
                    PutTxnResources(w2, m.Owned);
                });
                w1.Array(g.Offsets, (w2, o) => w2.U64(o.ResourceId).U32(o.Partition).U64(o.Offset));
            });
            var tags = new List<Tag>();
            if (details.Count > 0)
            {
                var v = new Writer();
                v.Array(details, (w1, q) =>
                {
                    w1.U64(q.ResourceId);
                    w1.Array(q.ReadyByLevel, (w2, n) => w2.U64(n));
                    w1.U64(q.ReadyLevelUnread);
                    w1.Array(q.Consumers, (w2, c) => w2.U32(c.SubscriptionId).U8((byte)c.State));
                });
                tags.Add(new Tag(TagQueueDetails, ByteString.CopyOf(v.ToByteArray())));
            }
            var parts = Partitions.Where(p => p.OldestTimestampMs is not null || p.OpenTransactions != 0).ToList();
            if (parts.Count > 0)
            {
                var v = new Writer();
                v.Array(parts, (w1, p) => w1.U64(p.Group).I64(p.OldestTimestampMs ?? -1).U32(p.OpenTransactions));
                tags.Add(new Tag(TagPartitionDetails, ByteString.CopyOf(v.ToByteArray())));
            }
            var holding = Queues.Where(q => q.Consumers.Any(c => c.InFlightBytes != 0)).ToList();
            if (holding.Count > 0)
            {
                var v = new Writer();
                v.Array(holding, (w1, q) =>
                {
                    w1.U64(q.ResourceId);
                    w1.Array(q.Consumers, (w2, c) => w2.U32(c.SubscriptionId).U64(c.InFlightBytes));
                });
                tags.Add(new Tag(TagInFlightBytes, ByteString.CopyOf(v.ToByteArray())));
            }
            if (Disk is not null)
            {
                var v = new Writer();
                v.U64(Disk.FreeLimitBytes).Bool(Disk.Low);
                tags.Add(new Tag(TagDisk, ByteString.CopyOf(v.ToByteArray())));
            }
            if (tags.Count == 0)
            {
                w.NoTags();
            }
            else
            {
                w.Tags(tags);
            }
        }

        /// <summary><paramref name="q"/> with what tag 3 says of it, if anything.</summary>
        private static QueueMetrics WithDetails(QueueMetrics q, List<QueueMetrics> details)
        {
            foreach (QueueMetrics d in details)
            {
                if (d.ResourceId != q.ResourceId)
                {
                    continue;
                }
                var consumers = q.Consumers
                    .Select(c => d.Consumers.FirstOrDefault(x => x.SubscriptionId == c.SubscriptionId) is { } found
                        ? c with { State = found.State }
                        : c)
                    .ToList();
                return q with
                {
                    Consumers = consumers,
                    ReadyByLevel = d.ReadyByLevel,
                    ReadyLevelUnread = d.ReadyLevelUnread,
                };
            }
            return q;
        }

        /// <summary><paramref name="q"/> with what tag 5 says of its consumers, if anything.</summary>
        private static QueueMetrics WithBytes(QueueMetrics q, List<QueueMetrics> held)
        {
            foreach (QueueMetrics h in held)
            {
                if (h.ResourceId != q.ResourceId)
                {
                    continue;
                }
                var consumers = q.Consumers
                    .Select(c => h.Consumers.FirstOrDefault(x => x.SubscriptionId == c.SubscriptionId) is { } found
                        ? c with { InFlightBytes = found.InFlightBytes }
                        : c)
                    .ToList();
                return q with { Consumers = consumers };
            }
            return q;
        }

        private static ConsumerState StateOf(byte code) => code switch
        {
            1 => ConsumerState.Active,
            2 => ConsumerState.Standby,
            _ => ConsumerState.Shared,
        };

        private static void PutOptU64(Writer w, ulong? v)
        {
            w.Bool(v is not null);
            if (v is not null)
            {
                w.U64(v.Value);
            }
        }

        private static ulong? GetOptU64(Reader r) => r.Bool() ? r.U64() : null;

        internal static DescribeMetrics Read(Reader r)
        {
            var error = new ErrorCode(r.U16());
            uint nodeId = r.U32();
            string version = r.String();
            ulong uptime = r.U64();
            ulong metadataGroup = r.U64();
            ulong coordinatorGroup = r.U64();
            ulong connections = r.U64();
            if (connections > uint.MaxValue)
            {
                throw DecodeException.Malformed("connections");
            }
            ulong clientIn = r.U64();
            ulong clientOut = r.U64();
            ulong peerIn = r.U64();
            ulong peerOut = r.U64();
            List<ulong> latency = r.Array(8, r1 => r1.U64());
            ushort? cpu = r.Bool() ? r.U16() : null;
            var host = new HostMetrics(cpu, GetOptU64(r), GetOptU64(r), GetOptU64(r), GetOptU64(r));
            // group, resource, partition, leader, nine counters, an empty array
            List<PartitionMetrics> partitions = r.Array(8 + 8 + 4 + 1 + 9 * 8 + 1, r1 =>
            {
                ulong group = r1.U64();
                ulong resource = r1.U64();
                uint partition = r1.U32();
                bool leader = r1.Bool();
                var v = new ulong[9];
                for (int i = 0; i < v.Length; i++)
                {
                    v[i] = r1.U64();
                }
                List<ReplicaProgress> replicas = r1.Array(12, r2 => new ReplicaProgress(r2.U32(), r2.U64()));
                return new PartitionMetrics(
                    group, resource, partition, leader, v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7], v[8], replicas);
            });
            // six counters and an empty array
            List<QueueMetrics> queues = r.Array(6 * 8 + 1, r1 =>
            {
                var v = new ulong[6];
                for (int i = 0; i < v.Length; i++)
                {
                    v[i] = r1.U64();
                }
                // id, an empty peer, in flight, credit
                List<ConsumerMetrics> consumers =
                    r1.Array(4 + 2 + 4 + 4, r2 => new ConsumerMetrics(r2.U32(), r2.String(), r2.U32(), r2.U32()));
                return new QueueMetrics(v[0], v[1], v[2], v[3], v[4], v[5], consumers, System.Array.Empty<ulong>(), 0);
            });
            // an empty id and tenant, the epoch, two empty arrays
            List<GroupMetrics> groups = r.Array(2 + 2 + 4 + 1 + 1, r1 =>
            {
                string groupId = r1.String();
                string tenant = r1.String();
                int epoch = r1.I32();
                // an empty id and an empty array
                List<GroupMemberMetrics> members =
                    r1.Array(2 + 1, r2 => new GroupMemberMetrics(r2.String(), GetTxnResources(r2)));
                List<CommittedOffset> offsets =
                    r1.Array(8 + 4 + 8, r2 => new CommittedOffset(r2.U64(), r2.U32(), r2.U64()));
                return new GroupMetrics(groupId, tenant, epoch, members, offsets);
            });
            DiskMetrics? disk = null;
            foreach (Tag t in r.Tags())
            {
                var vr = new Reader(t.Value.ToByteArray());
                if (t.Number == TagQueueDetails)
                {
                    // id, an empty array, unread, an empty array
                    List<QueueMetrics> details = vr.Array(8 + 1 + 8 + 1, r1 =>
                    {
                        ulong id = r1.U64();
                        List<ulong> levels = r1.Array(8, r2 => r2.U64());
                        ulong unread = r1.U64();
                        List<ConsumerMetrics> states =
                            r1.Array(4 + 1, r2 => new ConsumerMetrics(r2.U32(), "", 0, 0, StateOf(r2.U8())));
                        return new QueueMetrics(id, 0, 0, 0, 0, 0, states, levels, unread);
                    });
                    queues = queues.Select(q => WithDetails(q, details)).ToList();
                }
                else if (t.Number == TagPartitionDetails)
                {
                    // group, oldest, open transactions
                    var details = vr.Array(8 + 8 + 4, r1 => (Group: r1.U64(), Oldest: r1.I64(), Open: r1.U32()));
                    partitions = partitions.Select(p =>
                    {
                        foreach (var d in details)
                        {
                            if (d.Group == p.Group)
                            {
                                long? oldest = d.Oldest >= 0 ? d.Oldest : null;
                                return p with { OldestTimestampMs = oldest, OpenTransactions = d.Open };
                            }
                        }
                        return p;
                    }).ToList();
                }
                else if (t.Number == TagInFlightBytes)
                {
                    // id and an empty array
                    List<QueueMetrics> held = vr.Array(8 + 1, r1 =>
                    {
                        ulong id = r1.U64();
                        List<ConsumerMetrics> bytes =
                            r1.Array(4 + 8, r2 => new ConsumerMetrics(r2.U32(), "", 0, 0, ConsumerState.Shared, r2.U64()));
                        return new QueueMetrics(id, 0, 0, 0, 0, 0, bytes, System.Array.Empty<ulong>(), 0);
                    });
                    queues = queues.Select(q => WithBytes(q, held)).ToList();
                }
                else if (t.Number == TagDisk)
                {
                    disk = new DiskMetrics(vr.U64(), vr.Bool());
                }
                else
                {
                    continue; // between nodes, or from a newer version: skipped
                }
                vr.Finish();
            }
            return new DescribeMetrics(
                error, nodeId, version, uptime, metadataGroup, coordinatorGroup, connections, clientIn, clientOut,
                peerIn, peerOut, latency, host, partitions, queues, groups, disk);
        }
    }

    /// <summary>The SASL mechanisms the node offers.</summary>
    /// <param name="Error">The outcome.</param>
    /// <param name="Mechanisms">The enabled mechanisms.</param>
    public sealed record SaslHandshake(ErrorCode Error, IReadOnlyList<string> Mechanisms) : Response
    {
        /// <inheritdoc/>
        public override void Write(Writer w) => w.U16(Error.Code).Array(Mechanisms, (w1, m) => w1.String(m));

        internal static SaslHandshake Read(Reader r) => new(new ErrorCode(r.U16()), r.Array(2, r1 => r1.String()));
    }

    /// <summary>One step of SASL authentication.</summary>
    /// <param name="Error">The outcome.</param>
    /// <param name="ErrorMsg">Details for humans, or <c>null</c>.</param>
    /// <param name="AuthBytes">The mechanism's message.</param>
    /// <param name="SessionLifetimeMs">With the last step: how long the session lasts; 0 for the connection's life.</param>
    public sealed record SaslAuthenticate(ErrorCode Error, string? ErrorMsg, ByteString AuthBytes, ulong SessionLifetimeMs)
        : Response
    {
        /// <inheritdoc/>
        public override void Write(Writer w) =>
            w.U16(Error.Code).OptString(ErrorMsg).Bytes(AuthBytes).U64(SessionLifetimeMs);

        internal static SaslAuthenticate Read(Reader r) =>
            new(new ErrorCode(r.U16()), r.OptString(), r.Bytes(), r.U64());
    }

    /// <summary>The users with a SCRAM credential.</summary>
    /// <param name="Error">The outcome.</param>
    /// <param name="LeaderHint">The metadata leader, after <c>NOT_LEADER</c>.</param>
    /// <param name="Users">The users.</param>
    public sealed record DescribeUsers(ErrorCode Error, LeaderHint? LeaderHint, IReadOnlyList<UserEntry> Users) : Response
    {
        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.U16(Error.Code);
            PutHint(w, LeaderHint);
            w.Array(Users, (w1, u) => w1.String(u.Name).String(u.Mechanism).U32(u.Iterations).U8(u.Source));
        }

        internal static DescribeUsers Read(Reader r)
        {
            var error = new ErrorCode(r.U16());
            LeaderHint? hint = GetHint(r);
            // name, mechanism, iterations, source: 2 + 2 + 4 + 1 bytes
            return new DescribeUsers(error, hint, r.Array(9, r1 => new UserEntry(r1.String(), r1.String(), r1.U32(), r1.U8())));
        }
    }

    /// <summary>The certificate requests, newest first.</summary>
    /// <param name="Error">The outcome.</param>
    /// <param name="LeaderHint">The metadata leader, after <c>NOT_LEADER</c>.</param>
    /// <param name="NowMs">The answering node's clock in milliseconds since the epoch.</param>
    /// <param name="Requests">The requests.</param>
    public sealed record DescribeCertificateRequests(
        ErrorCode Error, LeaderHint? LeaderHint, ulong NowMs, IReadOnlyList<CertificateRequestEntry> Requests) : Response
    {
        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.U16(Error.Code);
            PutHint(w, LeaderHint);
            w.U64(NowMs);
            w.Array(Requests, (w1, c) => w1.Bytes(c.Id).String(c.Principal).Bytes(c.Csr).String(c.RequestedBy)
                .U64(c.RequestedAtMs).U64(c.ExpiresAtMs).U8(c.State).Bytes(c.Certificate).U64(c.SignedAtMs));
        }

        internal static DescribeCertificateRequests Read(Reader r)
        {
            var error = new ErrorCode(r.U16());
            LeaderHint? hint = GetHint(r);
            ulong now = r.U64();
            // id, principal, csr, requested_by, two u64, state, certificate, one u64: 4 + 2 + 4 + 2 + 2 * 8 + 1 + 4 + 8 bytes at least
            List<CertificateRequestEntry> requests = r.Array(
                41,
                r1 => new CertificateRequestEntry(
                    r1.Bytes(), r1.String(), r1.Bytes(), r1.String(), r1.U64(), r1.U64(), r1.U8(), r1.Bytes(), r1.U64()));
            return new DescribeCertificateRequests(error, hint, now, requests);
        }
    }

    /// <summary>The node holding the restart turn, in <see cref="DescribeUpgrade"/>.</summary>
    /// <param name="NodeId">The node.</param>
    /// <param name="SinceMs">When it got the turn, by the metadata leader's clock, in milliseconds since the epoch.</param>
    public sealed record UpgradeTurn(ulong NodeId, ulong SinceMs);

    /// <summary>One node in <see cref="DescribeUpgrade"/>.</summary>
    /// <param name="NodeId">Its id.</param>
    /// <param name="Release">Its release; empty until it reported, and then the versions mean nothing.</param>
    /// <param name="Oldest">The oldest cluster version it runs with, <c>major &lt;&lt; 16 | minor</c>.</param>
    /// <param name="Newest">The newest, its own.</param>
    public sealed record UpgradeNode(ulong NodeId, string Release, uint Oldest, uint Newest);

    /// <summary>
    /// <c>DescribeUpgrade</c> (65): where a rolling upgrade stands, from the metadata group's leader (ADR 0127). The
    /// leader hint is the structure's block of tagged fields.
    /// </summary>
    /// <param name="Error">The outcome.</param>
    /// <param name="LeaderHint">The metadata leader, after <c>NOT_LEADER</c>.</param>
    /// <param name="ClusterVersion">The cluster version in force, <c>major &lt;&lt; 16 | minor</c>, or <c>null</c> while
    /// there is none yet; version 0.0 is a version too.</param>
    /// <param name="Held">Whether restart turns are paused.</param>
    /// <param name="Turn">The node holding the restart turn, or <c>null</c>.</param>
    /// <param name="Nodes">Every node of the cluster, by id.</param>
    public sealed record DescribeUpgrade(
        ErrorCode Error, LeaderHint? LeaderHint, uint? ClusterVersion, bool Held, UpgradeTurn? Turn,
        IReadOnlyList<UpgradeNode> Nodes)
        : Response
    {
        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.U16(Error.Code);
            if (ClusterVersion is uint version)
            {
                w.U8(1).U32(version);
            }
            else
            {
                w.U8(0);
            }
            w.Bool(Held);
            if (Turn is null)
            {
                w.U8(0);
            }
            else
            {
                w.U8(1).U64(Turn.NodeId).U64(Turn.SinceMs);
            }
            w.Array(Nodes, (w1, n) => w1.U64(n.NodeId).String(n.Release).U32(n.Oldest).U32(n.Newest));
            PutHint(w, LeaderHint);
        }

        internal static DescribeUpgrade Read(Reader r)
        {
            var error = new ErrorCode(r.U16());
            uint? version = r.U8() == 0 ? null : r.U32();
            bool held = r.U8() != 0;
            UpgradeTurn? turn = r.U8() == 0 ? null : new UpgradeTurn(r.U64(), r.U64());
            // at least: id, release, two u32 (the bound the Rust codec uses)
            List<UpgradeNode> nodes = r.Array(17, r1 => new UpgradeNode(r1.U64(), r1.String(), r1.U32(), r1.U32()));
            return new DescribeUpgrade(error, GetHint(r), version, held, turn, nodes);
        }
    }

    /// <summary>A node's open client sessions, oldest first (ADR 0121).</summary>
    /// <param name="Error">The outcome.</param>
    /// <param name="NodeId">The answering node.</param>
    /// <param name="NowMs">The answering node's clock in milliseconds since the epoch.</param>
    /// <param name="Sessions">The sessions the asker may see.</param>
    public sealed record DescribeSessions(ErrorCode Error, uint NodeId, ulong NowMs, IReadOnlyList<SessionEntry> Sessions)
        : Response
    {
        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.U16(Error.Code).U32(NodeId).U64(NowMs);
            w.Array(Sessions, (w1, s) =>
            {
                w1.U8(s.PrincipalKind).String(s.Name);
                w1.Array(s.Groups, (w2, g) => w2.String(g));
                w1.U8(s.Mechanism).String(s.Peer);
                w1.U64(s.ConnectedMs).U64(s.CredentialMs).U64(s.ExpiresMs);
                if (s.TokenId is ulong tokenId)
                {
                    var v = new Writer();
                    v.U64(tokenId);
                    w1.Tags(new[] { new Tag(TagTokenId, ByteString.CopyOf(v.ToByteArray())) });
                }
                else
                {
                    w1.NoTags();
                }
            });
            w.NoTags();
        }

        internal static DescribeSessions Read(Reader r)
        {
            var error = new ErrorCode(r.U16());
            uint nodeId = r.U32();
            ulong now = r.U64();
            // kind, name, no groups, mechanism, peer, three u64, no tags
            List<SessionEntry> sessions = r.Array(1 + 2 + 1 + 1 + 2 + 3 * 8 + 1, r1 =>
            {
                byte principalKind = r1.U8();
                string name = r1.String();
                List<string> groups = r1.Array(2, r2 => r2.String());
                byte mechanism = r1.U8();
                string peer = r1.String();
                ulong connectedMs = r1.U64();
                ulong credentialMs = r1.U64();
                ulong expiresMs = r1.U64();
                ulong? tokenId = null;
                foreach (Tag t in r1.Tags())
                {
                    if (t.Number == TagTokenId)
                    {
                        var v = new Reader(t.Value.ToByteArray());
                        tokenId = v.U64();
                        v.Finish();
                    }
                    // any other tag is from a newer version: skipped
                }
                return new SessionEntry(
                    principalKind, name, groups, mechanism, peer, connectedMs, credentialMs, expiresMs, tokenId);
            });
            r.Tags();
            return new DescribeSessions(error, nodeId, now, sessions);
        }
    }

    /// <summary>The client certificates the cluster has seen or had revoked, by expiry.</summary>
    /// <param name="Error">The outcome.</param>
    /// <param name="LeaderHint">The metadata leader, after <c>NOT_LEADER</c>.</param>
    /// <param name="NowMs">The answering node's clock in milliseconds since the epoch, which the states are computed against.</param>
    /// <param name="Certificates">The certificates.</param>
    public sealed record DescribeCertificates(
        ErrorCode Error, LeaderHint? LeaderHint, ulong NowMs, IReadOnlyList<CertificateEntry> Certificates) : Response
    {
        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.U16(Error.Code);
            PutHint(w, LeaderHint);
            w.U64(NowMs);
            w.Array(Certificates, (w1, c) => w1.Bytes(c.Issuer).Bytes(c.Serial).String(c.Principal)
                .U64(c.NotBeforeMs).U64(c.NotAfterMs).U64(c.FirstSeenMs).U64(c.LastSeenMs)
                .U8(c.State).String(c.Reason).U64(c.RevokedAtMs));
        }

        internal static DescribeCertificates Read(Reader r)
        {
            var error = new ErrorCode(r.U16());
            LeaderHint? hint = GetHint(r);
            ulong now = r.U64();
            // issuer, serial, principal, four u64, state, reason, one u64: 4 + 4 + 2 + 4 * 8 + 1 + 2 + 8 bytes at least
            List<CertificateEntry> certificates = r.Array(
                53,
                r1 => new CertificateEntry(
                    r1.Bytes(), r1.Bytes(), r1.String(), r1.U64(), r1.U64(), r1.U64(), r1.U64(), r1.U8(), r1.String(), r1.U64()));
            return new DescribeCertificates(error, hint, now, certificates);
        }
    }

    /// <summary>The tenants.</summary>
    /// <param name="Error">The outcome.</param>
    /// <param name="LeaderHint">The metadata leader, after <c>NOT_LEADER</c>.</param>
    /// <param name="Tenants">The tenants.</param>
    public sealed record DescribeTenants(ErrorCode Error, LeaderHint? LeaderHint, IReadOnlyList<TenantEntry> Tenants) : Response
    {
        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.U16(Error.Code);
            PutHint(w, LeaderHint);
            w.Array(Tenants, (w1, t) => w1.String(t.Name).U8(t.Source).U32(t.Resources));
        }

        internal static DescribeTenants Read(Reader r)
        {
            var error = new ErrorCode(r.U16());
            LeaderHint? hint = GetHint(r);
            // name, source, resources: 2 + 1 + 4 bytes
            return new DescribeTenants(error, hint, r.Array(7, r1 => new TenantEntry(r1.String(), r1.U8(), r1.U32())));
        }
    }

    /// <summary>The role bindings.</summary>
    /// <param name="Error">The outcome.</param>
    /// <param name="LeaderHint">The metadata leader, after <c>NOT_LEADER</c>.</param>
    /// <param name="Bindings">The bindings.</param>
    public sealed record DescribeRoleBindings(ErrorCode Error, LeaderHint? LeaderHint, IReadOnlyList<SourcedBinding> Bindings)
        : Response
    {
        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.U16(Error.Code);
            PutHint(w, LeaderHint);
            w.Array(Bindings, (w1, b) =>
            {
                RoleBinding.Write(w1, b.Binding);
                w1.U8(b.Source);
            });
        }

        internal static DescribeRoleBindings Read(Reader r)
        {
            var error = new ErrorCode(r.U16());
            LeaderHint? hint = GetHint(r);
            return new DescribeRoleBindings(
                error, hint, r.Array(BindingMin + 1, r1 => new SourcedBinding(RoleBinding.Read(r1), r1.U8())));
        }
    }

    /// <summary>Who the connection acts as. This response has no tagged fields.</summary>
    /// <param name="Kind">See <see cref="PrincipalKind"/>.</param>
    /// <param name="Name">The principal's name.</param>
    /// <param name="Groups">Its identity-provider groups.</param>
    /// <param name="Bindings">The role bindings that apply to it.</param>
    public sealed record WhoAmI(byte Kind, string Name, IReadOnlyList<string> Groups, IReadOnlyList<RoleBinding> Bindings) : Response
    {
        /// <inheritdoc/>
        public override void Write(Writer w) =>
            w.U8(Kind).String(Name).Array(Groups, (w1, g) => w1.String(g)).Array(Bindings, RoleBinding.Write);

        internal static WhoAmI Read(Reader r) =>
            new(r.U8(), r.String(), r.Array(2, r1 => r1.String()), r.Array(BindingMin, RoleBinding.Read));
    }

    /// <summary>The cluster's nodes.</summary>
    /// <param name="Error">The outcome.</param>
    /// <param name="LeaderHint">The metadata leader, after <c>NOT_LEADER</c>.</param>
    /// <param name="Nodes">The nodes.</param>
    public sealed record DescribeNodes(ErrorCode Error, LeaderHint? LeaderHint, IReadOnlyList<NodeEntry> Nodes) : Response
    {
        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.U16(Error.Code);
            PutHint(w, LeaderHint);
            w.Array(Nodes, (w1, n) =>
                w1.U64(n.NodeId).String(n.Peer).String(n.Client).U8(n.State).U32(n.Replicas).U32(n.Moving));
        }

        internal static DescribeNodes Read(Reader r)
        {
            var error = new ErrorCode(r.U16());
            LeaderHint? hint = GetHint(r);
            return new DescribeNodes(
                error, hint, r.Array(30, r1 => new NodeEntry(r1.U64(), r1.String(), r1.String(), r1.U8(), r1.U32(), r1.U32())));
        }
    }

    /// <summary>The outcome of a produce, per partition.</summary>
    /// <param name="ThrottleMs">How long the client should hold back.</param>
    /// <param name="Resources">Per resource and partition.</param>
    public sealed record Produce(uint ThrottleMs, IReadOnlyList<ProduceResourceResponse> Resources) : Response
    {
        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.U32(ThrottleMs);
            w.Array(Resources, (w1, res) =>
            {
                w1.U64(res.ResourceId);
                w1.Array(res.Partitions, (w2, p) =>
                {
                    w2.U32(p.Partition).U16(p.Error.Code).U64(p.BaseOffset).I64(p.AppendTs);
                    w2.OptString(p.ErrorMsg);
                    PutHint(w2, p.LeaderHint);
                });
                w1.NoTags();
            });
            w.NoTags();
        }

        internal static Produce Read(Reader r)
        {
            uint throttle = r.U32();
            List<ProduceResourceResponse> resources = r.Array(10, r1 =>
            {
                ulong id = r1.U64();
                List<ProducePartitionResponse> partitions = r1.Array(25, r2 => new ProducePartitionResponse(
                    r2.U32(), new ErrorCode(r2.U16()), r2.U64(), r2.I64(), r2.OptString(), GetHint(r2)));
                r1.Tags();
                return new ProduceResourceResponse(id, partitions);
            });
            r.Tags();
            return new Produce(throttle, resources);
        }
    }

    /// <summary>The offsets looked up, per partition.</summary>
    /// <param name="Resources">Per resource and partition.</param>
    public sealed record ListOffsets(IReadOnlyList<ListOffsetsResourceResponse> Resources) : Response
    {
        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.Array(Resources, (w1, res) =>
            {
                w1.U64(res.ResourceId);
                w1.Array(res.Partitions, (w2, p) =>
                {
                    w2.U32(p.Partition).U16(p.Error.Code);
                    w2.U64(p.Offset).I64(p.Timestamp);
                    PutHint(w2, p.LeaderHint);
                });
                w1.NoTags();
            });
            w.NoTags();
        }

        internal static ListOffsets Read(Reader r)
        {
            List<ListOffsetsResourceResponse> resources = r.Array(10, r1 =>
            {
                ulong id = r1.U64();
                List<ListOffsetsPartitionResponse> partitions = r1.Array(23, r2 => new ListOffsetsPartitionResponse(
                    r2.U32(), new ErrorCode(r2.U16()), r2.U64(), r2.I64(), GetHint(r2)));
                r1.Tags();
                return new ListOffsetsResourceResponse(id, partitions);
            });
            r.Tags();
            return new ListOffsets(resources);
        }
    }

    /// <summary>What was read, per partition.</summary>
    /// <param name="ThrottleMs">How long the client should hold back.</param>
    /// <param name="Resources">Per resource and partition.</param>
    public sealed record Fetch(uint ThrottleMs, IReadOnlyList<FetchResourceResponse> Resources) : Response
    {
        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.U32(ThrottleMs);
            w.Array(Resources, (w1, res) =>
            {
                w1.U64(res.ResourceId);
                w1.Array(res.Partitions, (w2, p) =>
                {
                    w2.U32(p.Partition).U16(p.Error.Code);
                    w2.U64(p.HighWatermark).U64(p.LastStableOffset).U64(p.LogStartOffset);
                    w2.Array(p.AbortedTxns, (w3, a) => w3.U64(a.ProducerId).U64(a.FirstOffset));
                    w2.Bytes(p.Records);
                    PutHint(w2, p.LeaderHint);
                });
                w1.NoTags();
            });
            w.NoTags();
        }

        internal static Fetch Read(Reader r)
        {
            uint throttle = r.U32();
            List<FetchResourceResponse> resources = r.Array(10, r1 =>
            {
                ulong id = r1.U64();
                List<FetchPartitionResponse> partitions = r1.Array(36, r2 => new FetchPartitionResponse(
                    r2.U32(),
                    new ErrorCode(r2.U16()),
                    r2.U64(),
                    r2.U64(),
                    r2.U64(),
                    r2.Array(16, r3 => new AbortedTxn(r3.U64(), r3.U64())),
                    r2.Bytes(),
                    GetHint(r2)));
                r1.Tags();
                return new FetchResourceResponse(id, partitions);
            });
            r.Tags();
            return new Fetch(throttle, resources);
        }
    }

    /// <summary>
    /// A refused request: the whole frame was refused, and the body is only an error code. Decode it before any typed
    /// body (the error flag).
    /// </summary>
    /// <param name="Code">The error.</param>
    public sealed record Failed(ErrorCode Code) : Response
    {
        /// <inheritdoc/>
        public override void Write(Writer w) => w.U16(Code.Code).NoTags();
    }

    /// <summary>A refused request with a reason (tagged field 0 of an error frame, UTF-8).</summary>
    /// <param name="Code">The error.</param>
    /// <param name="Message">The reason.</param>
    public sealed record Refused(ErrorCode Code, string Message) : Response
    {
        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.U16(Code.Code);
            w.Tags(new[] { new Tag(TagReason, ByteString.Utf8(Message)) });
        }
    }

    /// <summary>The renewed certificate (ADR 0123). The leader hint is the structure's block of tagged fields.</summary>
    /// <param name="Error">The outcome.</param>
    /// <param name="ErrorMsg">Why, for a refusal, or <c>null</c>.</param>
    /// <param name="Chain">The new certificate first, then the renewal CA's, DER.</param>
    /// <param name="NotAfterMs">Milliseconds since the epoch at which the new certificate stops being valid.</param>
    /// <param name="LeaderHint">The metadata leader, after <c>NOT_LEADER</c>.</param>
    public sealed record RenewCertificate(
        ErrorCode Error, string? ErrorMsg, IReadOnlyList<ByteString> Chain, ulong NotAfterMs, LeaderHint? LeaderHint)
        : Response
    {
        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.U16(Error.Code).OptString(ErrorMsg).Array(Chain, (w1, c) => w1.Bytes(c)).U64(NotAfterMs);
            PutHint(w, LeaderHint);
        }

        internal static RenewCertificate Read(Reader r)
        {
            var error = new ErrorCode(r.U16());
            var errorMsg = r.OptString();
            // A certificate is at least its length prefix.
            var chain = r.Array(4, r1 => r1.Bytes());
            var notAfterMs = r.U64();
            return new RenewCertificate(error, errorMsg, chain, notAfterMs, GetHint(r));
        }
    }

    /// <summary>
    /// <c>CreateToken</c> (66): the token the metadata leader recorded (ADR 0133). The leader hint is the structure's
    /// block of tagged fields.
    /// </summary>
    /// <param name="Error">The outcome.</param>
    /// <param name="ErrorMsg">Why, for a refusal, or <c>null</c>.</param>
    /// <param name="Id">The token's id; 0 on a refusal.</param>
    /// <param name="ExpiresAtMs">When the token stops being valid, in milliseconds since the epoch; 0 on a refusal.</param>
    /// <param name="LeaderHint">The metadata leader, after <c>NOT_LEADER</c>.</param>
    public sealed record CreateToken(ErrorCode Error, string? ErrorMsg, ulong Id, ulong ExpiresAtMs, LeaderHint? LeaderHint)
        : Response
    {
        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.U16(Error.Code).OptString(ErrorMsg).U64(Id).U64(ExpiresAtMs);
            PutHint(w, LeaderHint);
        }

        internal static CreateToken Read(Reader r)
        {
            var error = new ErrorCode(r.U16());
            string? errorMsg = r.OptString();
            ulong id = r.U64();
            ulong expiresAtMs = r.U64();
            return new CreateToken(error, errorMsg, id, expiresAtMs, GetHint(r));
        }
    }

    /// <summary>
    /// <c>DescribeTokens</c> (67): the access tokens the asker may see, never a secret or hash (ADR 0133). The leader
    /// hint is the structure's block of tagged fields, after the tokens.
    /// </summary>
    /// <param name="Error">The outcome.</param>
    /// <param name="LeaderHint">The metadata leader, after <c>NOT_LEADER</c>.</param>
    /// <param name="Tokens">The tokens.</param>
    public sealed record DescribeTokens(ErrorCode Error, LeaderHint? LeaderHint, IReadOnlyList<TokenEntry> Tokens) : Response
    {
        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.U16(Error.Code);
            w.Array(Tokens, (w1, t) =>
            {
                w1.U64(t.Id).String(t.Name).String(t.Owner);
                w1.U64(t.CreatedAtMs).U64(t.ExpiresAtMs).String(t.CreatedBy);
                w1.NoTags();
            });
            PutHint(w, LeaderHint);
        }

        internal static DescribeTokens Read(Reader r)
        {
            var error = new ErrorCode(r.U16());
            // id, name, owner, two times, created by, no tags
            List<TokenEntry> tokens = r.Array(8 + 2 + 2 + 8 + 8 + 2 + 1, r1 =>
            {
                var t = new TokenEntry(r1.U64(), r1.String(), r1.String(), r1.U64(), r1.U64(), r1.String());
                r1.Tags();
                return t;
            });
            return new DescribeTokens(error, GetHint(r), tokens);
        }
    }
}
