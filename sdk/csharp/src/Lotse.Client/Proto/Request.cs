using System.Collections.Generic;
using Lotse.Record;
using static Lotse.Proto.Messages;

namespace Lotse.Proto;

/// <summary>
/// The requests a client sends, all at version 0. Each is written and read by its own code, in the layout of the
/// broker; <see cref="Frames"/> adds the frame header.
/// </summary>
public abstract record Request
{
    private protected Request()
    {
    }

    /// <summary>The key this request is sent with.</summary>
    public abstract ApiKey ApiKey { get; }

    /// <summary>Writes the body, the part after the frame header.</summary>
    public abstract void Write(Writer w);

    /// <summary>Asks for the versions and features the node speaks.</summary>
    public sealed record ApiVersions : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.ApiVersions;

        /// <inheritdoc/>
        public override void Write(Writer w) => w.NoTags();

        internal static ApiVersions Read(Reader r)
        {
            r.Tags();
            return new ApiVersions();
        }
    }

    /// <summary>Asks for brokers and the named resources; no name asks for all.</summary>
    /// <param name="Names">The resource names.</param>
    public sealed record Metadata(IReadOnlyList<string> Names) : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.Metadata;

        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.Array(Names, (w1, n) => w1.String(n));
            w.NoTags();
        }

        internal static Metadata Read(Reader r)
        {
            var m = new Metadata(r.Array(2, r1 => r1.String()));
            r.Tags();
            return m;
        }
    }

    /// <summary>Asks which node coordinates a group or producer.</summary>
    /// <param name="KeyType">See <see cref="CoordinatorType"/>.</param>
    /// <param name="Key">The group id, or the transactional id (empty for an idempotent producer).</param>
    public sealed record FindCoordinator(byte KeyType, string Key) : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.FindCoordinator;

        /// <inheritdoc/>
        public override void Write(Writer w) => w.U8(KeyType).String(Key).NoTags();

        internal static FindCoordinator Read(Reader r)
        {
            var x = new FindCoordinator(r.U8(), r.String());
            r.Tags();
            return x;
        }
    }

    /// <summary>Appends batches to partitions.</summary>
    /// <param name="TxnId">The transactional id, or <c>null</c>.</param>
    /// <param name="TimeoutMs">How long the leader waits for a quorum.</param>
    /// <param name="Resources">The batches, per resource and partition.</param>
    /// <param name="Tenant">The tenant of the transactional id.</param>
    public sealed record Produce(
        string? TxnId, uint TimeoutMs, IReadOnlyList<ProduceResource> Resources, string Tenant = DefaultTenant) : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.Produce;

        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.OptString(TxnId).U32(TimeoutMs);
            w.Array(Resources, (w1, res) =>
            {
                w1.U64(res.ResourceId);
                w1.Array(res.Partitions, (w2, p) =>
                {
                    w2.U32(p.Partition);
                    w2.Bytes(p.Batch);
                    w2.NoTags();
                });
                w1.NoTags();
            });
            PutTenant(w, Tenant);
        }

        internal static Produce Read(Reader r)
        {
            string? txnId = r.OptString();
            uint timeoutMs = r.U32();
            List<ProduceResource> resources = r.Array(10, r1 =>
            {
                ulong id = r1.U64();
                List<ProducePartition> partitions = r1.Array(9, r2 =>
                {
                    var p = new ProducePartition(r2.U32(), r2.Bytes());
                    r2.Tags();
                    return p;
                });
                r1.Tags();
                return new ProduceResource(id, partitions);
            });
            return new Produce(txnId, timeoutMs, resources, GetTenant(r));
        }
    }

    /// <summary>Looks up offsets by time or at either end of partitions.</summary>
    /// <param name="Isolation">See <see cref="Messages.Isolation"/>.</param>
    /// <param name="Resources">The partitions to look up.</param>
    public sealed record ListOffsets(byte Isolation, IReadOnlyList<ListOffsetsResource> Resources) : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.ListOffsets;

        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.U8(Isolation);
            w.Array(Resources, (w1, res) =>
            {
                w1.U64(res.ResourceId);
                w1.Array(res.Partitions, (w2, p) =>
                {
                    w2.U32(p.Partition);
                    w2.I64(p.Timestamp);
                    w2.NoTags();
                });
                w1.NoTags();
            });
            w.NoTags();
        }

        internal static ListOffsets Read(Reader r)
        {
            byte isolation = r.U8();
            List<ListOffsetsResource> resources = r.Array(10, r1 =>
            {
                ulong id = r1.U64();
                List<ListOffsetsPartition> partitions = r1.Array(13, r2 =>
                {
                    var p = new ListOffsetsPartition(r2.U32(), r2.I64());
                    r2.Tags();
                    return p;
                });
                r1.Tags();
                return new ListOffsetsResource(id, partitions);
            });
            r.Tags();
            return new ListOffsets(isolation, resources);
        }
    }

    /// <summary>Reads batches from partitions.</summary>
    /// <param name="MaxWaitMs">How long the leader holds the request when there is nothing to return.</param>
    /// <param name="MinBytes">Return as soon as this many bytes are ready.</param>
    /// <param name="MaxBytes">The limit for the whole response.</param>
    /// <param name="Isolation">See <see cref="Messages.Isolation"/>.</param>
    /// <param name="Resources">The partitions to read.</param>
    public sealed record Fetch(
        uint MaxWaitMs, uint MinBytes, uint MaxBytes, byte Isolation, IReadOnlyList<FetchResource> Resources) : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.Fetch;

        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.U32(MaxWaitMs).U32(MinBytes).U32(MaxBytes).U8(Isolation);
            w.Array(Resources, (w1, res) =>
            {
                w1.U64(res.ResourceId);
                w1.Array(res.Partitions, (w2, p) =>
                {
                    w2.U32(p.Partition);
                    w2.U64(p.FetchOffset);
                    w2.U64(p.LeaderTerm);
                    w2.U32(p.MaxBytes);
                    w2.NoTags();
                });
                w1.NoTags();
            });
            w.NoTags();
        }

        internal static Fetch Read(Reader r)
        {
            uint maxWait = r.U32();
            uint minBytes = r.U32();
            uint maxBytes = r.U32();
            byte isolation = r.U8();
            List<FetchResource> resources = r.Array(10, r1 =>
            {
                ulong id = r1.U64();
                List<FetchPartition> partitions = r1.Array(25, r2 =>
                {
                    var p = new FetchPartition(r2.U32(), r2.U64(), r2.U64(), r2.U32());
                    r2.Tags();
                    return p;
                });
                r1.Tags();
                return new FetchResource(id, partitions);
            });
            r.Tags();
            return new Fetch(maxWait, minBytes, maxBytes, isolation, resources);
        }
    }

    /// <summary>Asks the coordinator for a producer id and epoch.</summary>
    /// <param name="TxnId">The transactional id, or <c>null</c> for an idempotent producer.</param>
    /// <param name="TxnTimeoutMs">How long an open transaction may idle.</param>
    /// <param name="Tenant">The tenant of the transactional id.</param>
    public sealed record InitProducer(string? TxnId, uint TxnTimeoutMs, string Tenant = DefaultTenant) : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.InitProducer;

        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.OptString(TxnId).U32(TxnTimeoutMs);
            PutTenant(w, Tenant);
        }

        internal static InitProducer Read(Reader r) => new(r.OptString(), r.U32(), GetTenant(r));
    }

    /// <summary>Adds partitions to a transaction.</summary>
    /// <param name="TxnId">The transactional id.</param>
    /// <param name="ProducerId">The producer.</param>
    /// <param name="ProducerEpoch">Its epoch.</param>
    /// <param name="Resources">The partitions.</param>
    /// <param name="Tenant">The tenant of the transactional id.</param>
    public sealed record AddPartitionsToTxn(
        string TxnId, ulong ProducerId, ushort ProducerEpoch, IReadOnlyList<TxnResource> Resources, string Tenant = DefaultTenant)
        : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.AddPartitionsToTxn;

        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.String(TxnId).U64(ProducerId).U16(ProducerEpoch);
            PutTxnResources(w, Resources);
            PutTenant(w, Tenant);
        }

        internal static AddPartitionsToTxn Read(Reader r) =>
            new(r.String(), r.U64(), r.U16(), GetTxnResources(r), GetTenant(r));
    }

    /// <summary>Commits or aborts a transaction.</summary>
    /// <param name="TxnId">The transactional id.</param>
    /// <param name="ProducerId">The producer.</param>
    /// <param name="ProducerEpoch">Its epoch.</param>
    /// <param name="Commit"><c>true</c> to commit, <c>false</c> to abort.</param>
    /// <param name="Tenant">The tenant of the transactional id.</param>
    public sealed record EndTxn(string TxnId, ulong ProducerId, ushort ProducerEpoch, bool Commit, string Tenant = DefaultTenant)
        : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.EndTxn;

        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.String(TxnId).U64(ProducerId).U16(ProducerEpoch).Bool(Commit);
            PutTenant(w, Tenant);
        }

        internal static EndTxn Read(Reader r) => new(r.String(), r.U64(), r.U16(), r.Bool(), GetTenant(r));
    }

    /// <summary>Subscribes to a queue.</summary>
    /// <param name="QueueId">The queue.</param>
    /// <param name="ConsumerTag">A tag for the subscription.</param>
    /// <param name="CreditMsgs">How many messages may be pushed before more credit.</param>
    /// <param name="CreditBytes">How many bytes may be pushed before more credit.</param>
    /// <param name="Exclusive">Refused by the broker; do not set.</param>
    /// <param name="NotifyActive">Ask for a push when this subscription becomes the single active consumer (ADR 0076).</param>
    public sealed record Subscribe(
        ulong QueueId, string ConsumerTag, uint CreditMsgs, uint CreditBytes, bool Exclusive, bool NotifyActive = false)
        : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.Subscribe;

        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.U64(QueueId).String(ConsumerTag).U32(CreditMsgs).U32(CreditBytes).Bool(Exclusive);
            if (NotifyActive)
            {
                w.Tags(new[] { new Tag(TagNotifyActive, ByteString.CopyOf(new byte[] { 1 })) });
            }
            else
            {
                w.NoTags();
            }
        }

        internal static Subscribe Read(Reader r)
        {
            ulong queueId = r.U64();
            string tag = r.String();
            uint msgs = r.U32();
            uint bytes = r.U32();
            bool exclusive = r.Bool();
            bool notify = false;
            foreach (Tag t in r.Tags())
            {
                if (t.Number == TagNotifyActive)
                {
                    var vr = new Reader(t.Value.ToByteArray());
                    notify = vr.U8() != 0;
                    vr.Finish();
                }
            }
            return new Subscribe(queueId, tag, msgs, bytes, exclusive, notify);
        }
    }

    /// <summary>Adds credit to a subscription. Never answered.</summary>
    /// <param name="SubscriptionId">The subscription.</param>
    /// <param name="AddMsgs">Messages to add.</param>
    /// <param name="AddBytes">Bytes to add.</param>
    public sealed record Credit(uint SubscriptionId, uint AddMsgs, uint AddBytes) : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.Credit;

        /// <inheritdoc/>
        public override void Write(Writer w) => w.U32(SubscriptionId).U32(AddMsgs).U32(AddBytes).NoTags();

        internal static Credit Read(Reader r)
        {
            var x = new Credit(r.U32(), r.U32(), r.U32());
            r.Tags();
            return x;
        }
    }

    /// <summary>Acknowledges queue messages.</summary>
    /// <param name="QueueId">The queue.</param>
    /// <param name="TxnId">The transactional id when acknowledging within a transaction, otherwise <c>null</c>.</param>
    /// <param name="Ranges">The offsets, as ranges.</param>
    /// <param name="Tenant">The tenant of the transactional id.</param>
    /// <param name="ProducerId">With a transaction: the producer.</param>
    /// <param name="ProducerEpoch">With a transaction: its epoch.</param>
    public sealed record Ack(
        ulong QueueId,
        string? TxnId,
        IReadOnlyList<OffsetRange> Ranges,
        string Tenant = DefaultTenant,
        ulong ProducerId = 0,
        ushort ProducerEpoch = 0) : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.Ack;

        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.U64(QueueId).OptString(TxnId);
            PutRanges(w, Ranges);
            var tags = new List<Tag>();
            if (DefaultTenant != Tenant)
            {
                tags.Add(StringTag(TagTenant, Tenant));
            }
            if (TxnId is not null)
            {
                var v = new Writer();
                v.U64(ProducerId).U16(ProducerEpoch);
                tags.Add(new Tag(TagProducer, ByteString.CopyOf(v.ToByteArray())));
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

        internal static Ack Read(Reader r)
        {
            ulong queueId = r.U64();
            string? txnId = r.OptString();
            List<OffsetRange> ranges = GetRanges(r);
            string tenant = DefaultTenant;
            ulong producerId = 0;
            ushort epoch = 0;
            foreach (Tag t in r.Tags())
            {
                var vr = new Reader(t.Value.ToByteArray());
                if (t.Number == TagTenant)
                {
                    tenant = vr.String();
                }
                else if (t.Number == TagProducer)
                {
                    producerId = vr.U64();
                    epoch = vr.U16();
                }
                else
                {
                    continue; // from a newer version: skipped
                }
                vr.Finish();
            }
            return new Ack(queueId, txnId, ranges, tenant, producerId, epoch);
        }
    }

    /// <summary>Rejects queue messages: back to the queue, or discarded.</summary>
    /// <param name="QueueId">The queue.</param>
    /// <param name="Ranges">The offsets, as ranges.</param>
    /// <param name="Requeue">Deliver them again.</param>
    /// <param name="DelayMs">With requeue: how long they wait first (ADR 0077).</param>
    public sealed record Nack(ulong QueueId, IReadOnlyList<OffsetRange> Ranges, bool Requeue, uint DelayMs) : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.Nack;

        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.U64(QueueId);
            PutRanges(w, Ranges);
            w.Bool(Requeue).U32(DelayMs).NoTags();
        }

        internal static Nack Read(Reader r)
        {
            var x = new Nack(r.U64(), GetRanges(r), r.Bool(), r.U32());
            r.Tags();
            return x;
        }
    }

    /// <summary>Ends a subscription.</summary>
    /// <param name="SubscriptionId">The subscription.</param>
    public sealed record Unsubscribe(uint SubscriptionId) : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.Unsubscribe;

        /// <inheritdoc/>
        public override void Write(Writer w) => w.U32(SubscriptionId).NoTags();

        internal static Unsubscribe Read(Reader r)
        {
            var x = new Unsubscribe(r.U32());
            r.Tags();
            return x;
        }
    }

    /// <summary>Joins, stays in or leaves a consumer group.</summary>
    /// <param name="GroupId">The group.</param>
    /// <param name="MemberId">The member; empty to join.</param>
    /// <param name="MemberEpoch">The member's epoch; 0 to join, -1 to leave.</param>
    /// <param name="RebalanceTimeoutMs">How long the member may take to release partitions.</param>
    /// <param name="Subscribed">The subscribed stream ids (on joining), or <c>null</c>.</param>
    /// <param name="Owned">The partitions the member owns, or <c>null</c>.</param>
    /// <param name="Tenant">The tenant of the group id.</param>
    /// <param name="InstanceId">A static member's instance id (ADR 0072), or <c>null</c>.</param>
    /// <param name="SubscribedPattern">A subscription by regular expression (ADR 0073), or <c>null</c>.</param>
    public sealed record GroupHeartbeat(
        string GroupId,
        string MemberId,
        int MemberEpoch,
        uint RebalanceTimeoutMs,
        IReadOnlyList<ulong>? Subscribed,
        IReadOnlyList<TxnResource>? Owned,
        string Tenant = DefaultTenant,
        string? InstanceId = null,
        string? SubscribedPattern = null) : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.GroupHeartbeat;

        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.String(GroupId).String(MemberId).I32(MemberEpoch).U32(RebalanceTimeoutMs);
            w.Bool(Subscribed is not null);
            if (Subscribed is not null)
            {
                w.Array(Subscribed, (w1, id) => w1.U64(id));
            }
            PutOptResources(w, Owned);
            var tags = new List<Tag>();
            if (DefaultTenant != Tenant)
            {
                tags.Add(StringTag(TagTenant, Tenant));
            }
            if (InstanceId is not null)
            {
                tags.Add(StringTag(TagInstance, InstanceId));
            }
            if (SubscribedPattern is not null)
            {
                tags.Add(StringTag(TagPattern, SubscribedPattern));
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

        internal static GroupHeartbeat Read(Reader r)
        {
            string groupId = r.String();
            string memberId = r.String();
            int epoch = r.I32();
            uint timeout = r.U32();
            List<ulong>? subscribed = r.Bool() ? r.Array(8, r1 => r1.U64()) : null;
            List<TxnResource>? owned = GetOptResources(r);
            string tenant = DefaultTenant;
            string? instanceId = null;
            string? pattern = null;
            foreach (Tag t in r.Tags())
            {
                if (t.Number is TagTenant or TagInstance or TagPattern)
                {
                    var vr = new Reader(t.Value.ToByteArray());
                    string v = vr.String();
                    vr.Finish();
                    if (t.Number == TagTenant)
                    {
                        tenant = v;
                    }
                    else if (t.Number == TagInstance)
                    {
                        instanceId = v;
                    }
                    else
                    {
                        pattern = v;
                    }
                }
            }
            return new GroupHeartbeat(groupId, memberId, epoch, timeout, subscribed, owned, tenant, instanceId, pattern);
        }
    }

    /// <summary>Commits a group's offsets.</summary>
    /// <param name="GroupId">The group.</param>
    /// <param name="MemberId">The member.</param>
    /// <param name="MemberEpoch">The member's epoch.</param>
    /// <param name="Resources">The offsets.</param>
    /// <param name="Tenant">The tenant of the group id.</param>
    public sealed record CommitOffsets(
        string GroupId, string MemberId, int MemberEpoch, IReadOnlyList<OffsetCommitResource> Resources, string Tenant = DefaultTenant)
        : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.CommitOffsets;

        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.String(GroupId).String(MemberId).I32(MemberEpoch);
            PutCommits(w, Resources);
            PutTenant(w, Tenant);
        }

        internal static CommitOffsets Read(Reader r) =>
            new(r.String(), r.String(), r.I32(), GetCommits(r), GetTenant(r));
    }

    /// <summary>Reads a group's committed offsets.</summary>
    /// <param name="GroupId">The group.</param>
    /// <param name="RequireStable">Wait for offsets an open transaction holds.</param>
    /// <param name="Resources">The partitions to read.</param>
    /// <param name="Tenant">The tenant of the group id.</param>
    public sealed record OffsetFetch(
        string GroupId, bool RequireStable, IReadOnlyList<TxnResource> Resources, string Tenant = DefaultTenant) : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.OffsetFetch;

        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.String(GroupId).Bool(RequireStable);
            PutTxnResources(w, Resources);
            PutTenant(w, Tenant);
        }

        internal static OffsetFetch Read(Reader r) => new(r.String(), r.Bool(), GetTxnResources(r), GetTenant(r));
    }

    /// <summary>Commits a group's offsets within a transaction.</summary>
    /// <param name="TxnId">The transactional id.</param>
    /// <param name="ProducerId">The producer.</param>
    /// <param name="ProducerEpoch">Its epoch.</param>
    /// <param name="GroupId">The group.</param>
    /// <param name="MemberId">The member.</param>
    /// <param name="MemberEpoch">The member's epoch.</param>
    /// <param name="Resources">The offsets.</param>
    /// <param name="Tenant">The tenant of the ids.</param>
    public sealed record AddOffsetsToTxn(
        string TxnId,
        ulong ProducerId,
        ushort ProducerEpoch,
        string GroupId,
        string MemberId,
        int MemberEpoch,
        IReadOnlyList<OffsetCommitResource> Resources,
        string Tenant = DefaultTenant) : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.AddOffsetsToTxn;

        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.String(TxnId).U64(ProducerId).U16(ProducerEpoch).String(GroupId).String(MemberId).I32(MemberEpoch);
            PutCommits(w, Resources);
            PutTenant(w, Tenant);
        }

        internal static AddOffsetsToTxn Read(Reader r) =>
            new(r.String(), r.U64(), r.U16(), r.String(), r.String(), r.I32(), GetCommits(r), GetTenant(r));
    }

    /// <summary>Creates a stream (admin).</summary>
    /// <param name="Name">The name.</param>
    /// <param name="Tenant">The tenant.</param>
    /// <param name="Partitions">How many partitions.</param>
    /// <param name="Replication">How many replicas of each.</param>
    /// <param name="Configs">Settings.</param>
    public sealed record CreateStream(string Name, string Tenant, uint Partitions, byte Replication, IReadOnlyList<Config> Configs)
        : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.CreateStream;

        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.String(Name).String(Tenant).U32(Partitions).U8(Replication);
            PutConfigs(w, Configs);
            w.NoTags();
        }

        internal static CreateStream Read(Reader r)
        {
            var x = new CreateStream(r.String(), r.String(), r.U32(), r.U8(), GetConfigs(r));
            r.Tags();
            return x;
        }
    }

    /// <summary>Creates a queue (admin).</summary>
    /// <param name="Name">The name.</param>
    /// <param name="Tenant">The tenant.</param>
    /// <param name="PriorityLevels">0, or 2 to 10 priority levels.</param>
    /// <param name="SingleActive">Deliver to one subscription at a time.</param>
    /// <param name="Dlq">The dead-letter queue, or <c>null</c>.</param>
    /// <param name="Replication">How many replicas.</param>
    /// <param name="Configs">Settings.</param>
    public sealed record CreateQueue(
        string Name,
        string Tenant,
        byte PriorityLevels,
        bool SingleActive,
        string? Dlq,
        byte Replication,
        IReadOnlyList<Config> Configs) : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.CreateQueue;

        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.String(Name).String(Tenant).U8(PriorityLevels).Bool(SingleActive).OptString(Dlq).U8(Replication);
            PutConfigs(w, Configs);
            w.NoTags();
        }

        internal static CreateQueue Read(Reader r)
        {
            var x = new CreateQueue(r.String(), r.String(), r.U8(), r.Bool(), r.OptString(), r.U8(), GetConfigs(r));
            r.Tags();
            return x;
        }
    }

    /// <summary>Declares an exchange (admin).</summary>
    /// <param name="Name">The name.</param>
    /// <param name="Tenant">The tenant.</param>
    /// <param name="ExchangeType">See <see cref="Messages.ExchangeType"/>.</param>
    /// <param name="Configs">Settings.</param>
    public sealed record DeclareExchange(string Name, string Tenant, byte ExchangeType, IReadOnlyList<Config> Configs) : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.DeclareExchange;

        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.String(Name).String(Tenant).U8(ExchangeType);
            PutConfigs(w, Configs);
            w.NoTags();
        }

        internal static DeclareExchange Read(Reader r)
        {
            var x = new DeclareExchange(r.String(), r.String(), r.U8(), GetConfigs(r));
            r.Tags();
            return x;
        }
    }

    /// <summary>Binds a queue to an exchange (admin).</summary>
    /// <param name="ExchangeId">The exchange.</param>
    /// <param name="QueueId">The queue.</param>
    /// <param name="BindingKey">Matched against the routing key.</param>
    /// <param name="HeaderMatch">Headers to match (headers exchange).</param>
    /// <param name="MatchAll">All headers must match, or any one.</param>
    public sealed record Bind(
        ulong ExchangeId, ulong QueueId, string BindingKey, IReadOnlyList<HeaderMatch> HeaderMatch, bool MatchAll) : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.Bind;

        /// <inheritdoc/>
        public override void Write(Writer w) => WriteBinding(w, ExchangeId, QueueId, BindingKey, HeaderMatch, MatchAll);

        internal static Bind Read(Reader r)
        {
            var x = new Bind(r.U64(), r.U64(), r.String(), ReadHeaderMatches(r), r.Bool());
            r.Tags();
            return x;
        }
    }

    /// <summary>Removes a binding (admin).</summary>
    /// <param name="ExchangeId">The exchange.</param>
    /// <param name="QueueId">The queue.</param>
    /// <param name="BindingKey">Matched against the routing key.</param>
    /// <param name="HeaderMatch">Headers to match (headers exchange).</param>
    /// <param name="MatchAll">All headers must match, or any one.</param>
    public sealed record Unbind(
        ulong ExchangeId, ulong QueueId, string BindingKey, IReadOnlyList<HeaderMatch> HeaderMatch, bool MatchAll) : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.Unbind;

        /// <inheritdoc/>
        public override void Write(Writer w) => WriteBinding(w, ExchangeId, QueueId, BindingKey, HeaderMatch, MatchAll);

        internal static Unbind Read(Reader r)
        {
            var x = new Unbind(r.U64(), r.U64(), r.String(), ReadHeaderMatches(r), r.Bool());
            r.Tags();
            return x;
        }
    }

    private static void WriteBinding(
        Writer w, ulong exchangeId, ulong queueId, string key, IReadOnlyList<HeaderMatch> headers, bool matchAll)
    {
        w.U64(exchangeId).U64(queueId).String(key);
        w.Array(headers, HeaderMatch.Write);
        w.Bool(matchAll).NoTags();
    }

    private static List<HeaderMatch> ReadHeaderMatches(Reader r) => r.Array(5, HeaderMatch.Read);

    /// <summary>Deletes a resource created at runtime (admin).</summary>
    /// <param name="ResourceId">The resource.</param>
    /// <param name="IfEmpty">Only if it holds nothing (ADR 0063).</param>
    public sealed record DeleteResource(ulong ResourceId, bool IfEmpty) : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.DeleteResource;

        /// <inheritdoc/>
        public override void Write(Writer w) => w.U64(ResourceId).Bool(IfEmpty).NoTags();

        internal static DeleteResource Read(Reader r)
        {
            var x = new DeleteResource(r.U64(), r.Bool());
            r.Tags();
            return x;
        }
    }

    /// <summary>Pins a partition's leader to one of its voters, or unpins it (cluster admin, ADR 0065).</summary>
    /// <param name="ResourceId">The resource.</param>
    /// <param name="Partition">The partition.</param>
    /// <param name="NodeId">The node to pin to; 0 to unpin.</param>
    public sealed record MoveLeader(ulong ResourceId, uint Partition, ulong NodeId) : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.MoveLeader;

        /// <inheritdoc/>
        public override void Write(Writer w) => w.U64(ResourceId).U32(Partition).U64(NodeId).NoTags();

        internal static MoveLeader Read(Reader r)
        {
            var x = new MoveLeader(r.U64(), r.U32(), r.U64());
            r.Tags();
            return x;
        }
    }

    /// <summary>Publishes messages through exchanges; the node routes (ADR 0079).</summary>
    /// <param name="TxnId">Must be <c>null</c>.</param>
    /// <param name="TimeoutMs">How long the node waits for each queue.</param>
    /// <param name="Entries">The messages.</param>
    public sealed record Publish(string? TxnId, uint TimeoutMs, IReadOnlyList<PublishEntry> Entries) : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.Publish;

        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.OptString(TxnId).U32(TimeoutMs);
            w.Array(Entries, (w1, e) =>
            {
                w1.U64(e.ExchangeId).String(e.RoutingKey).Bool(e.Mandatory).Bytes(e.Batch);
                w1.NoTags();
            });
            w.NoTags();
        }

        internal static Publish Read(Reader r)
        {
            string? txnId = r.OptString();
            uint timeout = r.U32();
            List<PublishEntry> entries = r.Array(15, r1 =>
            {
                var e = new PublishEntry(r1.U64(), r1.String(), r1.Bool(), r1.Bytes());
                r1.Tags();
                return e;
            });
            r.Tags();
            return new Publish(txnId, timeout, entries);
        }
    }

    /// <summary>Raises a runtime stream's partition count (tenant admin, ADR 0074).</summary>
    /// <param name="ResourceId">The stream.</param>
    /// <param name="Count">The new partition count.</param>
    public sealed record CreatePartitions(ulong ResourceId, uint Count) : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.CreatePartitions;

        /// <inheritdoc/>
        public override void Write(Writer w) => w.U64(ResourceId).U32(Count).NoTags();

        internal static CreatePartitions Read(Reader r)
        {
            var x = new CreatePartitions(r.U64(), r.U32());
            r.Tags();
            return x;
        }
    }

    /// <summary>Ends a user's or group's sessions and refuses their tokens issued before a time (ADR 0070).</summary>
    /// <param name="SubjectKind">See <see cref="Messages.SubjectKind"/>.</param>
    /// <param name="Subject">The user or group.</param>
    /// <param name="BeforeMs">Tokens issued before this time, in milliseconds since the epoch, are refused.</param>
    public sealed record RevokeSessions(byte SubjectKind, string Subject, ulong BeforeMs) : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.RevokeSessions;

        /// <inheritdoc/>
        public override void Write(Writer w) => w.U8(SubjectKind).String(Subject).U64(BeforeMs).NoTags();

        internal static RevokeSessions Read(Reader r)
        {
            var x = new RevokeSessions(r.U8(), r.String(), r.U64());
            r.Tags();
            return x;
        }
    }

    /// <summary>Adds a certificate to the cluster's revocation list.</summary>
    /// <param name="Issuer">SHA-256 of the issuer's distinguished name in DER form (32 bytes).</param>
    /// <param name="Serial">The serial number, big-endian, as the certificate has it.</param>
    /// <param name="NotAfterMs">When the certificate stops being valid, in milliseconds since the epoch; the entry is dropped after that.</param>
    /// <param name="Reason">Why, for the people who read the list later; may be empty.</param>
    public sealed record RevokeCertificate(ByteString Issuer, ByteString Serial, ulong NotAfterMs, string Reason) : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.RevokeCertificate;

        /// <inheritdoc/>
        public override void Write(Writer w) => w.Bytes(Issuer).Bytes(Serial).U64(NotAfterMs).String(Reason).NoTags();

        internal static RevokeCertificate Read(Reader r)
        {
            var x = new RevokeCertificate(r.Bytes(), r.Bytes(), r.U64(), r.String());
            r.Tags();
            return x;
        }
    }

    /// <summary>Lists the client certificates the cluster has seen or had revoked (cluster admin, ADR 0091).</summary>
    public sealed record DescribeCertificates : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.DescribeCertificates;

        /// <inheritdoc/>
        public override void Write(Writer w) => w.NoTags();

        internal static DescribeCertificates Read(Reader r)
        {
            r.Tags();
            return new DescribeCertificates();
        }
    }

    /// <summary>
    /// A node reports client certificates it accepted. Internal: only broker nodes may send it, and the client has no
    /// method for it (ADR 0091). Answered with <see cref="Response.Admin"/>.
    /// </summary>
    /// <param name="Certificates">The certificates, at most a few hundred.</param>
    public sealed record RecordCertificates(IReadOnlyList<Messages.SeenCertificate> Certificates) : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.RecordCertificates;

        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.Array(Certificates, (w1, c) => w1.Bytes(c.Issuer).Bytes(c.Serial).String(c.Principal)
                .U64(c.NotBeforeMs).U64(c.NotAfterMs).U64(c.SeenMs));
            w.NoTags();
        }

        internal static RecordCertificates Read(Reader r)
        {
            // issuer, serial, principal: 4 + 4 + 2 bytes at least, then three u64
            List<Messages.SeenCertificate> certificates = r.Array(
                34, r1 => new Messages.SeenCertificate(r1.Bytes(), r1.Bytes(), r1.String(), r1.U64(), r1.U64(), r1.U64()));
            r.Tags();
            return new RecordCertificates(certificates);
        }
    }

    /// <summary>Purges a queue's ready messages, or moves a dead-letter queue's back (ADR 0064).</summary>
    /// <param name="Requeue"><c>true</c>: requeue (key 48); <c>false</c>: purge (key 47).</param>
    /// <param name="QueueId">The queue.</param>
    /// <param name="UpTo">0 for the first round, then the <c>up_to</c> the last answer gave.</param>
    public sealed record QueueAction(bool Requeue, ulong QueueId, ulong UpTo) : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => Requeue ? ApiKey.Requeue : ApiKey.PurgeQueue;

        /// <inheritdoc/>
        public override void Write(Writer w) => w.U64(QueueId).U64(UpTo).NoTags();

        internal static QueueAction Read(Reader r, bool requeue)
        {
            var x = new QueueAction(requeue, r.U64(), r.U64());
            r.Tags();
            return x;
        }
    }

    /// <summary>Reads resources' settings (admin).</summary>
    /// <param name="ResourceIds">The resources.</param>
    public sealed record DescribeConfigs(IReadOnlyList<ulong> ResourceIds) : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.DescribeConfigs;

        /// <inheritdoc/>
        public override void Write(Writer w) => w.Array(ResourceIds, (w1, id) => w1.U64(id)).NoTags();

        internal static DescribeConfigs Read(Reader r)
        {
            var x = new DescribeConfigs(r.Array(8, r1 => r1.U64()));
            r.Tags();
            return x;
        }
    }

    /// <summary>Changes a resource's settings (admin).</summary>
    /// <param name="ResourceId">The resource.</param>
    /// <param name="Set">Settings to set.</param>
    /// <param name="Delete">Settings to remove.</param>
    public sealed record AlterConfigs(ulong ResourceId, IReadOnlyList<Config> Set, IReadOnlyList<string> Delete) : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.AlterConfigs;

        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.U64(ResourceId);
            PutConfigs(w, Set);
            w.Array(Delete, (w1, k) => w1.String(k)).NoTags();
        }

        internal static AlterConfigs Read(Reader r)
        {
            var x = new AlterConfigs(r.U64(), GetConfigs(r), r.Array(2, r1 => r1.String()));
            r.Tags();
            return x;
        }
    }

    /// <summary>One node's metrics (admin).</summary>
    public sealed record DescribeMetrics : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.DescribeMetrics;

        /// <inheritdoc/>
        public override void Write(Writer w) => w.NoTags();

        internal static DescribeMetrics Read(Reader r)
        {
            r.Tags();
            return new DescribeMetrics();
        }
    }

    /// <summary>Chooses a SASL mechanism.</summary>
    /// <param name="Mechanism">The mechanism's name.</param>
    public sealed record SaslHandshake(string Mechanism) : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.SaslHandshake;

        /// <inheritdoc/>
        public override void Write(Writer w) => w.String(Mechanism).NoTags();

        internal static SaslHandshake Read(Reader r)
        {
            var x = new SaslHandshake(r.String());
            r.Tags();
            return x;
        }
    }

    /// <summary>One step of SASL authentication.</summary>
    /// <param name="AuthBytes">The mechanism's message.</param>
    public sealed record SaslAuthenticate(ByteString AuthBytes) : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.SaslAuthenticate;

        /// <inheritdoc/>
        public override void Write(Writer w) => w.Bytes(AuthBytes).NoTags();

        internal static SaslAuthenticate Read(Reader r)
        {
            var x = new SaslAuthenticate(r.Bytes());
            r.Tags();
            return x;
        }
    }

    /// <summary>Sets or removes a user's SCRAM credential (admin).</summary>
    /// <param name="User">The user.</param>
    /// <param name="Credential">The credential to set, or <c>null</c> to remove the user's.</param>
    public sealed record AlterUser(string User, ScramCredentialData? Credential) : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.AlterUser;

        /// <inheritdoc/>
        public override void Write(Writer w)
        {
            w.String(User);
            if (Credential is null)
            {
                w.U8(0);
            }
            else
            {
                w.U8(1).U32(Credential.Iterations);
                w.Bytes(Credential.Salt).Bytes(Credential.StoredKey).Bytes(Credential.ServerKey);
            }
            w.NoTags();
        }

        internal static AlterUser Read(Reader r)
        {
            string user = r.String();
            ScramCredentialData? credential;
            switch (r.U8())
            {
                case 0:
                    credential = null;
                    break;
                case 1:
                    credential = new ScramCredentialData(r.U32(), r.Bytes(), r.Bytes(), r.Bytes());
                    break;
                default:
                    throw DecodeException.Malformed("credential flag is neither 0 nor 1");
            }
            r.Tags();
            return new AlterUser(user, credential);
        }
    }

    /// <summary>Lists the users with a SCRAM credential (admin).</summary>
    public sealed record DescribeUsers : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.DescribeUsers;

        /// <inheritdoc/>
        public override void Write(Writer w) => w.NoTags();

        internal static DescribeUsers Read(Reader r)
        {
            r.Tags();
            return new DescribeUsers();
        }
    }

    /// <summary>Creates a tenant (cluster admin).</summary>
    /// <param name="Name">The name.</param>
    public sealed record CreateTenant(string Name) : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.CreateTenant;

        /// <inheritdoc/>
        public override void Write(Writer w) => w.String(Name).NoTags();

        internal static CreateTenant Read(Reader r)
        {
            var x = new CreateTenant(r.String());
            r.Tags();
            return x;
        }
    }

    /// <summary>Deletes an empty tenant (cluster admin).</summary>
    /// <param name="Name">The name.</param>
    public sealed record DeleteTenant(string Name) : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.DeleteTenant;

        /// <inheritdoc/>
        public override void Write(Writer w) => w.String(Name).NoTags();

        internal static DeleteTenant Read(Reader r)
        {
            var x = new DeleteTenant(r.String());
            r.Tags();
            return x;
        }
    }

    /// <summary>Lists tenants.</summary>
    public sealed record DescribeTenants : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.DescribeTenants;

        /// <inheritdoc/>
        public override void Write(Writer w) => w.NoTags();

        internal static DescribeTenants Read(Reader r)
        {
            r.Tags();
            return new DescribeTenants();
        }
    }

    /// <summary>Adds and removes role bindings (cluster admin).</summary>
    /// <param name="Add">Bindings to add.</param>
    /// <param name="Remove">Bindings to remove.</param>
    public sealed record AlterRoleBindings(IReadOnlyList<RoleBinding> Add, IReadOnlyList<RoleBinding> Remove) : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.AlterRoleBindings;

        /// <inheritdoc/>
        public override void Write(Writer w) => w.Array(Add, RoleBinding.Write).Array(Remove, RoleBinding.Write).NoTags();

        internal static AlterRoleBindings Read(Reader r)
        {
            var x = new AlterRoleBindings(
                r.Array(BindingMin, RoleBinding.Read), r.Array(BindingMin, RoleBinding.Read));
            r.Tags();
            return x;
        }
    }

    /// <summary>Lists role bindings (cluster admin).</summary>
    public sealed record DescribeRoleBindings : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.DescribeRoleBindings;

        /// <inheritdoc/>
        public override void Write(Writer w) => w.NoTags();

        internal static DescribeRoleBindings Read(Reader r)
        {
            r.Tags();
            return new DescribeRoleBindings();
        }
    }

    /// <summary>Asks who the connection acts as.</summary>
    public sealed record WhoAmI : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.WhoAmI;

        /// <inheritdoc/>
        public override void Write(Writer w) => w.NoTags();

        internal static WhoAmI Read(Reader r)
        {
            r.Tags();
            return new WhoAmI();
        }
    }

    /// <summary>Adds a node to the cluster (cluster admin).</summary>
    /// <param name="NodeId">The node's id.</param>
    /// <param name="Peer">Where the other nodes reach it.</param>
    /// <param name="Client">Where clients reach it.</param>
    public sealed record AddNode(ulong NodeId, string Peer, string Client) : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.AddNode;

        /// <inheritdoc/>
        public override void Write(Writer w) => w.U64(NodeId).String(Peer).String(Client).NoTags();

        internal static AddNode Read(Reader r)
        {
            var x = new AddNode(r.U64(), r.String(), r.String());
            r.Tags();
            return x;
        }
    }

    /// <summary>Drains a node and then removes it (cluster admin).</summary>
    /// <param name="NodeId">The node's id.</param>
    public sealed record RemoveNode(ulong NodeId) : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.RemoveNode;

        /// <inheritdoc/>
        public override void Write(Writer w) => w.U64(NodeId).NoTags();

        internal static RemoveNode Read(Reader r)
        {
            var x = new RemoveNode(r.U64());
            r.Tags();
            return x;
        }
    }

    /// <summary>Lists the cluster's nodes (cluster viewers).</summary>
    public sealed record DescribeNodes : Request
    {
        /// <inheritdoc/>
        public override ApiKey ApiKey => ApiKey.DescribeNodes;

        /// <inheritdoc/>
        public override void Write(Writer w) => w.NoTags();

        internal static DescribeNodes Read(Reader r)
        {
            r.Tags();
            return new DescribeNodes();
        }
    }
}
