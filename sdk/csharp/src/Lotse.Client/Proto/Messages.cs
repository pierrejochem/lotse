using System;
using System.Collections.Generic;
using Lotse.Record;

namespace Lotse.Proto;

/// <summary>
/// The structures that requests and responses share or contain, and the constants their fields take. Each
/// structure has a <c>Write</c> and a <c>Read</c> for its fields in the layout the broker uses. The messages
/// themselves are the nested types of <see cref="Request"/> and <see cref="Response"/>.
/// </summary>
public static class Messages
{
    /// <summary>
    /// The offset of "no committed offset": <c>u64::MAX</c>. Offsets are <c>ulong</c> in this SDK, so this is
    /// <c>ulong.MaxValue</c>; <see cref="NoOffsetSigned"/> is the same bit pattern as a signed value.
    /// </summary>
    public const ulong NoOffset = ulong.MaxValue;

    /// <summary>The same value as <see cref="NoOffset"/> seen as a signed 64-bit integer: -1.</summary>
    public const long NoOffsetSigned = -1L;

    /// <summary>Buckets of the commit latency histogram: bucket <c>i</c> counts produces acknowledged in under <c>2^i</c> ms, the last all slower ones.</summary>
    public const int LatencyBuckets = 17;

    /// <summary>The tenant that always exists, and the one a request means when it names none.</summary>
    public const string DefaultTenant = "default";

    /// <summary>Tag number of the <see cref="LeaderHint"/> in a response.</summary>
    internal const long TagLeaderHint = 0;

    /// <summary>Tag number of the reason in a frame-level error.</summary>
    internal const long TagReason = 0;

    /// <summary>Tag of the tenant in a request that names a transactional or group id (ADR 0062).</summary>
    internal const long TagTenant = 1;

    /// <summary>Tag of a group member's instance id (ADR 0072).</summary>
    internal const long TagInstance = 2;

    /// <summary>Tag of a group member's subscription pattern (ADR 0073).</summary>
    internal const long TagPattern = 3;

    /// <summary>Tag of a subscription that asks to hear it became the single active consumer (ADR 0076).</summary>
    internal const long TagNotifyActive = 1;

    /// <summary>Tag of a subscription that waits as a standby, next to the leader hint (ADR 0076).</summary>
    internal const long TagStandby = 1;

    /// <summary>Tag of a transactional ack's producer id and epoch (ADR 0075).</summary>
    internal const long TagProducer = 2;

    /// <summary>The smallest <see cref="RoleBinding"/>: two flags and three empty strings.</summary>
    internal const int BindingMin = 1 + 2 + 1 + 2 + 2;

    // ---- constants ----

    /// <summary>Kinds of resources in a <see cref="ResourceMetadata"/>.</summary>
    public static class ResourceKind
    {
        /// <summary>A stream with partitions.</summary>
        public const byte Stream = 0;

        /// <summary>A queue: one partition, 0.</summary>
        public const byte Queue = 1;

        /// <summary>An exchange: routes into queues, has no partitions.</summary>
        public const byte Exchange = 2;
    }

    /// <summary>Types of exchanges.</summary>
    public static class ExchangeType
    {
        /// <summary>The binding key equals the routing key.</summary>
        public const byte Direct = 0;

        /// <summary><c>.</c>-separated words, <c>*</c> one word, <c>#</c> zero or more.</summary>
        public const byte Topic = 1;

        /// <summary>Every bound queue.</summary>
        public const byte Fanout = 2;

        /// <summary>By message headers.</summary>
        public const byte Headers = 3;
    }

    /// <summary>Isolation levels for a fetch.</summary>
    public static class Isolation
    {
        /// <summary>Everything up to the high watermark.</summary>
        public const byte Uncommitted = 0;

        /// <summary>Up to the last stable offset; aborted transactions are listed.</summary>
        public const byte Committed = 1;
    }

    /// <summary>What a <c>FindCoordinator</c> key names.</summary>
    public static class CoordinatorType
    {
        /// <summary>A consumer group.</summary>
        public const byte Group = 0;

        /// <summary>A producer (idempotent or transactional); the key is the transactional id, or empty.</summary>
        public const byte Producer = 1;
    }

    /// <summary>Where a setting, user, tenant or binding comes from.</summary>
    public static class ConfigSource
    {
        /// <summary>Not set: the default.</summary>
        public const byte Default = 0;

        /// <summary>Set at runtime.</summary>
        public const byte Dynamic = 1;

        /// <summary>From the node configuration; cannot be changed at runtime.</summary>
        public const byte Static = 2;
    }

    /// <summary>Kinds of subjects in a <see cref="RoleBinding"/>.</summary>
    public static class SubjectKind
    {
        /// <summary>A principal by name.</summary>
        public const byte User = 0;

        /// <summary>A group at the identity provider.</summary>
        public const byte Group = 1;
    }

    /// <summary>Roles in a <see cref="RoleBinding"/>.</summary>
    public static class Role
    {
        /// <summary>Everything, everywhere.</summary>
        public const byte ClusterAdmin = 0;

        /// <summary>Creating, changing, deleting and binding resources in a tenant, and what the roles below may.</summary>
        public const byte TenantAdmin = 1;

        /// <summary>Writing to resources.</summary>
        public const byte Producer = 2;

        /// <summary>Reading from resources.</summary>
        public const byte Consumer = 3;

        /// <summary>Seeing resources.</summary>
        public const byte Viewer = 4;

        /// <summary>Seeing everything, everywhere, and nothing else.</summary>
        public const byte ClusterViewer = 5;
    }

    /// <summary>Kinds of principals in a <see cref="Response.WhoAmI"/>.</summary>
    public static class PrincipalKind
    {
        /// <summary>Not authenticated, on a node that does not require it: may do everything.</summary>
        public const byte Anonymous = 0;

        /// <summary>A broker node.</summary>
        public const byte Node = 1;

        /// <summary>A user or service.</summary>
        public const byte User = 2;
    }

    /// <summary>States of a node in a <see cref="NodeEntry"/>.</summary>
    public static class NodeState
    {
        /// <summary>Holds replicas; new ones may be placed on it.</summary>
        public const byte Active = 0;

        /// <summary>Its replicas are moving to other nodes; then it leaves.</summary>
        public const byte Draining = 1;
    }

    /// <summary>What <see cref="ListOffsetsPartition.Timestamp"/> may ask for besides a time.</summary>
    public static class ListOffsets
    {
        /// <summary>The offset after the last one a reader may see: the high watermark, or the last stable offset.</summary>
        public const long Latest = -1;

        /// <summary>The first offset still in the log.</summary>
        public const long Earliest = -2;
    }

    // ---- leader hint: a block of tagged fields ----

    /// <summary>Writes a leader hint as a block of tagged fields: tag 0 holds it, no hint is an empty block.</summary>
    internal static void PutHint(Writer w, LeaderHint? hint)
    {
        if (hint is null)
        {
            w.NoTags();
            return;
        }
        var v = new Writer();
        v.U32(hint.Leader);
        v.U64(hint.Term);
        w.Tags(new[] { new Tag(TagLeaderHint, ByteString.CopyOf(v.ToByteArray())) });
    }

    /// <summary>Reads a block of tagged fields and picks the leader hint out of it; other tags are skipped.</summary>
    internal static LeaderHint? GetHint(Reader r)
    {
        LeaderHint? hint = null;
        foreach (Tag t in r.Tags())
        {
            if (t.Number == TagLeaderHint)
            {
                var vr = new Reader(t.Value.ToByteArray());
                hint = new LeaderHint(vr.U32(), vr.U64());
                vr.Finish();
            }
        }
        return hint;
    }

    // ---- tenant of an id: a block of tagged fields ----

    /// <summary>A tag holding one string.</summary>
    internal static Tag StringTag(long tag, string value)
    {
        var v = new Writer();
        v.String(value);
        return new Tag(tag, ByteString.CopyOf(v.ToByteArray()));
    }

    /// <summary>Writes a request's tags: none in the default tenant, otherwise tag 1 holding the tenant.</summary>
    internal static void PutTenant(Writer w, string tenant)
    {
        if (DefaultTenant == tenant)
        {
            w.NoTags();
            return;
        }
        w.Tags(new[] { StringTag(TagTenant, tenant) });
    }

    /// <summary>Reads a request's tags and picks the tenant out of them; other tags are skipped.</summary>
    internal static string GetTenant(Reader r)
    {
        string tenant = DefaultTenant;
        foreach (Tag t in r.Tags())
        {
            if (t.Number == TagTenant)
            {
                var vr = new Reader(t.Value.ToByteArray());
                tenant = vr.String();
                vr.Finish();
            }
        }
        return tenant;
    }

    // ---- small shared structures ----

    /// <summary>A pair of strings, for settings.</summary>
    /// <param name="Key">The setting.</param>
    /// <param name="Value">Its value.</param>
    public sealed record Config(string Key, string Value)
    {
        internal static void Write(Writer w, Config c) => w.String(c.Key).String(c.Value);

        internal static Config Read(Reader r) => new(r.String(), r.String());
    }

    internal static void PutConfigs(Writer w, IReadOnlyList<Config> configs) => w.Array(configs, Config.Write);

    internal static List<Config> GetConfigs(Reader r) => r.Array(4, Config.Read);

    /// <summary>An offset range <c>[from, to)</c>, both <c>u64</c>.</summary>
    /// <param name="From">First offset.</param>
    /// <param name="To">Offset after the last.</param>
    public sealed record OffsetRange(ulong From, ulong To)
    {
        internal static void Write(Writer w, OffsetRange x) => w.U64(x.From).U64(x.To);

        internal static OffsetRange Read(Reader r) => new(r.U64(), r.U64());
    }

    internal static void PutRanges(Writer w, IReadOnlyList<OffsetRange> ranges) => w.Array(ranges, OffsetRange.Write);

    internal static List<OffsetRange> GetRanges(Reader r) => r.Array(16, OffsetRange.Read);

    /// <summary>A match on one header of a binding.</summary>
    /// <param name="Key">The header's name.</param>
    /// <param name="Value">The value to match, or <c>null</c> to match a header whose value is null.</param>
    public sealed record HeaderMatch(ByteString Key, ByteString? Value)
    {
        internal static void Write(Writer w, HeaderMatch h)
        {
            w.Bytes(h.Key);
            w.Bool(h.Value is not null);
            if (h.Value is not null)
            {
                w.Bytes(h.Value);
            }
        }

        internal static HeaderMatch Read(Reader r)
        {
            ByteString k = r.Bytes();
            return new HeaderMatch(k, r.Bool() ? r.Bytes() : null);
        }
    }

    // ---- partitions in requests and responses ----

    /// <summary>Partitions of one resource.</summary>
    /// <param name="ResourceId">The resource.</param>
    /// <param name="Partitions">Partition numbers.</param>
    public sealed record TxnResource(ulong ResourceId, IReadOnlyList<uint> Partitions)
    {
        internal static void Write(Writer w, TxnResource t)
        {
            w.U64(t.ResourceId);
            w.Array(t.Partitions, (w2, p) => w2.U32(p));
            w.NoTags();
        }

        internal static TxnResource Read(Reader r)
        {
            var t = new TxnResource(r.U64(), r.Array(4, r2 => r2.U32()));
            r.Tags();
            return t;
        }
    }

    internal static void PutTxnResources(Writer w, IReadOnlyList<TxnResource> resources) =>
        w.Array(resources, TxnResource.Write);

    internal static List<TxnResource> GetTxnResources(Reader r) => r.Array(10, TxnResource.Read);

    internal static void PutOptResources(Writer w, IReadOnlyList<TxnResource>? resources)
    {
        w.Bool(resources is not null);
        if (resources is not null)
        {
            PutTxnResources(w, resources);
        }
    }

    internal static List<TxnResource>? GetOptResources(Reader r) => r.Bool() ? GetTxnResources(r) : null;

    /// <summary>The outcome for one partition of a transaction or commit request.</summary>
    /// <param name="Partition">Partition number.</param>
    /// <param name="Error">The outcome.</param>
    public sealed record TxnPartitionResult(uint Partition, ErrorCode Error);

    /// <summary>Outcomes for one resource.</summary>
    /// <param name="ResourceId">The resource.</param>
    /// <param name="Partitions">Per partition.</param>
    public sealed record TxnResourceResult(ulong ResourceId, IReadOnlyList<TxnPartitionResult> Partitions)
    {
        internal static void Write(Writer w, TxnResourceResult t)
        {
            w.U64(t.ResourceId);
            w.Array(t.Partitions, (w2, p) =>
            {
                w2.U32(p.Partition);
                w2.U16(p.Error.Code);
                w2.NoTags();
            });
            w.NoTags();
        }

        internal static TxnResourceResult Read(Reader r)
        {
            ulong id = r.U64();
            List<TxnPartitionResult> partitions = r.Array(7, r2 =>
            {
                var p = new TxnPartitionResult(r2.U32(), new ErrorCode(r2.U16()));
                r2.Tags();
                return p;
            });
            r.Tags();
            return new TxnResourceResult(id, partitions);
        }
    }

    internal static void PutTxnResults(Writer w, IReadOnlyList<TxnResourceResult> results) =>
        w.Array(results, TxnResourceResult.Write);

    internal static List<TxnResourceResult> GetTxnResults(Reader r) => r.Array(10, TxnResourceResult.Read);

    // ---- metadata ----

    /// <summary>A broker node.</summary>
    /// <param name="NodeId">Node id.</param>
    /// <param name="Host">Host clients connect to.</param>
    /// <param name="Port">Port clients connect to.</param>
    /// <param name="Rack">Rack, or empty.</param>
    public sealed record Broker(uint NodeId, string Host, ushort Port, string Rack);

    /// <summary>One partition of a resource.</summary>
    /// <param name="Partition">Partition number.</param>
    /// <param name="Leader">Leader node; <c>0</c> if none is known.</param>
    /// <param name="LeaderTerm">The leader's term.</param>
    /// <param name="Voters">Voters of the partition's Raft group.</param>
    public sealed record PartitionMetadata(uint Partition, uint Leader, ulong LeaderTerm, IReadOnlyList<uint> Voters);

    /// <summary>A queue bound to an exchange.</summary>
    /// <param name="QueueId">The queue.</param>
    /// <param name="BindingKey">Matched against the routing key (direct, topic).</param>
    /// <param name="HeaderMatch">Headers to match (headers exchange).</param>
    /// <param name="MatchAll">All headers must match (<c>true</c>) or any one.</param>
    public sealed record BindingMetadata(
        ulong QueueId, string BindingKey, IReadOnlyList<HeaderMatch> HeaderMatch, bool MatchAll);

    /// <summary>An exchange's type and bindings: clients route with them.</summary>
    /// <param name="ExchangeType">See <see cref="Messages.ExchangeType"/>.</param>
    /// <param name="Bindings">The bound queues.</param>
    public sealed record ExchangeMetadata(byte ExchangeType, IReadOnlyList<BindingMetadata> Bindings);

    /// <summary>A resource.</summary>
    /// <param name="Error"><see cref="ErrorCode.UnknownResource"/> for a name that does not exist.</param>
    /// <param name="Name">The name.</param>
    /// <param name="Tenant">The tenant it belongs to; empty for a name that does not exist.</param>
    /// <param name="ResourceId">The id used by data-path requests.</param>
    /// <param name="Kind">See <see cref="ResourceKind"/>.</param>
    /// <param name="Partitions">The partitions.</param>
    /// <param name="Exchange">For an exchange: its type and bindings, otherwise <c>null</c>.</param>
    public sealed record ResourceMetadata(
        ErrorCode Error,
        string Name,
        string Tenant,
        ulong ResourceId,
        byte Kind,
        IReadOnlyList<PartitionMetadata> Partitions,
        ExchangeMetadata? Exchange);

    // ---- produce and fetch ----

    /// <summary>A batch for one partition.</summary>
    /// <param name="Partition">Partition number.</param>
    /// <param name="Batch">One record batch, as built by the producer.</param>
    public sealed record ProducePartition(uint Partition, ByteString Batch);

    /// <summary>Batches for one resource.</summary>
    /// <param name="ResourceId">The resource.</param>
    /// <param name="Partitions">Its partitions.</param>
    public sealed record ProduceResource(ulong ResourceId, IReadOnlyList<ProducePartition> Partitions);

    /// <summary>The outcome for one partition of a produce.</summary>
    /// <param name="Partition">Partition number.</param>
    /// <param name="Error">The outcome.</param>
    /// <param name="BaseOffset">Offset of the batch's first record.</param>
    /// <param name="AppendTs">Append time in milliseconds since the epoch, or <c>-1</c>.</param>
    /// <param name="ErrorMsg">Details for humans, or <c>null</c>.</param>
    /// <param name="LeaderHint">The leader, with <see cref="ErrorCode.NotLeader"/>, or <c>null</c>.</param>
    public sealed record ProducePartitionResponse(
        uint Partition, ErrorCode Error, ulong BaseOffset, long AppendTs, string? ErrorMsg, LeaderHint? LeaderHint);

    /// <summary>Outcomes for one resource.</summary>
    /// <param name="ResourceId">The resource.</param>
    /// <param name="Partitions">Its partitions.</param>
    public sealed record ProduceResourceResponse(ulong ResourceId, IReadOnlyList<ProducePartitionResponse> Partitions);

    /// <summary>Where to read in one partition.</summary>
    /// <param name="Partition">Partition number.</param>
    /// <param name="FetchOffset">First offset wanted.</param>
    /// <param name="LeaderTerm">The leader term the client knows; <c>0</c> not to check.</param>
    /// <param name="MaxBytes">Byte limit for this partition (at least one batch is returned).</param>
    public sealed record FetchPartition(uint Partition, ulong FetchOffset, ulong LeaderTerm, uint MaxBytes);

    /// <summary>Partitions to read of one resource.</summary>
    /// <param name="ResourceId">The resource.</param>
    /// <param name="Partitions">Its partitions.</param>
    public sealed record FetchResource(ulong ResourceId, IReadOnlyList<FetchPartition> Partitions);

    /// <summary>A transaction aborted inside the returned range.</summary>
    /// <param name="ProducerId">Its producer.</param>
    /// <param name="FirstOffset">Its first offset.</param>
    public sealed record AbortedTxn(ulong ProducerId, ulong FirstOffset);

    /// <summary>One partition to look up with <c>ListOffsets</c>.</summary>
    /// <param name="Partition">Partition number.</param>
    /// <param name="Timestamp">Milliseconds since the epoch, or <see cref="ListOffsets.Latest"/> or <see cref="ListOffsets.Earliest"/>.</param>
    public sealed record ListOffsetsPartition(uint Partition, long Timestamp);

    /// <summary>The partitions of one stream to look up.</summary>
    /// <param name="ResourceId">The stream.</param>
    /// <param name="Partitions">Its partitions.</param>
    public sealed record ListOffsetsResource(ulong ResourceId, IReadOnlyList<ListOffsetsPartition> Partitions);

    /// <summary>The answer for one partition.</summary>
    /// <param name="Partition">Partition number.</param>
    /// <param name="Error">Outcome.</param>
    /// <param name="Offset">For a time, the first offset of the first data batch with a record at or after it, or the latest offset if there is none.</param>
    /// <param name="Timestamp">For a time lookup that found a batch, its newest record's timestamp; -1 otherwise.</param>
    /// <param name="LeaderHint">The leader, with <see cref="ErrorCode.NotLeader"/>; <c>null</c> if none.</param>
    public sealed record ListOffsetsPartitionResponse(
        uint Partition, ErrorCode Error, ulong Offset, long Timestamp, LeaderHint? LeaderHint);

    /// <summary>The answers for one stream.</summary>
    /// <param name="ResourceId">The stream.</param>
    /// <param name="Partitions">Its partitions.</param>
    public sealed record ListOffsetsResourceResponse(
        ulong ResourceId, IReadOnlyList<ListOffsetsPartitionResponse> Partitions);

    /// <summary>What was read from one partition.</summary>
    /// <param name="Partition">Partition number.</param>
    /// <param name="Error">The outcome.</param>
    /// <param name="HighWatermark">Offsets below this are committed.</param>
    /// <param name="LastStableOffset">Offsets below this are committed and not in an open transaction.</param>
    /// <param name="LogStartOffset">First offset still in the log.</param>
    /// <param name="AbortedTxns">Aborted transactions in <paramref name="Records"/>, for read-committed.</param>
    /// <param name="Records">Whole record batches as stored, control batches included.</param>
    /// <param name="LeaderHint">The leader, with <see cref="ErrorCode.NotLeader"/>, or <c>null</c>.</param>
    public sealed record FetchPartitionResponse(
        uint Partition,
        ErrorCode Error,
        ulong HighWatermark,
        ulong LastStableOffset,
        ulong LogStartOffset,
        IReadOnlyList<AbortedTxn> AbortedTxns,
        ByteString Records,
        LeaderHint? LeaderHint);

    /// <summary>What was read from one resource.</summary>
    /// <param name="ResourceId">The resource.</param>
    /// <param name="Partitions">Its partitions.</param>
    public sealed record FetchResourceResponse(ulong ResourceId, IReadOnlyList<FetchPartitionResponse> Partitions);

    // ---- queues ----

    /// <summary>One message of a delivered batch, for this subscription.</summary>
    /// <param name="Offset">The message's offset.</param>
    /// <param name="DeliveryCount">How often it has been delivered, this time included.</param>
    public sealed record DeliveredOffset(ulong Offset, ushort DeliveryCount);

    // ---- publishing (ADR 0079) ----

    /// <summary>One message to publish.</summary>
    /// <param name="ExchangeId">The exchange.</param>
    /// <param name="RoutingKey">The routing key.</param>
    /// <param name="Mandatory">Refuse the message with <c>UNROUTABLE</c> if it routes to no queue, rather than drop it.</param>
    /// <param name="Batch">A batch of exactly one record, the message; its headers route it through a headers exchange.</param>
    public sealed record PublishEntry(ulong ExchangeId, string RoutingKey, bool Mandatory, ByteString Batch);

    /// <summary>What became of one published message.</summary>
    /// <param name="Error"><c>NONE</c> once routed (each queue's outcome is in <paramref name="Routed"/>); otherwise why nothing was written.</param>
    /// <param name="Routed">The queues it routes to.</param>
    public sealed record PublishResult(ErrorCode Error, IReadOnlyList<RoutedMessage> Routed);

    /// <summary>One queue's copy of a published message; <paramref name="Name"/> is empty when the node no longer knows the queue.</summary>
    /// <param name="QueueId">The queue.</param>
    /// <param name="Name">Its name.</param>
    /// <param name="Error"><c>NONE</c>: the queue holds it at <paramref name="BaseOffset"/>; <c>REQUEST_TIMED_OUT</c>: unknown; anything else: it does not.</param>
    /// <param name="BaseOffset">Where, with <c>NONE</c>.</param>
    public sealed record RoutedMessage(ulong QueueId, string Name, ErrorCode Error, ulong BaseOffset);

    // ---- consumer groups ----

    /// <summary>An offset to commit for one partition.</summary>
    /// <param name="Partition">Partition number.</param>
    /// <param name="Offset">The next offset to read.</param>
    /// <param name="LeaderTerm">The leader term the offset was read under, or 0.</param>
    /// <param name="Metadata">Free text kept with the offset, or <c>null</c>.</param>
    public sealed record OffsetCommitPartition(uint Partition, ulong Offset, ulong LeaderTerm, string? Metadata);

    /// <summary>Offsets to commit for one resource.</summary>
    /// <param name="ResourceId">The resource.</param>
    /// <param name="Partitions">Its partitions.</param>
    public sealed record OffsetCommitResource(ulong ResourceId, IReadOnlyList<OffsetCommitPartition> Partitions);

    internal static void PutCommits(Writer w, IReadOnlyList<OffsetCommitResource> resources) =>
        w.Array(resources, (w1, r) =>
        {
            w1.U64(r.ResourceId);
            w1.Array(r.Partitions, (w2, p) =>
            {
                w2.U32(p.Partition);
                w2.U64(p.Offset);
                w2.U64(p.LeaderTerm);
                w2.OptString(p.Metadata);
                w2.NoTags();
            });
            w1.NoTags();
        });

    internal static List<OffsetCommitResource> GetCommits(Reader r) =>
        r.Array(10, r1 =>
        {
            ulong id = r1.U64();
            List<OffsetCommitPartition> partitions = r1.Array(23, r2 =>
            {
                var p = new OffsetCommitPartition(r2.U32(), r2.U64(), r2.U64(), r2.OptString());
                r2.Tags();
                return p;
            });
            r1.Tags();
            return new OffsetCommitResource(id, partitions);
        });

    /// <summary>One partition's committed offset.</summary>
    /// <param name="Partition">Partition number.</param>
    /// <param name="Error">The outcome.</param>
    /// <param name="Offset">The next offset to read, or <see cref="NoOffset"/>.</param>
    /// <param name="LeaderTerm">The leader term it was committed with.</param>
    /// <param name="Metadata">What was kept with it, or <c>null</c>.</param>
    public sealed record OffsetFetchPartition(uint Partition, ErrorCode Error, ulong Offset, ulong LeaderTerm, string? Metadata);

    /// <summary>One resource's committed offsets.</summary>
    /// <param name="ResourceId">The resource.</param>
    /// <param name="Partitions">Its partitions.</param>
    public sealed record OffsetFetchResource(ulong ResourceId, IReadOnlyList<OffsetFetchPartition> Partitions);

    // ---- configs ----

    /// <summary>One setting of a resource.</summary>
    /// <param name="Key">Its key.</param>
    /// <param name="Value">Its value.</param>
    /// <param name="Source">See <see cref="ConfigSource"/>.</param>
    public sealed record ConfigEntry(string Key, string Value, byte Source);

    /// <summary>One resource's settings, or why not.</summary>
    /// <param name="ResourceId">The resource.</param>
    /// <param name="Error"><see cref="ErrorCode.UnknownResource"/> if there is no such resource.</param>
    /// <param name="Configs">Its settings.</param>
    public sealed record ResourceConfigs(ulong ResourceId, ErrorCode Error, IReadOnlyList<ConfigEntry> Configs);

    // ---- metrics ----

    /// <summary>The machine a node runs on; a <c>null</c> field is unknown.</summary>
    /// <param name="CpuPermille">CPU use, per mille.</param>
    /// <param name="MemoryBytes">Memory the node uses.</param>
    /// <param name="MemoryLimitBytes">Memory available to it.</param>
    /// <param name="DiskFreeBytes">Free bytes on the data volume.</param>
    /// <param name="DiskTotalBytes">Size of the data volume.</param>
    public sealed record HostMetrics(
        ushort? CpuPermille, ulong? MemoryBytes, ulong? MemoryLimitBytes, ulong? DiskFreeBytes, ulong? DiskTotalBytes);

    /// <summary>How far a replica is.</summary>
    /// <param name="NodeId">The replica's node.</param>
    /// <param name="LogEnd">Offset after the last one it holds.</param>
    public sealed record ReplicaProgress(uint NodeId, ulong LogEnd);

    /// <summary>One Raft group hosted on the node.</summary>
    /// <param name="Group">The group.</param>
    /// <param name="ResourceId">The stream or queue it belongs to; 0 for the node's own groups.</param>
    /// <param name="Partition">Partition (0 for a queue).</param>
    /// <param name="Leader">Whether this node leads the group.</param>
    /// <param name="Term">Raft term here.</param>
    /// <param name="HighWatermark">Committed offsets end here.</param>
    /// <param name="LastStable">Below the first open transaction.</param>
    /// <param name="LogStart">First offset still stored.</param>
    /// <param name="LogEnd">Offset after the last one in the log, committed or not.</param>
    /// <param name="SizeBytes">Bytes of the log on this node.</param>
    /// <param name="ProduceBytes">Acknowledged produce bytes, while this node led the group.</param>
    /// <param name="ProduceRecords">Acknowledged produce records, likewise.</param>
    /// <param name="FetchBytes">Bytes fetched, or for a queue delivered, likewise.</param>
    /// <param name="Replicas">On the leader: the last offset each replica holds (plus one).</param>
    public sealed record PartitionMetrics(
        ulong Group,
        ulong ResourceId,
        uint Partition,
        bool Leader,
        ulong Term,
        ulong HighWatermark,
        ulong LastStable,
        ulong LogStart,
        ulong LogEnd,
        ulong SizeBytes,
        ulong ProduceBytes,
        ulong ProduceRecords,
        ulong FetchBytes,
        IReadOnlyList<ReplicaProgress> Replicas);

    /// <summary>A queue subscription.</summary>
    /// <param name="SubscriptionId">The subscription.</param>
    /// <param name="Peer">The client's address.</param>
    /// <param name="InFlight">Messages delivered to it and not acknowledged.</param>
    /// <param name="Credit">Messages it may still be sent.</param>
    public sealed record ConsumerMetrics(uint SubscriptionId, string Peer, uint InFlight, uint Credit);

    /// <summary>A queue the node leads.</summary>
    /// <param name="ResourceId">The queue.</param>
    /// <param name="Ready">Messages ready for delivery.</param>
    /// <param name="InFlight">Delivered and not yet acknowledged.</param>
    /// <param name="InOpenTxns">Waiting for their transaction to end.</param>
    /// <param name="Delivered">Messages delivered, while this node led the queue.</param>
    /// <param name="Acked">Messages acknowledged, likewise.</param>
    /// <param name="Consumers">Subscriptions.</param>
    public sealed record QueueMetrics(
        ulong ResourceId,
        ulong Ready,
        ulong InFlight,
        ulong InOpenTxns,
        ulong Delivered,
        ulong Acked,
        IReadOnlyList<ConsumerMetrics> Consumers);

    /// <summary>A member of a consumer group.</summary>
    /// <param name="MemberId">The member.</param>
    /// <param name="Owned">Partitions it owns.</param>
    public sealed record GroupMemberMetrics(string MemberId, IReadOnlyList<TxnResource> Owned);

    /// <summary>A group's committed offset for one partition.</summary>
    /// <param name="ResourceId">The stream.</param>
    /// <param name="Partition">The partition.</param>
    /// <param name="Offset">The next offset to read.</param>
    public sealed record CommittedOffset(ulong ResourceId, uint Partition, ulong Offset);

    /// <summary>A consumer group, from the coordinator's leader.</summary>
    /// <param name="GroupId">The group.</param>
    /// <param name="Tenant">The tenant its id lives in (ADR 0062).</param>
    /// <param name="Epoch">Assignment epoch.</param>
    /// <param name="Members">Members and what they own.</param>
    /// <param name="Offsets">Committed offsets.</param>
    public sealed record GroupMetrics(
        string GroupId, string Tenant, int Epoch, IReadOnlyList<GroupMemberMetrics> Members, IReadOnlyList<CommittedOffset> Offsets);

    // ---- authentication, users, tenants, roles ----

    /// <summary>A user's SCRAM-SHA-512 credential as the broker keeps it: never the password.</summary>
    /// <param name="Iterations">PBKDF2 iterations, at least 4096.</param>
    /// <param name="Salt">The salt.</param>
    /// <param name="StoredKey"><c>H(ClientKey)</c>.</param>
    /// <param name="ServerKey"><c>HMAC(SaltedPassword, "Server Key")</c>.</param>
    public sealed record ScramCredentialData(uint Iterations, ByteString Salt, ByteString StoredKey, ByteString ServerKey);

    /// <summary>One user with a credential.</summary>
    /// <param name="Name">The user name.</param>
    /// <param name="Mechanism"><c>SCRAM-SHA-512</c>.</param>
    /// <param name="Iterations">The credential's iterations.</param>
    /// <param name="Source">See <see cref="ConfigSource"/>.</param>
    public sealed record UserEntry(string Name, string Mechanism, uint Iterations, byte Source);

    /// <summary>States of a certificate in a <see cref="CertificateEntry"/>.</summary>
    public static class CertificateState
    {
        /// <summary>Valid, and not ending within 30 days.</summary>
        public const byte Valid = 0;

        /// <summary>Valid, and ending within 30 days.</summary>
        public const byte Expiring = 1;

        /// <summary>Past its end.</summary>
        public const byte Expired = 2;

        /// <summary>Revoked by a cluster admin (<c>RevokeCertificate</c>).</summary>
        public const byte Revoked = 3;
    }

    /// <summary>One client certificate in the cluster's inventory.</summary>
    /// <param name="Issuer">SHA-256 of the issuer's name in DER form (32 bytes).</param>
    /// <param name="Serial">The serial number, big-endian.</param>
    /// <param name="Principal">The principal it names (its common name); empty for a certificate revoked before any node saw it.</param>
    /// <param name="NotBeforeMs">First millisecond it is valid in, since the epoch; 0 if unknown.</param>
    /// <param name="NotAfterMs">Millisecond it stops being valid in, since the epoch.</param>
    /// <param name="FirstSeenMs">When a node first accepted it, in milliseconds since the epoch; 0 if none did.</param>
    /// <param name="LastSeenMs">When a node last accepted it, as far as nodes report: at most an hour late.</param>
    /// <param name="State">See <see cref="CertificateState"/>.</param>
    /// <param name="Reason">Why it was revoked; empty if it was not.</param>
    /// <param name="RevokedAtMs">When it was revoked, in milliseconds since the epoch; 0 if it was not.</param>
    public sealed record CertificateEntry(
        ByteString Issuer, ByteString Serial, string Principal, ulong NotBeforeMs, ulong NotAfterMs, ulong FirstSeenMs,
        ulong LastSeenMs, byte State, string Reason, ulong RevokedAtMs);

    /// <summary>The certificate inventory with the answering node's clock.</summary>
    /// <param name="NowMs">The node's clock in milliseconds since the epoch, which the states are computed against.</param>
    /// <param name="Certificates">The certificates.</param>
    public sealed record CertificateInventory(ulong NowMs, IReadOnlyList<CertificateEntry> Certificates);

    /// <summary>States of a certificate request in a <see cref="CertificateRequestEntry"/>.</summary>
    public static class CertificateRequestState
    {
        /// <summary>Waiting for the CA holder.</summary>
        public const byte Pending = 0;

        /// <summary>Signed; the certificate is in the entry.</summary>
        public const byte Signed = 1;

        /// <summary>Unsigned past its seven days.</summary>
        public const byte Expired = 2;
    }

    /// <summary>One certificate request.</summary>
    /// <param name="Id">SHA-256 of the signing request (32 bytes).</param>
    /// <param name="Principal">The principal asked for.</param>
    /// <param name="Csr">The signing request, DER.</param>
    /// <param name="RequestedBy">Who asked.</param>
    /// <param name="RequestedAtMs">When, in milliseconds since the epoch.</param>
    /// <param name="ExpiresAtMs">When an unsigned request lapses, in milliseconds since the epoch.</param>
    /// <param name="State">See <see cref="CertificateRequestState"/>.</param>
    /// <param name="Certificate">The signed certificate, DER; empty until signed.</param>
    /// <param name="SignedAtMs">When it was signed, in milliseconds since the epoch; 0 if not yet.</param>
    public sealed record CertificateRequestEntry(
        ByteString Id, string Principal, ByteString Csr, string RequestedBy, ulong RequestedAtMs, ulong ExpiresAtMs,
        byte State, ByteString Certificate, ulong SignedAtMs);

    /// <summary>The certificate requests, newest first, with the answering node's clock.</summary>
    /// <param name="NowMs">The node's clock in milliseconds since the epoch.</param>
    /// <param name="Requests">The requests.</param>
    public sealed record CertificateRequests(ulong NowMs, IReadOnlyList<CertificateRequestEntry> Requests);

    /// <summary>One accepted client certificate, as a node reports it (internal, ADR 0091).</summary>
    /// <param name="Issuer">SHA-256 of the issuer's name in DER form (32 bytes).</param>
    /// <param name="Serial">The serial number, big-endian.</param>
    /// <param name="Principal">The principal it names.</param>
    /// <param name="NotBeforeMs">First millisecond it is valid in.</param>
    /// <param name="NotAfterMs">Millisecond it stops being valid in.</param>
    /// <param name="SeenMs">When the node accepted it, by its clock.</param>
    public sealed record SeenCertificate(
        ByteString Issuer, ByteString Serial, string Principal, ulong NotBeforeMs, ulong NotAfterMs, ulong SeenMs);

    /// <summary>One tenant.</summary>
    /// <param name="Name">The name.</param>
    /// <param name="Source">See <see cref="ConfigSource"/>.</param>
    /// <param name="Resources">How many resources it holds.</param>
    public sealed record TenantEntry(string Name, byte Source, uint Resources);

    /// <summary>A role granted to a user or group, in a tenant, for the resources whose names start with a prefix.</summary>
    /// <param name="SubjectKind">See <see cref="Messages.SubjectKind"/>.</param>
    /// <param name="Subject">The user or group.</param>
    /// <param name="Role">See <see cref="Messages.Role"/>.</param>
    /// <param name="Tenant">The tenant; empty for the two cluster-wide roles.</param>
    /// <param name="Prefix">Only resources whose names start with this; empty for all.</param>
    public sealed record RoleBinding(byte SubjectKind, string Subject, byte Role, string Tenant, string Prefix)
    {
        internal static void Write(Writer w, RoleBinding b) =>
            w.U8(b.SubjectKind).String(b.Subject).U8(b.Role).String(b.Tenant).String(b.Prefix);

        internal static RoleBinding Read(Reader r) => new(r.U8(), r.String(), r.U8(), r.String(), r.String());
    }

    /// <summary>A role binding and where it comes from.</summary>
    /// <param name="Binding">The binding.</param>
    /// <param name="Source">See <see cref="ConfigSource"/>.</param>
    public sealed record SourcedBinding(RoleBinding Binding, byte Source);

    /// <summary>One node of the cluster.</summary>
    /// <param name="NodeId">Its id.</param>
    /// <param name="Peer">Where the other nodes reach it.</param>
    /// <param name="Client">Where clients reach it.</param>
    /// <param name="State">See <see cref="NodeState"/>.</param>
    /// <param name="Replicas">Groups it is to be a voter of.</param>
    /// <param name="Moving">Of those, groups whose voters are not yet in place.</param>
    public sealed record NodeEntry(ulong NodeId, string Peer, string Client, byte State, uint Replicas, uint Moving);
}
