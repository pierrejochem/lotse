using System.Collections.Generic;

namespace Lotse.Proto;

/// <summary>An API key, a <c>u16</c>. The constants are the keys of the protocol; the numbers are fixed.</summary>
/// <param name="Id">The key as sent.</param>
public readonly record struct ApiKey(ushort Id)
{
    /// <summary>Versions and features of the node.</summary>
    public static readonly ApiKey ApiVersions = new(0);

    /// <summary>Nodes, resources and partition leaders.</summary>
    public static readonly ApiKey Metadata = new(1);

    /// <summary>The node coordinating a consumer group or producers.</summary>
    public static readonly ApiKey FindCoordinator = new(2);

    /// <summary>Appends batches to stream partitions.</summary>
    public static readonly ApiKey Produce = new(3);

    /// <summary>Reads batches from stream partitions.</summary>
    public static readonly ApiKey Fetch = new(4);

    /// <summary>Looks up offsets by time or at either end of a partition.</summary>
    public static readonly ApiKey ListOffsets = new(5);

    /// <summary>Joins, stays in or leaves a consumer group.</summary>
    public static readonly ApiKey GroupHeartbeat = new(6);

    /// <summary>Commits a group's offsets.</summary>
    public static readonly ApiKey CommitOffsets = new(7);

    /// <summary>Reads a group's committed offsets.</summary>
    public static readonly ApiKey OffsetFetch = new(8);

    /// <summary>Publishes messages through exchanges; the node routes (ADR 0079).</summary>
    public static readonly ApiKey Publish = new(9);

    /// <summary>Subscribes to a queue.</summary>
    public static readonly ApiKey Subscribe = new(10);

    /// <summary>Pushes messages to a subscription (never a request).</summary>
    public static readonly ApiKey Deliver = new(11);

    /// <summary>Adds credit to a subscription; never answered.</summary>
    public static readonly ApiKey Credit = new(12);

    /// <summary>Acknowledges queue messages.</summary>
    public static readonly ApiKey Ack = new(13);

    /// <summary>Rejects queue messages: back to the queue, or discarded.</summary>
    public static readonly ApiKey Nack = new(14);

    /// <summary>Ends a subscription.</summary>
    public static readonly ApiKey Unsubscribe = new(15);

    /// <summary>A producer id and epoch for idempotent and transactional produce.</summary>
    public static readonly ApiKey InitProducer = new(16);

    /// <summary>Adds partitions to a transaction.</summary>
    public static readonly ApiKey AddPartitionsToTxn = new(17);

    /// <summary>Commits a group's offsets within a transaction.</summary>
    public static readonly ApiKey AddOffsetsToTxn = new(19);

    /// <summary>Commits or aborts a transaction.</summary>
    public static readonly ApiKey EndTxn = new(20);

    /// <summary>Creates a stream (admin).</summary>
    public static readonly ApiKey CreateStream = new(21);

    /// <summary>Creates a queue (admin).</summary>
    public static readonly ApiKey CreateQueue = new(22);

    /// <summary>Declares an exchange (admin).</summary>
    public static readonly ApiKey DeclareExchange = new(23);

    /// <summary>Binds a queue to an exchange (admin).</summary>
    public static readonly ApiKey Bind = new(24);

    /// <summary>Deletes a resource created at runtime (admin).</summary>
    public static readonly ApiKey DeleteResource = new(25);

    /// <summary>Reads resources' settings (admin).</summary>
    public static readonly ApiKey DescribeConfigs = new(26);

    /// <summary>Changes a resource's settings (admin).</summary>
    public static readonly ApiKey AlterConfigs = new(27);

    /// <summary>Removes a binding (admin).</summary>
    public static readonly ApiKey Unbind = new(28);

    /// <summary>One node's metrics (admin).</summary>
    public static readonly ApiKey DescribeMetrics = new(29);

    /// <summary>Chooses a SASL mechanism.</summary>
    public static readonly ApiKey SaslHandshake = new(30);

    /// <summary>One step of SASL authentication.</summary>
    public static readonly ApiKey SaslAuthenticate = new(31);

    /// <summary>Sets or removes a user's SCRAM credential (admin).</summary>
    public static readonly ApiKey AlterUser = new(32);

    /// <summary>Lists the users with a SCRAM credential (admin).</summary>
    public static readonly ApiKey DescribeUsers = new(33);

    /// <summary>Creates a tenant (cluster admin).</summary>
    public static readonly ApiKey CreateTenant = new(34);

    /// <summary>Deletes an empty tenant (cluster admin).</summary>
    public static readonly ApiKey DeleteTenant = new(35);

    /// <summary>Lists tenants.</summary>
    public static readonly ApiKey DescribeTenants = new(36);

    /// <summary>Adds and removes role bindings (cluster admin).</summary>
    public static readonly ApiKey AlterRoleBindings = new(37);

    /// <summary>Lists role bindings (cluster admin).</summary>
    public static readonly ApiKey DescribeRoleBindings = new(38);

    /// <summary>Who the connection acts as.</summary>
    public static readonly ApiKey WhoAmI = new(39);

    /// <summary>Adds a node to the cluster (cluster admin).</summary>
    public static readonly ApiKey AddNode = new(40);

    /// <summary>Drains a node and then removes it (cluster admin).</summary>
    public static readonly ApiKey RemoveNode = new(41);

    /// <summary>Lists the cluster's nodes (cluster viewers).</summary>
    public static readonly ApiKey DescribeNodes = new(42);

    /// <summary>Node to node, not for clients: a group's leader reports the voters in place.</summary>
    public static readonly ApiKey SettleVoters = new(43);

    /// <summary>Node to node, not for clients: a queue's leader forwards messages to its dead-letter queue.</summary>
    public static readonly ApiKey DeadLetter = new(44);

    /// <summary>Node to node, not for clients: a partition leader asks the coordinator whether a transaction is open.</summary>
    public static readonly ApiKey VerifyTxn = new(45);

    /// <summary>Node to node, not for clients: the metadata leader asks a partition leader whether it holds anything.</summary>
    public static readonly ApiKey CheckEmpty = new(46);

    /// <summary>Settles a queue's ready messages, on the queue's leader (tenant admin, ADR 0064).</summary>
    public static readonly ApiKey PurgeQueue = new(47);

    /// <summary>Moves a dead-letter queue's ready messages back to where they came from (tenant admin, ADR 0064).</summary>
    public static readonly ApiKey Requeue = new(48);

    /// <summary>Pins a partition's leader to one of its voters, or unpins it (cluster admin, ADR 0065).</summary>
    public static readonly ApiKey MoveLeader = new(49);

    /// <summary>Ends a user's or group's sessions and refuses their tokens issued before a time (cluster admin, ADR 0070).</summary>
    public static readonly ApiKey RevokeSessions = new(50);

    /// <summary>Raises a runtime stream's partition count (tenant admin, ADR 0074).</summary>
    public static readonly ApiKey CreatePartitions = new(51);

    /// <summary>Node to node, not for clients: the coordinator asks a partition leader to write a transaction marker.</summary>
    public static readonly ApiKey WriteTxnMarkers = new(1000);

    /// <summary>
    /// The client-facing keys this library has request and response types for, all at version 0. The node-internal
    /// keys 43, 44, 45, 46 and 1000 are constants only.
    /// </summary>
    public static readonly IReadOnlyList<ApiVersionRange> Implemented = new[]
    {
        Range(ApiVersions), Range(Metadata), Range(FindCoordinator), Range(Produce), Range(Fetch),
        Range(ListOffsets),
        Range(InitProducer), Range(AddPartitionsToTxn), Range(EndTxn), Range(Subscribe), Range(Credit),
        Range(Ack), Range(Nack), Range(Unsubscribe), Range(GroupHeartbeat), Range(CommitOffsets),
        Range(OffsetFetch), Range(AddOffsetsToTxn), Range(CreateStream), Range(CreateQueue),
        Range(DeclareExchange), Range(Bind), Range(DeleteResource), Range(DescribeConfigs),
        Range(AlterConfigs), Range(Unbind), Range(DescribeMetrics), Range(SaslHandshake),
        Range(SaslAuthenticate), Range(AlterUser), Range(DescribeUsers), Range(CreateTenant),
        Range(DeleteTenant), Range(DescribeTenants), Range(AlterRoleBindings),
        Range(DescribeRoleBindings), Range(WhoAmI), Range(AddNode), Range(RemoveNode),
        Range(DescribeNodes), Range(PurgeQueue), Range(Requeue), Range(MoveLeader), Range(RevokeSessions),
        Range(CreatePartitions), Range(Publish),
    };

    private static ApiVersionRange Range(ApiKey key) => new(key, 0, 0);

    /// <summary>Whether <paramref name="key"/> is a client-facing key this library has request types for.</summary>
    internal static bool IsClientKey(ApiKey key)
    {
        foreach (ApiVersionRange r in Implemented)
        {
            if (r.ApiKey == key)
            {
                return true;
            }
        }
        return false;
    }

    /// <summary>Whether this library speaks <paramref name="version"/> of this key.</summary>
    public bool IsImplemented(int version)
    {
        foreach (ApiVersionRange r in Implemented)
        {
            if (r.ApiKey == this)
            {
                return r.Contains(version);
            }
        }
        return false;
    }
}
