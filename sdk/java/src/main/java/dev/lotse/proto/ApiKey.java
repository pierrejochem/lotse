package dev.lotse.proto;

import java.util.List;

/**
 * An API key, a {@code u16}. The constants are the keys of the protocol; the numbers are fixed.
 *
 * @param id the key as sent
 */
public record ApiKey(int id) {
    /** Versions and features of the node. */
    public static final ApiKey API_VERSIONS = new ApiKey(0);
    /** Nodes, resources and partition leaders. */
    public static final ApiKey METADATA = new ApiKey(1);
    /** The node coordinating a consumer group or producers. */
    public static final ApiKey FIND_COORDINATOR = new ApiKey(2);
    /** Appends batches to stream partitions. */
    public static final ApiKey PRODUCE = new ApiKey(3);
    /** Reads batches from stream partitions. */
    public static final ApiKey FETCH = new ApiKey(4);
    /** Looks up offsets by time or at either end of a partition. */
    public static final ApiKey LIST_OFFSETS = new ApiKey(5);
    /** Joins, stays in or leaves a consumer group. */
    public static final ApiKey GROUP_HEARTBEAT = new ApiKey(6);
    /** Commits a group's offsets. */
    public static final ApiKey COMMIT_OFFSETS = new ApiKey(7);
    /** Reads a group's committed offsets. */
    public static final ApiKey OFFSET_FETCH = new ApiKey(8);
    /** Subscribes to a queue. */
    public static final ApiKey SUBSCRIBE = new ApiKey(10);
    /** Pushes messages to a subscription (never a request). */
    public static final ApiKey DELIVER = new ApiKey(11);
    /** Adds credit to a subscription; never answered. */
    public static final ApiKey CREDIT = new ApiKey(12);
    /** Acknowledges queue messages. */
    public static final ApiKey ACK = new ApiKey(13);
    /** Rejects queue messages: back to the queue, or discarded. */
    public static final ApiKey NACK = new ApiKey(14);
    /** Ends a subscription. */
    public static final ApiKey UNSUBSCRIBE = new ApiKey(15);
    /** A producer id and epoch for idempotent and transactional produce. */
    public static final ApiKey INIT_PRODUCER = new ApiKey(16);
    /** Adds partitions to a transaction. */
    public static final ApiKey ADD_PARTITIONS_TO_TXN = new ApiKey(17);
    /** Commits a group's offsets within a transaction. */
    public static final ApiKey ADD_OFFSETS_TO_TXN = new ApiKey(19);
    /** Commits or aborts a transaction. */
    public static final ApiKey END_TXN = new ApiKey(20);
    /** Creates a stream (admin). */
    public static final ApiKey CREATE_STREAM = new ApiKey(21);
    /** Creates a queue (admin). */
    public static final ApiKey CREATE_QUEUE = new ApiKey(22);
    /** Declares an exchange (admin). */
    public static final ApiKey DECLARE_EXCHANGE = new ApiKey(23);
    /** Binds a queue to an exchange (admin). */
    public static final ApiKey BIND = new ApiKey(24);
    /** Deletes a resource created at runtime (admin). */
    public static final ApiKey DELETE_RESOURCE = new ApiKey(25);
    /** Reads resources' settings (admin). */
    public static final ApiKey DESCRIBE_CONFIGS = new ApiKey(26);
    /** Changes a resource's settings (admin). */
    public static final ApiKey ALTER_CONFIGS = new ApiKey(27);
    /** Removes a binding (admin). */
    public static final ApiKey UNBIND = new ApiKey(28);
    /** One node's metrics (admin). */
    public static final ApiKey DESCRIBE_METRICS = new ApiKey(29);
    /** Chooses a SASL mechanism. */
    public static final ApiKey SASL_HANDSHAKE = new ApiKey(30);
    /** One step of SASL authentication. */
    public static final ApiKey SASL_AUTHENTICATE = new ApiKey(31);
    /** Sets or removes a user's SCRAM credential (admin). */
    public static final ApiKey ALTER_USER = new ApiKey(32);
    /** Lists the users with a SCRAM credential (admin). */
    public static final ApiKey DESCRIBE_USERS = new ApiKey(33);
    /** Creates a tenant (cluster admin). */
    public static final ApiKey CREATE_TENANT = new ApiKey(34);
    /** Deletes an empty tenant (cluster admin). */
    public static final ApiKey DELETE_TENANT = new ApiKey(35);
    /** Lists tenants. */
    public static final ApiKey DESCRIBE_TENANTS = new ApiKey(36);
    /** Adds and removes role bindings (cluster admin). */
    public static final ApiKey ALTER_ROLE_BINDINGS = new ApiKey(37);
    /** Lists role bindings (cluster admin). */
    public static final ApiKey DESCRIBE_ROLE_BINDINGS = new ApiKey(38);
    /** Who the connection acts as. */
    public static final ApiKey WHO_AM_I = new ApiKey(39);
    /** Adds a node to the cluster (cluster admin). */
    public static final ApiKey ADD_NODE = new ApiKey(40);
    /** Drains a node and then removes it (cluster admin). */
    public static final ApiKey REMOVE_NODE = new ApiKey(41);
    /** Lists the cluster's nodes (cluster viewers). */
    public static final ApiKey DESCRIBE_NODES = new ApiKey(42);
    /** Node to node, not for clients: a group's leader reports the voters in place. */
    public static final ApiKey SETTLE_VOTERS = new ApiKey(43);
    /** Node to node, not for clients: a queue's leader forwards messages to its dead-letter queue. */
    public static final ApiKey DEAD_LETTER = new ApiKey(44);
    /** Node to node, not for clients: a partition leader asks the coordinator whether a transaction is open. */
    public static final ApiKey VERIFY_TXN = new ApiKey(45);
    /** Node to node, not for clients: the metadata leader asks a partition leader whether it holds anything. */
    public static final ApiKey CHECK_EMPTY = new ApiKey(46);
    /** Settles a queue's ready messages, on the queue's leader (tenant admin, ADR 0064). */
    public static final ApiKey PURGE_QUEUE = new ApiKey(47);
    /** Moves a dead-letter queue's ready messages back to where they came from (tenant admin, ADR 0064). */
    public static final ApiKey REQUEUE = new ApiKey(48);
    /** Pins a partition's leader to one of its voters, or unpins it (cluster admin, ADR 0065). */
    public static final ApiKey MOVE_LEADER = new ApiKey(49);
    /** Ends a user's or group's sessions and refuses their tokens issued before a time (cluster admin, ADR 0070). */
    public static final ApiKey REVOKE_SESSIONS = new ApiKey(50);
    /** Raises a runtime stream's partition count (tenant admin, ADR 0074). */
    public static final ApiKey CREATE_PARTITIONS = new ApiKey(51);
    /** Adds a certificate to the cluster's revocation list (cluster admin). */
    public static final ApiKey REVOKE_CERTIFICATE = new ApiKey(52);
    /** Publishes messages through exchanges; the node routes (ADR 0079). */
    public static final ApiKey PUBLISH = new ApiKey(9);
    /** Node to node, not for clients: the coordinator asks a partition leader to write a transaction marker. */
    public static final ApiKey WRITE_TXN_MARKERS = new ApiKey(1000);

    /** Creates a key. */
    public ApiKey {
        if (id < 0 || id > 0xFFFF) {
            throw new IllegalArgumentException("not a u16: " + id);
        }
    }

    /**
     * The client-facing keys this library has request and response types for, all at version 0. The node-internal
     * keys 43, 44, 45 and 1000 are constants only.
     */
    public static final List<ApiVersionRange> IMPLEMENTED = List.of(
            range(API_VERSIONS), range(METADATA), range(FIND_COORDINATOR), range(PRODUCE), range(FETCH),
            range(LIST_OFFSETS),
            range(INIT_PRODUCER), range(ADD_PARTITIONS_TO_TXN), range(END_TXN), range(SUBSCRIBE), range(CREDIT),
            range(ACK), range(NACK), range(UNSUBSCRIBE), range(GROUP_HEARTBEAT), range(COMMIT_OFFSETS),
            range(OFFSET_FETCH), range(ADD_OFFSETS_TO_TXN), range(CREATE_STREAM), range(CREATE_QUEUE),
            range(DECLARE_EXCHANGE), range(BIND), range(DELETE_RESOURCE), range(DESCRIBE_CONFIGS),
            range(ALTER_CONFIGS), range(UNBIND), range(DESCRIBE_METRICS), range(SASL_HANDSHAKE),
            range(SASL_AUTHENTICATE), range(ALTER_USER), range(DESCRIBE_USERS), range(CREATE_TENANT),
            range(DELETE_TENANT), range(DESCRIBE_TENANTS), range(ALTER_ROLE_BINDINGS),
            range(DESCRIBE_ROLE_BINDINGS), range(WHO_AM_I), range(ADD_NODE), range(REMOVE_NODE),
            range(DESCRIBE_NODES), range(PURGE_QUEUE), range(REQUEUE), range(MOVE_LEADER), range(REVOKE_SESSIONS),
            range(CREATE_PARTITIONS), range(REVOKE_CERTIFICATE), range(PUBLISH));

    private static ApiVersionRange range(ApiKey key) {
        return new ApiVersionRange(key, 0, 0);
    }

    /** Whether {@code key} is a client-facing key this library has request types for. */
    static boolean isClientKey(ApiKey key) {
        for (ApiVersionRange r : IMPLEMENTED) {
            if (r.apiKey().equals(key)) {
                return true;
            }
        }
        return false;
    }

    /** Whether this library speaks {@code version} of this key. */
    public boolean isImplemented(int version) {
        for (ApiVersionRange r : IMPLEMENTED) {
            if (r.apiKey().equals(this)) {
                return r.contains(version);
            }
        }
        return false;
    }
}
