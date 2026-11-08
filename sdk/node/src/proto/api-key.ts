/** API keys of the native protocol. All of them are version 0. */
export const ApiKey = {
  API_VERSIONS: 0,
  METADATA: 1,
  FIND_COORDINATOR: 2,
  PRODUCE: 3,
  FETCH: 4,
  LIST_OFFSETS: 5,
  GROUP_HEARTBEAT: 6,
  COMMIT_OFFSETS: 7,
  OFFSET_FETCH: 8,
  PUBLISH: 9,
  SUBSCRIBE: 10,
  DELIVER: 11,
  CREDIT: 12,
  ACK: 13,
  NACK: 14,
  UNSUBSCRIBE: 15,
  INIT_PRODUCER: 16,
  ADD_PARTITIONS_TO_TXN: 17,
  ADD_OFFSETS_TO_TXN: 19,
  END_TXN: 20,
  CREATE_STREAM: 21,
  CREATE_QUEUE: 22,
  DECLARE_EXCHANGE: 23,
  BIND: 24,
  DELETE_RESOURCE: 25,
  DESCRIBE_CONFIGS: 26,
  ALTER_CONFIGS: 27,
  UNBIND: 28,
  DESCRIBE_METRICS: 29,
  SASL_HANDSHAKE: 30,
  SASL_AUTHENTICATE: 31,
  ALTER_USER: 32,
  DESCRIBE_USERS: 33,
  CREATE_TENANT: 34,
  DELETE_TENANT: 35,
  DESCRIBE_TENANTS: 36,
  ALTER_ROLE_BINDINGS: 37,
  DESCRIBE_ROLE_BINDINGS: 38,
  WHO_AM_I: 39,
  ADD_NODE: 40,
  REMOVE_NODE: 41,
  DESCRIBE_NODES: 42,
  // Between nodes only; a client never sends these, and a node refuses them from a client.
  SETTLE_VOTERS: 43,
  DEAD_LETTER: 44,
  VERIFY_TXN: 45,
  CHECK_EMPTY: 46,
  PURGE_QUEUE: 47,
  REQUEUE: 48,
  MOVE_LEADER: 49,
  REVOKE_SESSIONS: 50,
  CREATE_PARTITIONS: 51,
  WRITE_TXN_MARKERS: 1000,
} as const;

export type ApiKeyValue = number;

/** The client-facing keys, all at version 0. */
export const IMPLEMENTED_KEYS: readonly number[] = [
  ApiKey.API_VERSIONS,
  ApiKey.METADATA,
  ApiKey.FIND_COORDINATOR,
  ApiKey.PRODUCE,
  ApiKey.FETCH,
  ApiKey.LIST_OFFSETS,
  ApiKey.INIT_PRODUCER,
  ApiKey.ADD_PARTITIONS_TO_TXN,
  ApiKey.END_TXN,
  ApiKey.SUBSCRIBE,
  ApiKey.CREDIT,
  ApiKey.ACK,
  ApiKey.NACK,
  ApiKey.UNSUBSCRIBE,
  ApiKey.GROUP_HEARTBEAT,
  ApiKey.COMMIT_OFFSETS,
  ApiKey.OFFSET_FETCH,
  ApiKey.ADD_OFFSETS_TO_TXN,
  ApiKey.CREATE_STREAM,
  ApiKey.CREATE_QUEUE,
  ApiKey.DECLARE_EXCHANGE,
  ApiKey.BIND,
  ApiKey.DELETE_RESOURCE,
  ApiKey.DESCRIBE_CONFIGS,
  ApiKey.ALTER_CONFIGS,
  ApiKey.UNBIND,
  ApiKey.DESCRIBE_METRICS,
  ApiKey.SASL_HANDSHAKE,
  ApiKey.SASL_AUTHENTICATE,
  ApiKey.ALTER_USER,
  ApiKey.DESCRIBE_USERS,
  ApiKey.CREATE_TENANT,
  ApiKey.DELETE_TENANT,
  ApiKey.DESCRIBE_TENANTS,
  ApiKey.ALTER_ROLE_BINDINGS,
  ApiKey.DESCRIBE_ROLE_BINDINGS,
  ApiKey.WHO_AM_I,
  ApiKey.ADD_NODE,
  ApiKey.REMOVE_NODE,
  ApiKey.DESCRIBE_NODES,
  ApiKey.PURGE_QUEUE,
  ApiKey.REQUEUE,
  ApiKey.MOVE_LEADER,
  ApiKey.REVOKE_SESSIONS,
  ApiKey.CREATE_PARTITIONS,
  ApiKey.PUBLISH,
];

const IMPLEMENTED = new Set(IMPLEMENTED_KEYS);

/** Whether a client may send this key at this version. */
export function isImplemented(key: number, version: number): boolean {
  return version === 0 && IMPLEMENTED.has(key);
}

/** Header flags. */
export const Flags = { PUSH: 1, ERROR: 2 } as const;

export interface RequestHeader {
  readonly apiKey: number;
  readonly apiVersion: number;
  readonly correlationId: number;
  readonly flags: number;
}

export interface ApiVersionRange {
  readonly apiKey: number;
  readonly min: number;
  readonly max: number;
}
