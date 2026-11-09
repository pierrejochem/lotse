import { ApiKey } from "./api-key.js";
import { DecodeError } from "./error.js";
import * as m from "./messages.js";
import { Reader, type Tag } from "./reader.js";
import { Writer } from "./writer.js";

/**
 * Every request a client sends, one interface per message and a codec per message below. Optional wire fields are
 * optional here and `null` when read. `tenant` is the tenant of a transactional or group id (ADR 0062); absent means
 * `default`, and only another tenant is written.
 */

export interface ApiVersionsRequest {
  readonly type: "ApiVersions";
}
export interface MetadataRequest {
  readonly type: "Metadata";
  readonly names: readonly string[];
}
export interface FindCoordinatorRequest {
  readonly type: "FindCoordinator";
  readonly keyType: number;
  readonly key: string;
}
export interface ProduceRequest {
  readonly type: "Produce";
  readonly txnId?: string | null;
  readonly timeoutMs: number;
  readonly resources: readonly m.ProduceResource[];
  readonly tenant?: string;
}
export interface ListOffsetsRequest {
  readonly type: "ListOffsets";
  readonly isolation: number;
  readonly resources: readonly m.ListOffsetsResource[];
}
export interface FetchRequest {
  readonly type: "Fetch";
  readonly maxWaitMs: number;
  readonly minBytes: number;
  readonly maxBytes: number;
  readonly isolation: number;
  readonly resources: readonly m.FetchResource[];
}
export interface InitProducerRequest {
  readonly type: "InitProducer";
  readonly txnId?: string | null;
  readonly txnTimeoutMs: number;
  readonly tenant?: string;
}
export interface AddPartitionsToTxnRequest {
  readonly type: "AddPartitionsToTxn";
  readonly txnId: string;
  readonly producerId: bigint;
  readonly producerEpoch: number;
  readonly resources: readonly m.TxnResource[];
  readonly tenant?: string;
}
export interface EndTxnRequest {
  readonly type: "EndTxn";
  readonly txnId: string;
  readonly producerId: bigint;
  readonly producerEpoch: number;
  readonly commit: boolean;
  readonly tenant?: string;
}
export interface SubscribeRequest {
  readonly type: "Subscribe";
  readonly queueId: bigint;
  readonly consumerTag: string;
  readonly creditMsgs: number;
  readonly creditBytes: number;
  readonly exclusive: boolean;
  /** Tag 1: ask for a push when a standby subscription becomes the active one (ADR 0076). */
  readonly notifyActive?: boolean;
}
export interface CreditRequest {
  readonly type: "Credit";
  readonly subscriptionId: number;
  readonly addMsgs: number;
  readonly addBytes: number;
}
export interface AckRequest {
  readonly type: "Ack";
  readonly queueId: bigint;
  readonly txnId?: string | null;
  readonly ranges: readonly m.Range[];
  readonly tenant?: string;
  /** Written (tag 2) only with a `txnId`. */
  readonly producerId?: bigint;
  readonly producerEpoch?: number;
}
export interface NackRequest {
  readonly type: "Nack";
  readonly queueId: bigint;
  readonly ranges: readonly m.Range[];
  readonly requeue: boolean;
  readonly delayMs: number;
}
export interface UnsubscribeRequest {
  readonly type: "Unsubscribe";
  readonly subscriptionId: number;
}
export interface GroupHeartbeatRequest {
  readonly type: "GroupHeartbeat";
  readonly groupId: string;
  readonly memberId: string;
  readonly memberEpoch: number;
  readonly rebalanceTimeoutMs: number;
  /** Stream ids; `null` when the subscription is unchanged. */
  readonly subscribed: readonly bigint[] | null;
  readonly owned: readonly m.TxnResource[] | null;
  readonly tenant?: string;
  readonly instanceId?: string | null;
  readonly subscribedPattern?: string | null;
}
export interface CommitOffsetsRequest {
  readonly type: "CommitOffsets";
  readonly groupId: string;
  readonly memberId: string;
  readonly memberEpoch: number;
  readonly resources: readonly m.OffsetCommitResource[];
  readonly tenant?: string;
}
export interface OffsetFetchRequest {
  readonly type: "OffsetFetch";
  readonly groupId: string;
  readonly requireStable: boolean;
  readonly resources: readonly m.TxnResource[];
  readonly tenant?: string;
}
export interface AddOffsetsToTxnRequest {
  readonly type: "AddOffsetsToTxn";
  readonly txnId: string;
  readonly producerId: bigint;
  readonly producerEpoch: number;
  readonly groupId: string;
  readonly memberId: string;
  readonly memberEpoch: number;
  readonly resources: readonly m.OffsetCommitResource[];
  readonly tenant?: string;
}
export interface CreateStreamRequest {
  readonly type: "CreateStream";
  readonly name: string;
  readonly tenant: string;
  readonly partitions: number;
  readonly replication: number;
  readonly configs: readonly m.Config[];
}
export interface CreateQueueRequest {
  readonly type: "CreateQueue";
  readonly name: string;
  readonly tenant: string;
  readonly priorityLevels: number;
  readonly singleActive: boolean;
  readonly dlq?: string | null;
  readonly replication: number;
  readonly configs: readonly m.Config[];
}
export interface DeclareExchangeRequest {
  readonly type: "DeclareExchange";
  readonly name: string;
  readonly tenant: string;
  readonly exchangeType: number;
  readonly configs: readonly m.Config[];
}
export interface BindRequest {
  readonly type: "Bind";
  readonly exchangeId: bigint;
  readonly queueId: bigint;
  readonly bindingKey: string;
  readonly headerMatch: readonly m.HeaderMatch[];
  readonly matchAll: boolean;
}
export interface UnbindRequest {
  readonly type: "Unbind";
  readonly exchangeId: bigint;
  readonly queueId: bigint;
  readonly bindingKey: string;
  readonly headerMatch: readonly m.HeaderMatch[];
  readonly matchAll: boolean;
}
export interface DeleteResourceRequest {
  readonly type: "DeleteResource";
  readonly resourceId: bigint;
  readonly ifEmpty: boolean;
}
export interface MoveLeaderRequest {
  readonly type: "MoveLeader";
  readonly resourceId: bigint;
  readonly partition: number;
  readonly nodeId: bigint;
}
export interface PublishRequest {
  readonly type: "Publish";
  readonly txnId?: string | null;
  readonly timeoutMs: number;
  readonly entries: readonly m.PublishEntry[];
}
export interface CreatePartitionsRequest {
  readonly type: "CreatePartitions";
  readonly resourceId: bigint;
  readonly count: number;
}
export interface RevokeSessionsRequest {
  readonly type: "RevokeSessions";
  readonly subjectKind: number;
  readonly subject: string;
  readonly beforeMs: bigint;
}
/** `RevokeCertificate` (key 52): one client certificate, named by its issuer hash and serial. */
export interface RevokeCertificateRequest {
  readonly type: "RevokeCertificate";
  /** SHA-256 of the issuer's distinguished name in DER form (32 bytes). */
  readonly issuer: Uint8Array;
  /** The serial number, big-endian. */
  readonly serial: Uint8Array;
  /** Milliseconds since the Unix epoch at which the certificate stops being valid. */
  readonly notAfterMs: bigint;
  readonly reason: string;
}
/** `DescribeCertificates` (key 53): the client certificates the cluster has seen or had revoked. Cluster admin. */
export interface DescribeCertificatesRequest {
  readonly type: "DescribeCertificates";
}
/** `RequestCertificate` (key 55): asks for a certificate with a signing request (ADR 0092). Cluster admin. */
export interface RequestCertificateRequest {
  readonly type: "RequestCertificate";
  /** The principal the certificate will name: its common name. */
  readonly principal: string;
  /** A PKCS#10 certificate signing request in DER form. */
  readonly csr: Uint8Array;
}
/** `DescribeCertificateRequests` (key 56): lists the certificate requests. Cluster admin. */
export interface DescribeCertificateRequestsRequest {
  readonly type: "DescribeCertificateRequests";
}
/** `RecordCertificate` (key 57): the CA holder records the certificate it signed for a request. Cluster admin. */
export interface RecordCertificateRequest {
  readonly type: "RecordCertificate";
  /** The request it answers: SHA-256 of its signing request. */
  readonly id: Uint8Array;
  /** The signed certificate, DER. */
  readonly certificate: Uint8Array;
  /** SHA-256 of the issuer's name in DER form (32 bytes). */
  readonly issuer: Uint8Array;
  /** The serial number, big-endian. */
  readonly serial: Uint8Array;
  readonly notBeforeMs: bigint;
  readonly notAfterMs: bigint;
}
/** `RecordCertificates` (key 54, internal: only broker nodes send it): certificates a node accepted. */
export interface RecordCertificatesRequest {
  readonly type: "RecordCertificates";
  readonly certificates: readonly m.SeenCertificate[];
}
/** `PurgeQueue` (key 47) or `Requeue` (key 48). */
export interface QueueActionRequest {
  readonly type: "QueueAction";
  readonly requeue: boolean;
  readonly queueId: bigint;
  readonly upTo: bigint;
}
export interface DescribeConfigsRequest {
  readonly type: "DescribeConfigs";
  readonly resourceIds: readonly bigint[];
}
export interface AlterConfigsRequest {
  readonly type: "AlterConfigs";
  readonly resourceId: bigint;
  readonly set: readonly m.Config[];
  readonly delete: readonly string[];
}
export interface DescribeMetricsRequest {
  readonly type: "DescribeMetrics";
}
export interface SaslHandshakeRequest {
  readonly type: "SaslHandshake";
  readonly mechanism: string;
}
export interface SaslAuthenticateRequest {
  readonly type: "SaslAuthenticate";
  readonly authBytes: Uint8Array;
}
export interface AlterUserRequest {
  readonly type: "AlterUser";
  readonly user: string;
  /** `null` removes the user's credential. */
  readonly credential: m.ScramCredentialData | null;
}
export interface DescribeUsersRequest {
  readonly type: "DescribeUsers";
}
export interface CreateTenantRequest {
  readonly type: "CreateTenant";
  readonly name: string;
}
export interface DeleteTenantRequest {
  readonly type: "DeleteTenant";
  readonly name: string;
}
export interface DescribeTenantsRequest {
  readonly type: "DescribeTenants";
}
export interface AlterRoleBindingsRequest {
  readonly type: "AlterRoleBindings";
  readonly add: readonly m.RoleBinding[];
  readonly remove: readonly m.RoleBinding[];
}
export interface DescribeRoleBindingsRequest {
  readonly type: "DescribeRoleBindings";
}
export interface WhoAmIRequest {
  readonly type: "WhoAmI";
}
export interface AddNodeRequest {
  readonly type: "AddNode";
  readonly nodeId: bigint;
  readonly peer: string;
  readonly client: string;
}
export interface RemoveNodeRequest {
  readonly type: "RemoveNode";
  readonly nodeId: bigint;
}
export interface DescribeNodesRequest {
  readonly type: "DescribeNodes";
}

export type Request =
  | ApiVersionsRequest
  | MetadataRequest
  | FindCoordinatorRequest
  | ProduceRequest
  | ListOffsetsRequest
  | FetchRequest
  | InitProducerRequest
  | AddPartitionsToTxnRequest
  | EndTxnRequest
  | SubscribeRequest
  | CreditRequest
  | AckRequest
  | NackRequest
  | UnsubscribeRequest
  | GroupHeartbeatRequest
  | CommitOffsetsRequest
  | OffsetFetchRequest
  | AddOffsetsToTxnRequest
  | CreateStreamRequest
  | CreateQueueRequest
  | DeclareExchangeRequest
  | BindRequest
  | UnbindRequest
  | DeleteResourceRequest
  | MoveLeaderRequest
  | PublishRequest
  | CreatePartitionsRequest
  | RevokeSessionsRequest
  | RevokeCertificateRequest
  | DescribeCertificatesRequest
  | RecordCertificatesRequest
  | RequestCertificateRequest
  | DescribeCertificateRequestsRequest
  | RecordCertificateRequest
  | QueueActionRequest
  | DescribeConfigsRequest
  | AlterConfigsRequest
  | DescribeMetricsRequest
  | SaslHandshakeRequest
  | SaslAuthenticateRequest
  | AlterUserRequest
  | DescribeUsersRequest
  | CreateTenantRequest
  | DeleteTenantRequest
  | DescribeTenantsRequest
  | AlterRoleBindingsRequest
  | DescribeRoleBindingsRequest
  | WhoAmIRequest
  | AddNodeRequest
  | RemoveNodeRequest
  | DescribeNodesRequest;

const KEYS: { readonly [T in Request["type"]]: number } = {
  ApiVersions: ApiKey.API_VERSIONS,
  Metadata: ApiKey.METADATA,
  FindCoordinator: ApiKey.FIND_COORDINATOR,
  Produce: ApiKey.PRODUCE,
  ListOffsets: ApiKey.LIST_OFFSETS,
  Fetch: ApiKey.FETCH,
  InitProducer: ApiKey.INIT_PRODUCER,
  AddPartitionsToTxn: ApiKey.ADD_PARTITIONS_TO_TXN,
  EndTxn: ApiKey.END_TXN,
  Subscribe: ApiKey.SUBSCRIBE,
  Credit: ApiKey.CREDIT,
  Ack: ApiKey.ACK,
  Nack: ApiKey.NACK,
  Unsubscribe: ApiKey.UNSUBSCRIBE,
  GroupHeartbeat: ApiKey.GROUP_HEARTBEAT,
  CommitOffsets: ApiKey.COMMIT_OFFSETS,
  OffsetFetch: ApiKey.OFFSET_FETCH,
  AddOffsetsToTxn: ApiKey.ADD_OFFSETS_TO_TXN,
  CreateStream: ApiKey.CREATE_STREAM,
  CreateQueue: ApiKey.CREATE_QUEUE,
  DeclareExchange: ApiKey.DECLARE_EXCHANGE,
  Bind: ApiKey.BIND,
  Unbind: ApiKey.UNBIND,
  DeleteResource: ApiKey.DELETE_RESOURCE,
  MoveLeader: ApiKey.MOVE_LEADER,
  Publish: ApiKey.PUBLISH,
  CreatePartitions: ApiKey.CREATE_PARTITIONS,
  RevokeSessions: ApiKey.REVOKE_SESSIONS,
  RevokeCertificate: ApiKey.REVOKE_CERTIFICATE,
  DescribeCertificates: ApiKey.DESCRIBE_CERTIFICATES,
  RecordCertificates: ApiKey.RECORD_CERTIFICATES,
  RequestCertificate: ApiKey.REQUEST_CERTIFICATE,
  DescribeCertificateRequests: ApiKey.DESCRIBE_CERTIFICATE_REQUESTS,
  RecordCertificate: ApiKey.RECORD_CERTIFICATE,
  QueueAction: ApiKey.PURGE_QUEUE, // or REQUEUE: see requestApiKey
  DescribeConfigs: ApiKey.DESCRIBE_CONFIGS,
  AlterConfigs: ApiKey.ALTER_CONFIGS,
  DescribeMetrics: ApiKey.DESCRIBE_METRICS,
  SaslHandshake: ApiKey.SASL_HANDSHAKE,
  SaslAuthenticate: ApiKey.SASL_AUTHENTICATE,
  AlterUser: ApiKey.ALTER_USER,
  DescribeUsers: ApiKey.DESCRIBE_USERS,
  CreateTenant: ApiKey.CREATE_TENANT,
  DeleteTenant: ApiKey.DELETE_TENANT,
  DescribeTenants: ApiKey.DESCRIBE_TENANTS,
  AlterRoleBindings: ApiKey.ALTER_ROLE_BINDINGS,
  DescribeRoleBindings: ApiKey.DESCRIBE_ROLE_BINDINGS,
  WhoAmI: ApiKey.WHO_AM_I,
  AddNode: ApiKey.ADD_NODE,
  RemoveNode: ApiKey.REMOVE_NODE,
  DescribeNodes: ApiKey.DESCRIBE_NODES,
};

/** The API key a request is sent under. */
export function requestApiKey(req: Request): number {
  if (req.type === "QueueAction") {
    return req.requeue ? ApiKey.REQUEUE : ApiKey.PURGE_QUEUE;
  }
  return KEYS[req.type];
}

function writeBinding(
  w: Writer,
  r: { exchangeId: bigint; queueId: bigint; bindingKey: string; headerMatch: readonly m.HeaderMatch[]; matchAll: boolean },
): void {
  w.u64(r.exchangeId).u64(r.queueId).string(r.bindingKey);
  w.array(r.headerMatch, m.putHeaderMatch);
  w.bool(r.matchAll).noTags();
}

function readBinding(r: Reader) {
  const x = {
    exchangeId: r.u64(),
    queueId: r.u64(),
    bindingKey: r.string(),
    headerMatch: r.array(5, m.getHeaderMatch),
    matchAll: r.bool(),
  };
  r.tags();
  return x;
}

/** Writes a request's body (everything after the frame header). */
export function writeRequest(w: Writer, req: Request): void {
  switch (req.type) {
    case "ApiVersions":
    case "DescribeMetrics":
    case "DescribeUsers":
    case "DescribeCertificates":
    case "DescribeCertificateRequests":
    case "DescribeTenants":
    case "DescribeRoleBindings":
    case "WhoAmI":
    case "DescribeNodes":
      w.noTags();
      return;
    case "Metadata":
      w.array(req.names, (w1, s) => w1.string(s)).noTags();
      return;
    case "FindCoordinator":
      w.u8(req.keyType).string(req.key).noTags();
      return;
    case "Produce":
      w.optString(req.txnId).u32(req.timeoutMs);
      w.array(req.resources, (w1, res) => {
        w1.u64(res.resourceId);
        w1.array(res.partitions, (w2, p) => {
          w2.u32(p.partition);
          w2.bytes(p.batch);
          w2.noTags();
        });
        w1.noTags();
      });
      m.putTenant(w, m.tenantOrDefault(req.tenant));
      return;
    case "ListOffsets":
      w.u8(req.isolation);
      w.array(req.resources, (w1, res) => {
        w1.u64(res.resourceId);
        w1.array(res.partitions, (w2, p) => {
          w2.u32(p.partition);
          w2.i64(p.timestamp);
          w2.noTags();
        });
        w1.noTags();
      });
      w.noTags();
      return;
    case "Fetch":
      w.u32(req.maxWaitMs).u32(req.minBytes).u32(req.maxBytes).u8(req.isolation);
      w.array(req.resources, (w1, res) => {
        w1.u64(res.resourceId);
        w1.array(res.partitions, (w2, p) => {
          w2.u32(p.partition);
          w2.u64(p.fetchOffset);
          w2.u64(p.leaderTerm);
          w2.u32(p.maxBytes);
          w2.noTags();
        });
        w1.noTags();
      });
      w.noTags();
      return;
    case "InitProducer":
      w.optString(req.txnId).u32(req.txnTimeoutMs);
      m.putTenant(w, m.tenantOrDefault(req.tenant));
      return;
    case "AddPartitionsToTxn":
      w.string(req.txnId).u64(req.producerId).u16(req.producerEpoch);
      m.putTxnResources(w, req.resources);
      m.putTenant(w, m.tenantOrDefault(req.tenant));
      return;
    case "EndTxn":
      w.string(req.txnId).u64(req.producerId).u16(req.producerEpoch).bool(req.commit);
      m.putTenant(w, m.tenantOrDefault(req.tenant));
      return;
    case "Subscribe":
      w.u64(req.queueId).string(req.consumerTag).u32(req.creditMsgs).u32(req.creditBytes).bool(req.exclusive);
      m.putTagList(w, req.notifyActive === true ? [{ tag: m.TAG_NOTIFY_ACTIVE, value: new Uint8Array([1]) }] : []);
      return;
    case "Credit":
      w.u32(req.subscriptionId).u32(req.addMsgs).u32(req.addBytes).noTags();
      return;
    case "Ack": {
      w.u64(req.queueId).optString(req.txnId);
      m.putRanges(w, req.ranges);
      const tags: Tag[] = [];
      const tenant = m.tenantOrDefault(req.tenant);
      if (tenant !== m.DEFAULT_TENANT) {
        tags.push(m.stringTag(m.TAG_TENANT, tenant));
      }
      if (req.txnId !== null && req.txnId !== undefined) {
        tags.push({
          tag: m.TAG_PRODUCER,
          value: new Writer().u64(req.producerId ?? 0n).u16(req.producerEpoch ?? 0).toBytes(),
        });
      }
      m.putTagList(w, tags);
      return;
    }
    case "Nack":
      w.u64(req.queueId);
      m.putRanges(w, req.ranges);
      w.bool(req.requeue).u32(req.delayMs).noTags();
      return;
    case "Unsubscribe":
      w.u32(req.subscriptionId).noTags();
      return;
    case "GroupHeartbeat": {
      w.string(req.groupId).string(req.memberId).i32(req.memberEpoch).u32(req.rebalanceTimeoutMs);
      w.bool(req.subscribed !== null);
      if (req.subscribed !== null) {
        w.array(req.subscribed, (w1, id) => w1.u64(id));
      }
      m.putOptResources(w, req.owned);
      const tags: Tag[] = [];
      const tenant = m.tenantOrDefault(req.tenant);
      if (tenant !== m.DEFAULT_TENANT) {
        tags.push(m.stringTag(m.TAG_TENANT, tenant));
      }
      if (req.instanceId !== undefined && req.instanceId !== null) {
        tags.push(m.stringTag(m.TAG_INSTANCE, req.instanceId));
      }
      if (req.subscribedPattern !== undefined && req.subscribedPattern !== null) {
        tags.push(m.stringTag(m.TAG_PATTERN, req.subscribedPattern));
      }
      m.putTagList(w, tags);
      return;
    }
    case "CommitOffsets":
      w.string(req.groupId).string(req.memberId).i32(req.memberEpoch);
      m.putCommits(w, req.resources);
      m.putTenant(w, m.tenantOrDefault(req.tenant));
      return;
    case "OffsetFetch":
      w.string(req.groupId).bool(req.requireStable);
      m.putTxnResources(w, req.resources);
      m.putTenant(w, m.tenantOrDefault(req.tenant));
      return;
    case "AddOffsetsToTxn":
      w.string(req.txnId).u64(req.producerId).u16(req.producerEpoch);
      w.string(req.groupId).string(req.memberId).i32(req.memberEpoch);
      m.putCommits(w, req.resources);
      m.putTenant(w, m.tenantOrDefault(req.tenant));
      return;
    case "CreateStream":
      w.string(req.name).string(req.tenant).u32(req.partitions).u8(req.replication);
      m.putConfigs(w, req.configs);
      w.noTags();
      return;
    case "CreateQueue":
      w.string(req.name).string(req.tenant).u8(req.priorityLevels).bool(req.singleActive).optString(req.dlq);
      w.u8(req.replication);
      m.putConfigs(w, req.configs);
      w.noTags();
      return;
    case "DeclareExchange":
      w.string(req.name).string(req.tenant).u8(req.exchangeType);
      m.putConfigs(w, req.configs);
      w.noTags();
      return;
    case "Bind":
    case "Unbind":
      writeBinding(w, req);
      return;
    case "DeleteResource":
      w.u64(req.resourceId).bool(req.ifEmpty).noTags();
      return;
    case "MoveLeader":
      w.u64(req.resourceId).u32(req.partition).u64(req.nodeId).noTags();
      return;
    case "Publish":
      w.optString(req.txnId).u32(req.timeoutMs);
      w.array(req.entries, (w1, e) => {
        w1.u64(e.exchangeId).string(e.routingKey).bool(e.mandatory).bytes(e.batch);
        w1.noTags();
      });
      w.noTags();
      return;
    case "CreatePartitions":
      w.u64(req.resourceId).u32(req.count).noTags();
      return;
    case "RevokeSessions":
      w.u8(req.subjectKind).string(req.subject).u64(req.beforeMs).noTags();
      return;
    case "RevokeCertificate":
      w.bytes(req.issuer).bytes(req.serial).u64(req.notAfterMs).string(req.reason).noTags();
      return;
    case "RequestCertificate":
      w.string(req.principal).bytes(req.csr).noTags();
      return;
    case "RecordCertificate":
      w.bytes(req.id)
        .bytes(req.certificate)
        .bytes(req.issuer)
        .bytes(req.serial)
        .u64(req.notBeforeMs)
        .u64(req.notAfterMs)
        .noTags();
      return;
    case "RecordCertificates":
      w.array(req.certificates, (w1, c) =>
        w1.bytes(c.issuer).bytes(c.serial).string(c.principal).u64(c.notBeforeMs).u64(c.notAfterMs).u64(c.seenMs),
      ).noTags();
      return;
    case "QueueAction":
      w.u64(req.queueId).u64(req.upTo).noTags();
      return;
    case "DescribeConfigs":
      w.array(req.resourceIds, (w1, id) => w1.u64(id)).noTags();
      return;
    case "AlterConfigs":
      w.u64(req.resourceId);
      m.putConfigs(w, req.set);
      w.array(req.delete, (w1, s) => w1.string(s)).noTags();
      return;
    case "SaslHandshake":
      w.string(req.mechanism).noTags();
      return;
    case "SaslAuthenticate":
      w.bytes(req.authBytes).noTags();
      return;
    case "AlterUser":
      w.string(req.user);
      if (req.credential === null) {
        w.u8(0);
      } else {
        w.u8(1).u32(req.credential.iterations);
        w.bytes(req.credential.salt).bytes(req.credential.storedKey).bytes(req.credential.serverKey);
      }
      w.noTags();
      return;
    case "CreateTenant":
    case "DeleteTenant":
      w.string(req.name).noTags();
      return;
    case "AlterRoleBindings":
      w.array(req.add, m.putRoleBinding).array(req.remove, m.putRoleBinding).noTags();
      return;
    case "AddNode":
      w.u64(req.nodeId).string(req.peer).string(req.client).noTags();
      return;
    case "RemoveNode":
      w.u64(req.nodeId).noTags();
      return;
  }
}

/** Reads a request's body for `apiKey`. The caller has checked that the key is a client key at version 0. */
export function readRequest(apiKey: number, r: Reader): Request {
  switch (apiKey) {
    case ApiKey.API_VERSIONS:
      r.tags();
      return { type: "ApiVersions" };
    case ApiKey.METADATA: {
      const names = r.array(2, (r1) => r1.string());
      r.tags();
      return { type: "Metadata", names };
    }
    case ApiKey.FIND_COORDINATOR: {
      const x: FindCoordinatorRequest = { type: "FindCoordinator", keyType: r.u8(), key: r.string() };
      r.tags();
      return x;
    }
    case ApiKey.PRODUCE: {
      const txnId = r.optString();
      const timeoutMs = r.u32();
      const resources = r.array(10, (r1) => {
        const resourceId = r1.u64();
        const partitions = r1.array(9, (r2) => {
          const p = { partition: r2.u32(), batch: r2.bytes() };
          r2.tags();
          return p;
        });
        r1.tags();
        return { resourceId, partitions };
      });
      return { type: "Produce", txnId, timeoutMs, resources, tenant: m.getTenant(r) };
    }
    case ApiKey.LIST_OFFSETS: {
      const isolation = r.u8();
      const resources = r.array(10, (r1) => {
        const resourceId = r1.u64();
        const partitions = r1.array(13, (r2) => {
          const p = { partition: r2.u32(), timestamp: r2.i64() };
          r2.tags();
          return p;
        });
        r1.tags();
        return { resourceId, partitions };
      });
      r.tags();
      return { type: "ListOffsets", isolation, resources };
    }
    case ApiKey.FETCH: {
      const maxWaitMs = r.u32();
      const minBytes = r.u32();
      const maxBytes = r.u32();
      const isolation = r.u8();
      const resources = r.array(10, (r1) => {
        const resourceId = r1.u64();
        const partitions = r1.array(25, (r2) => {
          const p = { partition: r2.u32(), fetchOffset: r2.u64(), leaderTerm: r2.u64(), maxBytes: r2.u32() };
          r2.tags();
          return p;
        });
        r1.tags();
        return { resourceId, partitions };
      });
      r.tags();
      return { type: "Fetch", maxWaitMs, minBytes, maxBytes, isolation, resources };
    }
    case ApiKey.INIT_PRODUCER:
      return { type: "InitProducer", txnId: r.optString(), txnTimeoutMs: r.u32(), tenant: m.getTenant(r) };
    case ApiKey.ADD_PARTITIONS_TO_TXN:
      return {
        type: "AddPartitionsToTxn",
        txnId: r.string(),
        producerId: r.u64(),
        producerEpoch: r.u16(),
        resources: m.getTxnResources(r),
        tenant: m.getTenant(r),
      };
    case ApiKey.END_TXN:
      return {
        type: "EndTxn",
        txnId: r.string(),
        producerId: r.u64(),
        producerEpoch: r.u16(),
        commit: r.bool(),
        tenant: m.getTenant(r),
      };
    case ApiKey.SUBSCRIBE: {
      const queueId = r.u64();
      const consumerTag = r.string();
      const creditMsgs = r.u32();
      const creditBytes = r.u32();
      const exclusive = r.bool();
      let notifyActive = false;
      for (const t of r.tags()) {
        if (t.tag === m.TAG_NOTIFY_ACTIVE) {
          const vr = new Reader(t.value);
          notifyActive = vr.u8() !== 0;
          vr.finish();
        }
      }
      return { type: "Subscribe", queueId, consumerTag, creditMsgs, creditBytes, exclusive, notifyActive };
    }
    case ApiKey.CREDIT: {
      const x: CreditRequest = { type: "Credit", subscriptionId: r.u32(), addMsgs: r.u32(), addBytes: r.u32() };
      r.tags();
      return x;
    }
    case ApiKey.ACK: {
      const queueId = r.u64();
      const txnId = r.optString();
      const ranges = m.getRanges(r);
      let tenant = m.DEFAULT_TENANT;
      let producerId = 0n;
      let producerEpoch = 0;
      for (const t of r.tags()) {
        const vr = new Reader(t.value);
        if (t.tag === m.TAG_TENANT) {
          tenant = vr.string();
        } else if (t.tag === m.TAG_PRODUCER) {
          producerId = vr.u64();
          producerEpoch = vr.u16();
        } else {
          continue; // from a newer version: skipped
        }
        vr.finish();
      }
      return { type: "Ack", queueId, txnId, ranges, tenant, producerId, producerEpoch };
    }
    case ApiKey.NACK: {
      const x: NackRequest = {
        type: "Nack",
        queueId: r.u64(),
        ranges: m.getRanges(r),
        requeue: r.bool(),
        delayMs: r.u32(),
      };
      r.tags();
      return x;
    }
    case ApiKey.UNSUBSCRIBE: {
      const x: UnsubscribeRequest = { type: "Unsubscribe", subscriptionId: r.u32() };
      r.tags();
      return x;
    }
    case ApiKey.GROUP_HEARTBEAT: {
      const groupId = r.string();
      const memberId = r.string();
      const memberEpoch = r.i32();
      const rebalanceTimeoutMs = r.u32();
      const subscribed = r.bool() ? r.array(8, (r1) => r1.u64()) : null;
      const owned = m.getOptResources(r);
      let tenant = m.DEFAULT_TENANT;
      let instanceId: string | null = null;
      let subscribedPattern: string | null = null;
      for (const t of r.tags()) {
        if (t.tag === m.TAG_TENANT || t.tag === m.TAG_INSTANCE || t.tag === m.TAG_PATTERN) {
          const vr = new Reader(t.value);
          const v = vr.string();
          vr.finish();
          if (t.tag === m.TAG_TENANT) {
            tenant = v;
          } else if (t.tag === m.TAG_INSTANCE) {
            instanceId = v;
          } else {
            subscribedPattern = v;
          }
        }
      }
      return {
        type: "GroupHeartbeat",
        groupId,
        memberId,
        memberEpoch,
        rebalanceTimeoutMs,
        subscribed,
        owned,
        tenant,
        instanceId,
        subscribedPattern,
      };
    }
    case ApiKey.COMMIT_OFFSETS:
      return {
        type: "CommitOffsets",
        groupId: r.string(),
        memberId: r.string(),
        memberEpoch: r.i32(),
        resources: m.getCommits(r),
        tenant: m.getTenant(r),
      };
    case ApiKey.OFFSET_FETCH:
      return {
        type: "OffsetFetch",
        groupId: r.string(),
        requireStable: r.bool(),
        resources: m.getTxnResources(r),
        tenant: m.getTenant(r),
      };
    case ApiKey.ADD_OFFSETS_TO_TXN:
      return {
        type: "AddOffsetsToTxn",
        txnId: r.string(),
        producerId: r.u64(),
        producerEpoch: r.u16(),
        groupId: r.string(),
        memberId: r.string(),
        memberEpoch: r.i32(),
        resources: m.getCommits(r),
        tenant: m.getTenant(r),
      };
    case ApiKey.CREATE_STREAM: {
      const x: CreateStreamRequest = {
        type: "CreateStream",
        name: r.string(),
        tenant: r.string(),
        partitions: r.u32(),
        replication: r.u8(),
        configs: m.getConfigs(r),
      };
      r.tags();
      return x;
    }
    case ApiKey.CREATE_QUEUE: {
      const x: CreateQueueRequest = {
        type: "CreateQueue",
        name: r.string(),
        tenant: r.string(),
        priorityLevels: r.u8(),
        singleActive: r.bool(),
        dlq: r.optString(),
        replication: r.u8(),
        configs: m.getConfigs(r),
      };
      r.tags();
      return x;
    }
    case ApiKey.DECLARE_EXCHANGE: {
      const x: DeclareExchangeRequest = {
        type: "DeclareExchange",
        name: r.string(),
        tenant: r.string(),
        exchangeType: r.u8(),
        configs: m.getConfigs(r),
      };
      r.tags();
      return x;
    }
    case ApiKey.BIND:
      return { type: "Bind", ...readBinding(r) };
    case ApiKey.UNBIND:
      return { type: "Unbind", ...readBinding(r) };
    case ApiKey.DELETE_RESOURCE: {
      const x: DeleteResourceRequest = { type: "DeleteResource", resourceId: r.u64(), ifEmpty: r.bool() };
      r.tags();
      return x;
    }
    case ApiKey.MOVE_LEADER: {
      const x: MoveLeaderRequest = { type: "MoveLeader", resourceId: r.u64(), partition: r.u32(), nodeId: r.u64() };
      r.tags();
      return x;
    }
    case ApiKey.PUBLISH: {
      const txnId = r.optString();
      const timeoutMs = r.u32();
      const entries = r.array(15, (r1) => {
        const e = { exchangeId: r1.u64(), routingKey: r1.string(), mandatory: r1.bool(), batch: r1.bytes() };
        r1.tags();
        return e;
      });
      r.tags();
      return { type: "Publish", txnId, timeoutMs, entries };
    }
    case ApiKey.CREATE_PARTITIONS: {
      const x: CreatePartitionsRequest = { type: "CreatePartitions", resourceId: r.u64(), count: r.u32() };
      r.tags();
      return x;
    }
    case ApiKey.REVOKE_SESSIONS: {
      const x: RevokeSessionsRequest = {
        type: "RevokeSessions",
        subjectKind: r.u8(),
        subject: r.string(),
        beforeMs: r.u64(),
      };
      r.tags();
      return x;
    }
    case ApiKey.REVOKE_CERTIFICATE: {
      const x: RevokeCertificateRequest = {
        type: "RevokeCertificate",
        issuer: r.bytes(),
        serial: r.bytes(),
        notAfterMs: r.u64(),
        reason: r.string(),
      };
      r.tags();
      return x;
    }
    case ApiKey.REQUEST_CERTIFICATE: {
      const x: RequestCertificateRequest = { type: "RequestCertificate", principal: r.string(), csr: r.bytes() };
      r.tags();
      return x;
    }
    case ApiKey.RECORD_CERTIFICATE: {
      const x: RecordCertificateRequest = {
        type: "RecordCertificate",
        id: r.bytes(),
        certificate: r.bytes(),
        issuer: r.bytes(),
        serial: r.bytes(),
        notBeforeMs: r.u64(),
        notAfterMs: r.u64(),
      };
      r.tags();
      return x;
    }
    case ApiKey.RECORD_CERTIFICATES: {
      // 4 + 4 + 2 + 3 * 8 bytes at least.
      const certificates = r.array(34, (r1) => ({
        issuer: r1.bytes(),
        serial: r1.bytes(),
        principal: r1.string(),
        notBeforeMs: r1.u64(),
        notAfterMs: r1.u64(),
        seenMs: r1.u64(),
      }));
      r.tags();
      return { type: "RecordCertificates", certificates };
    }
    case ApiKey.PURGE_QUEUE:
    case ApiKey.REQUEUE: {
      const x: QueueActionRequest = {
        type: "QueueAction",
        requeue: apiKey === ApiKey.REQUEUE,
        queueId: r.u64(),
        upTo: r.u64(),
      };
      r.tags();
      return x;
    }
    case ApiKey.DESCRIBE_CONFIGS: {
      const resourceIds = r.array(8, (r1) => r1.u64());
      r.tags();
      return { type: "DescribeConfigs", resourceIds };
    }
    case ApiKey.ALTER_CONFIGS: {
      const x: AlterConfigsRequest = {
        type: "AlterConfigs",
        resourceId: r.u64(),
        set: m.getConfigs(r),
        delete: r.array(2, (r1) => r1.string()),
      };
      r.tags();
      return x;
    }
    case ApiKey.DESCRIBE_METRICS:
      r.tags();
      return { type: "DescribeMetrics" };
    case ApiKey.SASL_HANDSHAKE: {
      const x: SaslHandshakeRequest = { type: "SaslHandshake", mechanism: r.string() };
      r.tags();
      return x;
    }
    case ApiKey.SASL_AUTHENTICATE: {
      const x: SaslAuthenticateRequest = { type: "SaslAuthenticate", authBytes: r.bytes() };
      r.tags();
      return x;
    }
    case ApiKey.ALTER_USER: {
      const user = r.string();
      let credential: m.ScramCredentialData | null;
      switch (r.u8()) {
        case 0:
          credential = null;
          break;
        case 1:
          credential = { iterations: r.u32(), salt: r.bytes(), storedKey: r.bytes(), serverKey: r.bytes() };
          break;
        default:
          throw DecodeError.malformed("credential flag is neither 0 nor 1");
      }
      r.tags();
      return { type: "AlterUser", user, credential };
    }
    case ApiKey.DESCRIBE_USERS:
      r.tags();
      return { type: "DescribeUsers" };
    case ApiKey.DESCRIBE_CERTIFICATES:
      r.tags();
      return { type: "DescribeCertificates" };
    case ApiKey.DESCRIBE_CERTIFICATE_REQUESTS:
      r.tags();
      return { type: "DescribeCertificateRequests" };
    case ApiKey.CREATE_TENANT: {
      const x: CreateTenantRequest = { type: "CreateTenant", name: r.string() };
      r.tags();
      return x;
    }
    case ApiKey.DELETE_TENANT: {
      const x: DeleteTenantRequest = { type: "DeleteTenant", name: r.string() };
      r.tags();
      return x;
    }
    case ApiKey.DESCRIBE_TENANTS:
      r.tags();
      return { type: "DescribeTenants" };
    case ApiKey.ALTER_ROLE_BINDINGS: {
      const x: AlterRoleBindingsRequest = {
        type: "AlterRoleBindings",
        add: r.array(m.BINDING_MIN, m.getRoleBinding),
        remove: r.array(m.BINDING_MIN, m.getRoleBinding),
      };
      r.tags();
      return x;
    }
    case ApiKey.DESCRIBE_ROLE_BINDINGS:
      r.tags();
      return { type: "DescribeRoleBindings" };
    case ApiKey.WHO_AM_I:
      r.tags();
      return { type: "WhoAmI" };
    case ApiKey.ADD_NODE: {
      const x: AddNodeRequest = { type: "AddNode", nodeId: r.u64(), peer: r.string(), client: r.string() };
      r.tags();
      return x;
    }
    case ApiKey.REMOVE_NODE: {
      const x: RemoveNodeRequest = { type: "RemoveNode", nodeId: r.u64() };
      r.tags();
      return x;
    }
    case ApiKey.DESCRIBE_NODES:
      r.tags();
      return { type: "DescribeNodes" };
    default:
      throw new Error("checked against the implemented keys");
  }
}
