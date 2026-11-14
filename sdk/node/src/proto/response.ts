import { ApiKey, type ApiVersionRange } from "./api-key.js";
import { DecodeError } from "./error.js";
import type { ErrorCodeValue } from "./error-code.js";
import * as m from "./messages.js";
import { Reader, type Tag } from "./reader.js";
import { U32_MAX } from "../record/unsigned.js";
import { Writer } from "./writer.js";

/**
 * Every typed response. A response has no API key on the wire, so the reader is told which request it answers.
 * `Error` and `Refused` are the two shapes of a frame with the ERROR flag and are not decoded from a key.
 */

export interface ApiVersionsResponse {
  readonly type: "ApiVersions";
  readonly versions: readonly ApiVersionRange[];
  readonly features: readonly string[];
}
export interface MetadataResponse {
  readonly type: "Metadata";
  readonly brokers: readonly m.Broker[];
  readonly resources: readonly m.ResourceMetadata[];
}
export interface FindCoordinatorResponse {
  readonly type: "FindCoordinator";
  readonly error: ErrorCodeValue;
  readonly nodeId: number;
  readonly host: string;
  readonly port: number;
  readonly errorMsg: string | null;
}
export interface InitProducerResponse {
  readonly type: "InitProducer";
  readonly error: ErrorCodeValue;
  readonly producerId: bigint;
  readonly producerEpoch: number;
  readonly leaderHint: m.LeaderHint | null;
}
export interface AddPartitionsToTxnResponse {
  readonly type: "AddPartitionsToTxn";
  readonly resources: readonly m.TxnResourceResult[];
}
export interface EndTxnResponse {
  readonly type: "EndTxn";
  readonly error: ErrorCodeValue;
  readonly producerId: bigint;
  readonly producerEpoch: number;
}
export interface SubscribeResponse {
  readonly type: "Subscribe";
  readonly error: ErrorCodeValue;
  readonly subscriptionId: number;
  readonly leaderHint: m.LeaderHint | null;
  /** Tag 1: accepted, but gets nothing until its turn (ADR 0076). */
  readonly standby: boolean;
  /** Tag 2: the consumer this subscription is (ADR 0109), to resume with; absent or 0 from an older node. */
  readonly consumerId?: bigint;
}
export interface PublishResponse {
  readonly type: "Publish";
  readonly throttleMs: number;
  readonly entries: readonly m.PublishResult[];
}
/** The answer to `Ack`, `Nack`, `Unsubscribe`, `Bind` and `Unbind`. */
export interface OutcomeResponse {
  readonly type: "Outcome";
  readonly error: ErrorCodeValue;
  readonly leaderHint: m.LeaderHint | null;
}
export interface GroupHeartbeatResponse {
  readonly type: "GroupHeartbeat";
  readonly throttleMs: number;
  readonly error: ErrorCodeValue;
  readonly memberId: string;
  readonly memberEpoch: number;
  readonly heartbeatIntervalMs: number;
  /** `null` when the assignment is unchanged. */
  readonly assignment: readonly m.TxnResource[] | null;
}
/** The answer to `CommitOffsets` and `AddOffsetsToTxn`. */
export interface CommitOffsetsResponse {
  readonly type: "CommitOffsets";
  readonly resources: readonly m.TxnResourceResult[];
}
export interface OffsetFetchResponse {
  readonly type: "OffsetFetch";
  readonly resources: readonly m.OffsetFetchResource[];
}
/** The answer to `CreateStream`, `CreateQueue` and `DeclareExchange`. */
export interface CreateResponse {
  readonly type: "Create";
  readonly error: ErrorCodeValue;
  readonly resourceId: bigint;
  readonly errorMsg: string | null;
  readonly leaderHint: m.LeaderHint | null;
}
/** The answer to the admin requests that return only an outcome. */
export interface AdminResponse {
  readonly type: "Admin";
  readonly error: ErrorCodeValue;
  readonly errorMsg: string | null;
  readonly leaderHint: m.LeaderHint | null;
}
export interface QueueActionResponse {
  readonly type: "QueueAction";
  readonly error: ErrorCodeValue;
  readonly errorMsg: string | null;
  readonly count: bigint;
  readonly upTo: bigint;
  readonly more: boolean;
  readonly leaderHint: m.LeaderHint | null;
}
export interface DescribeConfigsResponse {
  readonly type: "DescribeConfigs";
  readonly error: ErrorCodeValue;
  readonly leaderHint: m.LeaderHint | null;
  readonly resources: readonly m.ResourceConfigs[];
}
export interface DescribeMetricsResponse {
  readonly type: "DescribeMetrics";
  readonly error: ErrorCodeValue;
  readonly nodeId: number;
  readonly version: string;
  readonly uptimeMs: bigint;
  readonly metadataGroup: bigint;
  readonly coordinatorGroup: bigint;
  /** Sent as a `u64`, but must fit a `u32`. */
  readonly connections: bigint;
  readonly clientInBytes: bigint;
  readonly clientOutBytes: bigint;
  readonly peerInBytes: bigint;
  readonly peerOutBytes: bigint;
  readonly commitLatency: readonly bigint[];
  readonly host: m.HostMetrics;
  readonly partitions: readonly m.PartitionMetrics[];
  readonly queues: readonly m.QueueMetrics[];
  readonly groups: readonly m.GroupMetrics[];
}
export interface SaslHandshakeResponse {
  readonly type: "SaslHandshake";
  readonly error: ErrorCodeValue;
  readonly mechanisms: readonly string[];
}
export interface SaslAuthenticateResponse {
  readonly type: "SaslAuthenticate";
  readonly error: ErrorCodeValue;
  readonly errorMsg: string | null;
  readonly authBytes: Uint8Array;
  readonly sessionLifetimeMs: bigint;
}
export interface DescribeUsersResponse {
  readonly type: "DescribeUsers";
  readonly error: ErrorCodeValue;
  readonly leaderHint: m.LeaderHint | null;
  readonly users: readonly m.UserEntry[];
}
export interface DescribeCertificatesResponse {
  readonly type: "DescribeCertificates";
  readonly error: ErrorCodeValue;
  readonly leaderHint: m.LeaderHint | null;
  /** The answering node's clock, ms since the Unix epoch, which the states are computed against. */
  readonly nowMs: bigint;
  readonly certificates: readonly m.CertificateEntry[];
}
export interface DescribeCertificateRequestsResponse {
  readonly type: "DescribeCertificateRequests";
  readonly error: ErrorCodeValue;
  readonly leaderHint: m.LeaderHint | null;
  /** The answering node's clock, ms since the Unix epoch. */
  readonly nowMs: bigint;
  readonly requests: readonly m.CertificateRequestEntry[];
}
export interface DescribeTenantsResponse {
  readonly type: "DescribeTenants";
  readonly error: ErrorCodeValue;
  readonly leaderHint: m.LeaderHint | null;
  readonly tenants: readonly m.TenantEntry[];
}
export interface DescribeRoleBindingsResponse {
  readonly type: "DescribeRoleBindings";
  readonly error: ErrorCodeValue;
  readonly leaderHint: m.LeaderHint | null;
  readonly bindings: readonly m.SourcedBinding[];
}
export interface WhoAmIResponse {
  readonly type: "WhoAmI";
  readonly kind: number;
  readonly name: string;
  readonly groups: readonly string[];
  readonly bindings: readonly m.RoleBinding[];
}
export interface DescribeNodesResponse {
  readonly type: "DescribeNodes";
  readonly error: ErrorCodeValue;
  readonly leaderHint: m.LeaderHint | null;
  readonly nodes: readonly m.NodeEntry[];
}
export interface ProduceResponse {
  readonly type: "Produce";
  readonly throttleMs: number;
  readonly resources: readonly m.ProduceResourceResponse[];
}
export interface ListOffsetsResponse {
  readonly type: "ListOffsets";
  readonly resources: readonly m.ListOffsetsResourceResponse[];
}
export interface FetchResponse {
  readonly type: "Fetch";
  readonly throttleMs: number;
  readonly resources: readonly m.FetchResourceResponse[];
}
/** A frame with the ERROR flag and no reason: whole-request refusal, such as an unsupported version. */
export interface ErrorResponse {
  readonly type: "Error";
  readonly code: ErrorCodeValue;
}
/** A frame with the ERROR flag and a reason in tag 0 (a missing role, say). */
export interface RefusedResponse {
  readonly type: "Refused";
  readonly code: ErrorCodeValue;
  readonly message: string;
}

export type Response =
  | ApiVersionsResponse
  | MetadataResponse
  | FindCoordinatorResponse
  | InitProducerResponse
  | AddPartitionsToTxnResponse
  | EndTxnResponse
  | SubscribeResponse
  | PublishResponse
  | OutcomeResponse
  | GroupHeartbeatResponse
  | CommitOffsetsResponse
  | OffsetFetchResponse
  | CreateResponse
  | AdminResponse
  | QueueActionResponse
  | DescribeConfigsResponse
  | DescribeMetricsResponse
  | SaslHandshakeResponse
  | SaslAuthenticateResponse
  | DescribeUsersResponse
  | DescribeCertificatesResponse
  | DescribeCertificateRequestsResponse
  | DescribeTenantsResponse
  | DescribeRoleBindingsResponse
  | WhoAmIResponse
  | DescribeNodesResponse
  | ProduceResponse
  | ListOffsetsResponse
  | FetchResponse
  | ErrorResponse
  | RefusedResponse;

/** Whether the frame carrying this response needs the ERROR flag. */
export function isErrorResponse(res: Response): res is ErrorResponse | RefusedResponse {
  return res.type === "Error" || res.type === "Refused";
}

function putOptU64(w: Writer, v: bigint | null): void {
  w.bool(v !== null);
  if (v !== null) {
    w.u64(v);
  }
}

function getOptU64(r: Reader): bigint | null {
  return r.bool() ? r.u64() : null;
}

/** Writes a response's body (everything after the frame header). */
export function writeResponse(w: Writer, res: Response): void {
  switch (res.type) {
    case "ApiVersions":
      w.array(res.versions, (w1, v) => w1.u16(v.apiKey).u16(v.min).u16(v.max));
      w.array(res.features, (w1, s) => w1.string(s));
      w.noTags();
      return;
    case "Metadata":
      w.array(res.brokers, (w1, b) => {
        w1.u32(b.nodeId).string(b.host).u16(b.port).string(b.rack);
        w1.noTags();
      });
      w.array(res.resources, (w1, x) => {
        w1.u16(x.error).string(x.name).string(x.tenant);
        w1.u64(x.resourceId).u8(x.kind);
        w1.array(x.partitions, (w2, p) => {
          w2.u32(p.partition).u32(p.leader).u64(p.leaderTerm);
          w2.array(p.voters, (w3, v) => w3.u32(v));
          w2.noTags();
        });
        w1.bool(x.exchange !== null);
        if (x.exchange !== null) {
          w1.u8(x.exchange.exchangeType);
          w1.array(x.exchange.bindings, (w2, b) => {
            w2.u64(b.queueId).string(b.bindingKey);
            w2.array(b.headerMatch, m.putHeaderMatch);
            w2.bool(b.matchAll);
            w2.noTags();
          });
        }
        w1.noTags();
      });
      w.noTags();
      return;
    case "FindCoordinator":
      w.u16(res.error).u32(res.nodeId).string(res.host).u16(res.port).optString(res.errorMsg).noTags();
      return;
    case "InitProducer":
      w.u16(res.error).u64(res.producerId).u16(res.producerEpoch);
      m.putHint(w, res.leaderHint);
      return;
    case "AddPartitionsToTxn":
    case "CommitOffsets":
      m.putTxnResults(w, res.resources);
      w.noTags();
      return;
    case "EndTxn":
      w.u16(res.error).u64(res.producerId).u16(res.producerEpoch).noTags();
      return;
    case "Subscribe": {
      w.u16(res.error).u32(res.subscriptionId);
      const tags: Tag[] = [];
      if (res.leaderHint !== null) {
        tags.push({ tag: m.TAG_LEADER_HINT, value: m.hintBytes(res.leaderHint) });
      }
      if (res.standby) {
        tags.push({ tag: m.TAG_STANDBY, value: new Uint8Array([1]) });
      }
      if (res.consumerId !== undefined && res.consumerId !== 0n) {
        tags.push({ tag: m.TAG_CONSUMER, value: new Writer().u64(res.consumerId).toBytes() });
      }
      m.putTagList(w, tags);
      return;
    }
    case "Publish":
      w.u32(res.throttleMs);
      w.array(res.entries, (w1, e) => {
        w1.u16(e.error);
        w1.array(e.routed, (w2, q) => w2.u64(q.queueId).string(q.name).u16(q.error).u64(q.baseOffset));
        w1.noTags();
      });
      w.noTags();
      return;
    case "Outcome":
      w.u16(res.error);
      m.putHint(w, res.leaderHint);
      return;
    case "GroupHeartbeat":
      w.u32(res.throttleMs).u16(res.error).string(res.memberId).i32(res.memberEpoch).u32(res.heartbeatIntervalMs);
      m.putOptResources(w, res.assignment);
      w.noTags();
      return;
    case "OffsetFetch":
      w.array(res.resources, (w1, x) => {
        w1.u64(x.resourceId);
        w1.array(x.partitions, (w2, p) => {
          w2.u32(p.partition).u16(p.error).u64(p.offset).u64(p.leaderTerm);
          w2.optString(p.metadata);
          w2.noTags();
        });
        w1.noTags();
      });
      w.noTags();
      return;
    case "Create":
      w.u16(res.error).u64(res.resourceId).optString(res.errorMsg);
      m.putHint(w, res.leaderHint);
      return;
    case "Admin":
      w.u16(res.error).optString(res.errorMsg);
      m.putHint(w, res.leaderHint);
      return;
    case "QueueAction":
      w.u16(res.error).optString(res.errorMsg).u64(res.count).u64(res.upTo).bool(res.more);
      m.putHint(w, res.leaderHint);
      return;
    case "DescribeConfigs":
      w.u16(res.error);
      m.putHint(w, res.leaderHint);
      w.array(res.resources, (w1, x) => {
        w1.u64(x.resourceId).u16(x.error);
        w1.array(x.configs, (w2, c) => w2.string(c.key).string(c.value).u8(c.source));
      });
      return;
    case "DescribeMetrics":
      w.u16(res.error).u32(res.nodeId).string(res.version);
      w.u64(res.uptimeMs).u64(res.metadataGroup).u64(res.coordinatorGroup).u64(res.connections);
      w.u64(res.clientInBytes).u64(res.clientOutBytes).u64(res.peerInBytes).u64(res.peerOutBytes);
      w.array(res.commitLatency, (w1, v) => w1.u64(v));
      w.bool(res.host.cpuPermille !== null);
      if (res.host.cpuPermille !== null) {
        w.u16(res.host.cpuPermille);
      }
      putOptU64(w, res.host.memoryBytes);
      putOptU64(w, res.host.memoryLimitBytes);
      putOptU64(w, res.host.diskFreeBytes);
      putOptU64(w, res.host.diskTotalBytes);
      w.array(res.partitions, (w1, p) => {
        w1.u64(p.group).u64(p.resourceId).u32(p.partition).bool(p.leader);
        w1.u64(p.term).u64(p.highWatermark).u64(p.lastStable).u64(p.logStart).u64(p.logEnd);
        w1.u64(p.sizeBytes).u64(p.produceBytes).u64(p.produceRecords).u64(p.fetchBytes);
        w1.array(p.replicas, (w2, rp) => w2.u32(rp.nodeId).u64(rp.logEnd));
      });
      w.array(res.queues, (w1, q) => {
        w1.u64(q.resourceId).u64(q.ready).u64(q.inFlight).u64(q.inOpenTxns);
        w1.u64(q.delivered).u64(q.acked);
        w1.array(q.consumers, (w2, c) => w2.u32(c.subscriptionId).string(c.peer).u32(c.inFlight).u32(c.credit));
      });
      w.array(res.groups, (w1, g) => {
        w1.string(g.groupId).string(g.tenant).i32(g.epoch);
        w1.array(g.members, (w2, mem) => {
          w2.string(mem.memberId);
          m.putTxnResources(w2, mem.owned);
        });
        w1.array(g.offsets, (w2, o) => w2.u64(o.resourceId).u32(o.partition).u64(o.offset));
      });
      w.noTags();
      return;
    case "SaslHandshake":
      w.u16(res.error).array(res.mechanisms, (w1, s) => w1.string(s));
      return;
    case "SaslAuthenticate":
      w.u16(res.error).optString(res.errorMsg).bytes(res.authBytes).u64(res.sessionLifetimeMs);
      return;
    case "DescribeUsers":
      w.u16(res.error);
      m.putHint(w, res.leaderHint);
      w.array(res.users, (w1, u) => w1.string(u.name).string(u.mechanism).u32(u.iterations).u8(u.source));
      return;
    case "DescribeCertificateRequests":
      w.u16(res.error);
      m.putHint(w, res.leaderHint);
      w.u64(res.nowMs);
      w.array(res.requests, (w1, c) =>
        w1
          .bytes(c.id)
          .string(c.principal)
          .bytes(c.csr)
          .string(c.requestedBy)
          .u64(c.requestedAtMs)
          .u64(c.expiresAtMs)
          .u8(c.state)
          .bytes(c.certificate)
          .u64(c.signedAtMs),
      );
      return;
    case "DescribeCertificates":
      w.u16(res.error);
      m.putHint(w, res.leaderHint);
      w.u64(res.nowMs);
      w.array(res.certificates, (w1, c) =>
        w1
          .bytes(c.issuer)
          .bytes(c.serial)
          .string(c.principal)
          .u64(c.notBeforeMs)
          .u64(c.notAfterMs)
          .u64(c.firstSeenMs)
          .u64(c.lastSeenMs)
          .u8(c.state)
          .string(c.reason)
          .u64(c.revokedAtMs),
      );
      return;
    case "DescribeTenants":
      w.u16(res.error);
      m.putHint(w, res.leaderHint);
      w.array(res.tenants, (w1, t) => w1.string(t.name).u8(t.source).u32(t.resources));
      return;
    case "DescribeRoleBindings":
      w.u16(res.error);
      m.putHint(w, res.leaderHint);
      w.array(res.bindings, (w1, b) => {
        m.putRoleBinding(w1, b.binding);
        w1.u8(b.source);
      });
      return;
    case "WhoAmI":
      w.u8(res.kind).string(res.name);
      w.array(res.groups, (w1, s) => w1.string(s));
      w.array(res.bindings, m.putRoleBinding);
      return;
    case "DescribeNodes":
      w.u16(res.error);
      m.putHint(w, res.leaderHint);
      w.array(res.nodes, (w1, n) =>
        w1.u64(n.nodeId).string(n.peer).string(n.client).u8(n.state).u32(n.replicas).u32(n.moving),
      );
      return;
    case "Produce":
      w.u32(res.throttleMs);
      w.array(res.resources, (w1, x) => {
        w1.u64(x.resourceId);
        w1.array(x.partitions, (w2, p) => {
          w2.u32(p.partition).u16(p.error).u64(p.baseOffset).i64(p.appendTs);
          w2.optString(p.errorMsg);
          m.putHint(w2, p.leaderHint);
        });
        w1.noTags();
      });
      w.noTags();
      return;
    case "ListOffsets":
      w.array(res.resources, (w1, x) => {
        w1.u64(x.resourceId);
        w1.array(x.partitions, (w2, p) => {
          w2.u32(p.partition).u16(p.error);
          w2.u64(p.offset).i64(p.timestamp);
          m.putHint(w2, p.leaderHint);
        });
        w1.noTags();
      });
      w.noTags();
      return;
    case "Fetch":
      w.u32(res.throttleMs);
      w.array(res.resources, (w1, x) => {
        w1.u64(x.resourceId);
        w1.array(x.partitions, (w2, p) => {
          w2.u32(p.partition).u16(p.error);
          w2.u64(p.highWatermark).u64(p.lastStableOffset).u64(p.logStartOffset);
          w2.array(p.abortedTxns, (w3, a) => w3.u64(a.producerId).u64(a.firstOffset));
          w2.bytes(p.records);
          m.putHint(w2, p.leaderHint);
        });
        w1.noTags();
      });
      w.noTags();
      return;
    case "Error":
      w.u16(res.code).noTags();
      return;
    case "Refused":
      w.u16(res.code);
      w.tags([{ tag: m.TAG_REASON, value: new TextEncoder().encode(res.message) }]);
      return;
  }
}

/** Reads the typed body of the answer to the request under `apiKey`. */
export function readResponse(apiKey: number, r: Reader): Response {
  switch (apiKey) {
    case ApiKey.API_VERSIONS: {
      const versions = r.array(6, (r1) => ({ apiKey: r1.u16(), min: r1.u16(), max: r1.u16() }));
      const features = r.array(2, (r1) => r1.string());
      r.tags();
      return { type: "ApiVersions", versions, features };
    }
    case ApiKey.METADATA: {
      const brokers = r.array(11, (r1) => {
        const b = { nodeId: r1.u32(), host: r1.string(), port: r1.u16(), rack: r1.string() };
        r1.tags();
        return b;
      });
      const resources = r.array(18, (r1) => {
        const error = r1.u16();
        const name = r1.string();
        const tenant = r1.string();
        const resourceId = r1.u64();
        const kind = r1.u8();
        const partitions = r1.array(18, (r2) => {
          const p = {
            partition: r2.u32(),
            leader: r2.u32(),
            leaderTerm: r2.u64(),
            voters: r2.array(4, (r3) => r3.u32()),
          };
          r2.tags();
          return p;
        });
        let exchange: m.ExchangeMetadata | null = null;
        if (r1.bool()) {
          exchange = {
            exchangeType: r1.u8(),
            bindings: r1.array(13, (r2) => {
              const b = {
                queueId: r2.u64(),
                bindingKey: r2.string(),
                headerMatch: r2.array(5, m.getHeaderMatch),
                matchAll: r2.bool(),
              };
              r2.tags();
              return b;
            }),
          };
        }
        r1.tags();
        return { error, name, tenant, resourceId, kind, partitions, exchange };
      });
      r.tags();
      return { type: "Metadata", brokers, resources };
    }
    case ApiKey.FIND_COORDINATOR: {
      const x: FindCoordinatorResponse = {
        type: "FindCoordinator",
        error: r.u16(),
        nodeId: r.u32(),
        host: r.string(),
        port: r.u16(),
        errorMsg: r.optString(),
      };
      r.tags();
      return x;
    }
    case ApiKey.INIT_PRODUCER:
      return {
        type: "InitProducer",
        error: r.u16(),
        producerId: r.u64(),
        producerEpoch: r.u16(),
        leaderHint: m.getHint(r),
      };
    case ApiKey.ADD_PARTITIONS_TO_TXN: {
      const resources = m.getTxnResults(r);
      r.tags();
      return { type: "AddPartitionsToTxn", resources };
    }
    case ApiKey.END_TXN: {
      const x: EndTxnResponse = { type: "EndTxn", error: r.u16(), producerId: r.u64(), producerEpoch: r.u16() };
      r.tags();
      return x;
    }
    case ApiKey.SUBSCRIBE: {
      const error = r.u16();
      const subscriptionId = r.u32();
      let leaderHint: m.LeaderHint | null = null;
      let standby = false;
      let consumerId = 0n;
      for (const t of r.tags()) {
        const vr = new Reader(t.value);
        if (t.tag === m.TAG_LEADER_HINT) {
          leaderHint = { leader: vr.u32(), term: vr.u64() };
        } else if (t.tag === m.TAG_STANDBY) {
          standby = vr.u8() !== 0;
        } else if (t.tag === m.TAG_CONSUMER) {
          consumerId = vr.u64();
        } else {
          continue; // from a newer version: skipped
        }
        vr.finish();
      }
      return { type: "Subscribe", error, subscriptionId, leaderHint, standby, consumerId };
    }
    case ApiKey.ACK:
    case ApiKey.NACK:
    case ApiKey.UNSUBSCRIBE:
    case ApiKey.BIND:
    case ApiKey.UNBIND:
    case 44: // DeadLetter, between nodes: answered like an outcome
      return { type: "Outcome", error: r.u16(), leaderHint: m.getHint(r) };
    case ApiKey.CREATE_STREAM:
    case ApiKey.CREATE_QUEUE:
    case ApiKey.DECLARE_EXCHANGE:
      return {
        type: "Create",
        error: r.u16(),
        resourceId: r.u64(),
        errorMsg: r.optString(),
        leaderHint: m.getHint(r),
      };
    case ApiKey.DELETE_RESOURCE:
    case ApiKey.ALTER_CONFIGS:
    case ApiKey.ALTER_USER:
    case ApiKey.CREATE_TENANT:
    case ApiKey.DELETE_TENANT:
    case ApiKey.ALTER_ROLE_BINDINGS:
    case ApiKey.ADD_NODE:
    case ApiKey.REMOVE_NODE:
    case ApiKey.SETTLE_VOTERS:
    case ApiKey.MOVE_LEADER:
    case ApiKey.REVOKE_SESSIONS:
    case ApiKey.REVOKE_CERTIFICATE:
    case ApiKey.RECORD_CERTIFICATES:
    case ApiKey.REQUEST_CERTIFICATE:
    case ApiKey.RECORD_CERTIFICATE:
    case ApiKey.CREATE_PARTITIONS:
      return { type: "Admin", error: r.u16(), errorMsg: r.optString(), leaderHint: m.getHint(r) };
    case ApiKey.DESCRIBE_NODES: {
      const error = r.u16();
      const leaderHint = m.getHint(r);
      const nodes = r.array(30, (r1) => ({
        nodeId: r1.u64(),
        peer: r1.string(),
        client: r1.string(),
        state: r1.u8(),
        replicas: r1.u32(),
        moving: r1.u32(),
      }));
      return { type: "DescribeNodes", error, leaderHint, nodes };
    }
    case ApiKey.DESCRIBE_CONFIGS: {
      const error = r.u16();
      const leaderHint = m.getHint(r);
      const resources = r.array(11, (r1) => ({
        resourceId: r1.u64(),
        error: r1.u16(),
        configs: r1.array(5, (r2) => ({ key: r2.string(), value: r2.string(), source: r2.u8() })),
      }));
      return { type: "DescribeConfigs", error, leaderHint, resources };
    }
    case ApiKey.DESCRIBE_METRICS:
      return readMetrics(r);
    case ApiKey.SASL_HANDSHAKE:
      return { type: "SaslHandshake", error: r.u16(), mechanisms: r.array(2, (r1) => r1.string()) };
    case ApiKey.SASL_AUTHENTICATE:
      return {
        type: "SaslAuthenticate",
        error: r.u16(),
        errorMsg: r.optString(),
        authBytes: r.bytes(),
        sessionLifetimeMs: r.u64(),
      };
    case ApiKey.DESCRIBE_USERS: {
      const error = r.u16();
      const leaderHint = m.getHint(r);
      const users = r.array(9, (r1) => ({
        name: r1.string(),
        mechanism: r1.string(),
        iterations: r1.u32(),
        source: r1.u8(),
      }));
      return { type: "DescribeUsers", error, leaderHint, users };
    }
    case ApiKey.DESCRIBE_CERTIFICATE_REQUESTS: {
      const error = r.u16();
      const leaderHint = m.getHint(r);
      const nowMs = r.u64();
      // 4 + 2 + 4 + 2 + 2 * 8 + 1 + 4 + 8 bytes at least.
      const requests = r.array(41, (r1) => ({
        id: r1.bytes(),
        principal: r1.string(),
        csr: r1.bytes(),
        requestedBy: r1.string(),
        requestedAtMs: r1.u64(),
        expiresAtMs: r1.u64(),
        state: r1.u8(),
        certificate: r1.bytes(),
        signedAtMs: r1.u64(),
      }));
      return { type: "DescribeCertificateRequests", error, leaderHint, nowMs, requests };
    }
    case ApiKey.DESCRIBE_CERTIFICATES: {
      const error = r.u16();
      const leaderHint = m.getHint(r);
      const nowMs = r.u64();
      // 4 + 4 + 2 + 4 * 8 + 1 + 2 + 8 bytes at least.
      const certificates = r.array(53, (r1) => ({
        issuer: r1.bytes(),
        serial: r1.bytes(),
        principal: r1.string(),
        notBeforeMs: r1.u64(),
        notAfterMs: r1.u64(),
        firstSeenMs: r1.u64(),
        lastSeenMs: r1.u64(),
        state: r1.u8(),
        reason: r1.string(),
        revokedAtMs: r1.u64(),
      }));
      return { type: "DescribeCertificates", error, leaderHint, nowMs, certificates };
    }
    case ApiKey.DESCRIBE_TENANTS: {
      const error = r.u16();
      const leaderHint = m.getHint(r);
      const tenants = r.array(7, (r1) => ({ name: r1.string(), source: r1.u8(), resources: r1.u32() }));
      return { type: "DescribeTenants", error, leaderHint, tenants };
    }
    case ApiKey.DESCRIBE_ROLE_BINDINGS: {
      const error = r.u16();
      const leaderHint = m.getHint(r);
      const bindings = r.array(m.BINDING_MIN + 1, (r1) => ({ binding: m.getRoleBinding(r1), source: r1.u8() }));
      return { type: "DescribeRoleBindings", error, leaderHint, bindings };
    }
    case ApiKey.WHO_AM_I:
      return {
        type: "WhoAmI",
        kind: r.u8(),
        name: r.string(),
        groups: r.array(2, (r1) => r1.string()),
        bindings: r.array(m.BINDING_MIN, m.getRoleBinding),
      };
    case ApiKey.GROUP_HEARTBEAT: {
      const x: GroupHeartbeatResponse = {
        type: "GroupHeartbeat",
        throttleMs: r.u32(),
        error: r.u16(),
        memberId: r.string(),
        memberEpoch: r.i32(),
        heartbeatIntervalMs: r.u32(),
        assignment: m.getOptResources(r),
      };
      r.tags();
      return x;
    }
    case ApiKey.COMMIT_OFFSETS:
    case ApiKey.ADD_OFFSETS_TO_TXN: {
      const resources = m.getTxnResults(r);
      r.tags();
      return { type: "CommitOffsets", resources };
    }
    case ApiKey.OFFSET_FETCH: {
      const resources = r.array(10, (r1) => {
        const resourceId = r1.u64();
        const partitions = r1.array(25, (r2) => {
          const p = {
            partition: r2.u32(),
            error: r2.u16(),
            offset: r2.u64(),
            leaderTerm: r2.u64(),
            metadata: r2.optString(),
          };
          r2.tags();
          return p;
        });
        r1.tags();
        return { resourceId, partitions };
      });
      r.tags();
      return { type: "OffsetFetch", resources };
    }
    case ApiKey.PRODUCE: {
      const throttleMs = r.u32();
      const resources = r.array(10, (r1) => {
        const resourceId = r1.u64();
        const partitions = r1.array(25, (r2) => ({
          partition: r2.u32(),
          error: r2.u16(),
          baseOffset: r2.u64(),
          appendTs: r2.i64(),
          errorMsg: r2.optString(),
          leaderHint: m.getHint(r2),
        }));
        r1.tags();
        return { resourceId, partitions };
      });
      r.tags();
      return { type: "Produce", throttleMs, resources };
    }
    case ApiKey.FETCH: {
      const throttleMs = r.u32();
      const resources = r.array(10, (r1) => {
        const resourceId = r1.u64();
        const partitions = r1.array(36, (r2) => ({
          partition: r2.u32(),
          error: r2.u16(),
          highWatermark: r2.u64(),
          lastStableOffset: r2.u64(),
          logStartOffset: r2.u64(),
          abortedTxns: r2.array(16, (r3) => ({ producerId: r3.u64(), firstOffset: r3.u64() })),
          records: r2.bytes(),
          leaderHint: m.getHint(r2),
        }));
        r1.tags();
        return { resourceId, partitions };
      });
      r.tags();
      return { type: "Fetch", throttleMs, resources };
    }
    case ApiKey.LIST_OFFSETS: {
      const resources = r.array(10, (r1) => {
        const resourceId = r1.u64();
        const partitions = r1.array(23, (r2) => ({
          partition: r2.u32(),
          error: r2.u16(),
          offset: r2.u64(),
          timestamp: r2.i64(),
          leaderHint: m.getHint(r2),
        }));
        r1.tags();
        return { resourceId, partitions };
      });
      r.tags();
      return { type: "ListOffsets", resources };
    }
    case ApiKey.PURGE_QUEUE:
    case ApiKey.REQUEUE:
      return {
        type: "QueueAction",
        error: r.u16(),
        errorMsg: r.optString(),
        count: r.u64(),
        upTo: r.u64(),
        more: r.bool(),
        leaderHint: m.getHint(r),
      };
    case ApiKey.PUBLISH: {
      const throttleMs = r.u32();
      const entries = r.array(4, (r1) => {
        const error = r1.u16();
        const routed = r1.array(20, (r2) => ({
          queueId: r2.u64(),
          name: r2.string(),
          error: r2.u16(),
          baseOffset: r2.u64(),
        }));
        r1.tags();
        return { error, routed };
      });
      r.tags();
      return { type: "Publish", throttleMs, entries };
    }
    default:
      throw DecodeError.malformed("unknown API key");
  }
}

function readMetrics(r: Reader): DescribeMetricsResponse {
  const error = r.u16();
  const nodeId = r.u32();
  const version = r.string();
  const uptimeMs = r.u64();
  const metadataGroup = r.u64();
  const coordinatorGroup = r.u64();
  const connections = r.u64();
  if (BigInt.asUintN(64, connections) > BigInt(U32_MAX)) {
    throw DecodeError.malformed("connections");
  }
  const clientInBytes = r.u64();
  const clientOutBytes = r.u64();
  const peerInBytes = r.u64();
  const peerOutBytes = r.u64();
  const commitLatency = r.array(8, (r1) => r1.u64());
  const cpuPermille = r.bool() ? r.u16() : null;
  const host: m.HostMetrics = {
    cpuPermille,
    memoryBytes: getOptU64(r),
    memoryLimitBytes: getOptU64(r),
    diskFreeBytes: getOptU64(r),
    diskTotalBytes: getOptU64(r),
  };
  const partitions = r.array(8 + 8 + 4 + 1 + 9 * 8 + 1, (r1) => {
    const group = r1.u64();
    const resourceId = r1.u64();
    const partition = r1.u32();
    const leader = r1.bool();
    const v: bigint[] = [];
    for (let i = 0; i < 9; i++) {
      v.push(r1.u64());
    }
    const replicas = r1.array(12, (r2) => ({ nodeId: r2.u32(), logEnd: r2.u64() }));
    const [term, highWatermark, lastStable, logStart, logEnd, sizeBytes, produceBytes, produceRecords, fetchBytes] =
      v as [bigint, bigint, bigint, bigint, bigint, bigint, bigint, bigint, bigint];
    return {
      group,
      resourceId,
      partition,
      leader,
      term,
      highWatermark,
      lastStable,
      logStart,
      logEnd,
      sizeBytes,
      produceBytes,
      produceRecords,
      fetchBytes,
      replicas,
    };
  });
  const queues = r.array(6 * 8 + 1, (r1) => {
    const resourceId = r1.u64();
    const ready = r1.u64();
    const inFlight = r1.u64();
    const inOpenTxns = r1.u64();
    const delivered = r1.u64();
    const acked = r1.u64();
    const consumers = r1.array(4 + 2 + 4 + 4, (r2) => ({
      subscriptionId: r2.u32(),
      peer: r2.string(),
      inFlight: r2.u32(),
      credit: r2.u32(),
    }));
    return { resourceId, ready, inFlight, inOpenTxns, delivered, acked, consumers };
  });
  const groups = r.array(2 + 2 + 4 + 1 + 1, (r1) => {
    const groupId = r1.string();
    const tenant = r1.string();
    const epoch = r1.i32();
    const members = r1.array(2 + 1, (r2) => ({ memberId: r2.string(), owned: m.getTxnResources(r2) }));
    const offsets = r1.array(8 + 4 + 8, (r2) => ({ resourceId: r2.u64(), partition: r2.u32(), offset: r2.u64() }));
    return { groupId, tenant, epoch, members, offsets };
  });
  r.tags();
  return {
    type: "DescribeMetrics",
    error,
    nodeId,
    version,
    uptimeMs,
    metadataGroup,
    coordinatorGroup,
    connections,
    clientInBytes,
    clientOutBytes,
    peerInBytes,
    peerOutBytes,
    commitLatency,
    host,
    partitions,
    queues,
    groups,
  };
}
