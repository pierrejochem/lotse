import type { ErrorCodeValue } from "./error-code.js";
import { Reader, type Tag, } from "./reader.js";
import { Writer } from "./writer.js";

/**
 * The parts that requests and responses share. Field types follow the wire: `u64` is `bigint`, every narrower
 * integer is `number`. Optional wire fields are `null` when absent.
 */

/** "No committed offset", `u64::MAX` as a bit pattern. */
export const NO_OFFSET = -1n;
export const LATENCY_BUCKETS = 17;
export const DEFAULT_TENANT = "default";

export const ResourceKind = { STREAM: 0, QUEUE: 1, EXCHANGE: 2 } as const;
export const ExchangeType = { DIRECT: 0, TOPIC: 1, FANOUT: 2, HEADERS: 3 } as const;
export const Isolation = { UNCOMMITTED: 0, COMMITTED: 1 } as const;
export const CoordinatorType = { GROUP: 0, PRODUCER: 1 } as const;
export const ConfigSource = { DEFAULT: 0, DYNAMIC: 1, STATIC: 2 } as const;
export const SubjectKind = { USER: 0, GROUP: 1 } as const;
export const Role = {
  CLUSTER_ADMIN: 0,
  TENANT_ADMIN: 1,
  PRODUCER: 2,
  CONSUMER: 3,
  VIEWER: 4,
  CLUSTER_VIEWER: 5,
} as const;
export const PrincipalKind = { ANONYMOUS: 0, NODE: 1, USER: 2 } as const;
export const NodeState = { ACTIVE: 0, DRAINING: 1 } as const;
/** Timestamps of `ListOffsets`: the first offset still stored, and the offset after the last one a reader may see. */
export const ListOffsetsAt = { LATEST: -1n, EARLIEST: -2n } as const;

// ---- tags ----

export const TAG_LEADER_HINT = 0;
export const TAG_REASON = 0;
export const TAG_TENANT = 1;
export const TAG_INSTANCE = 2;
export const TAG_PATTERN = 3;
export const TAG_NOTIFY_ACTIVE = 1;
export const TAG_STANDBY = 1;
/** The consumer a subscription resumes (ADR 0109). */
export const TAG_RESUME = 2;
/** The consumer id the leader gave a subscription (ADR 0109). */
export const TAG_CONSUMER = 2;
/** The messages a resuming consumer holds (ADR 0112). */
export const TAG_HELD = 3;
export const TAG_PRODUCER = 2;
/** The subscription a requeueing `Nack` came on (ADR 0106). */
export const TAG_NACK_SUBSCRIPTION = 0;
/** A queue's ready messages per priority level and its subscriptions' states, in a `DescribeMetrics` answer (ADR 0120). */
export const TAG_QUEUE_DETAILS = 3;
/** Each partition's oldest record and open transactions, in a `DescribeMetrics` answer (ADR 0122). */
export const TAG_PARTITION_DETAILS = 4;
/** The bytes each queue consumer holds, in a `DescribeMetrics` answer (ADR 0122). */
export const TAG_IN_FLIGHT_BYTES = 5;
/** The node's free disk limit, in a `DescribeMetrics` answer (ADR 0122). */
export const TAG_DISK = 6;

/** Where a new leader is, after `NOT_LEADER` or `FENCED_LEADER_TERM`. */
export interface LeaderHint {
  readonly leader: number;
  readonly term: bigint;
}

export function putHint(w: Writer, hint: LeaderHint | null | undefined): void {
  if (hint === null || hint === undefined) {
    w.noTags();
    return;
  }
  w.tags([{ tag: TAG_LEADER_HINT, value: hintBytes(hint) }]);
}

export function hintBytes(hint: LeaderHint): Uint8Array {
  return new Writer().u32(hint.leader).u64(hint.term).toBytes();
}

export function hintFromBytes(value: Uint8Array): LeaderHint {
  const vr = new Reader(value);
  const hint = { leader: vr.u32(), term: vr.u64() };
  vr.finish();
  return hint;
}

/** Reads a tag block that is only a leader hint (unknown tags are skipped). */
export function getHint(r: Reader): LeaderHint | null {
  let hint: LeaderHint | null = null;
  for (const t of r.tags()) {
    if (t.tag === TAG_LEADER_HINT) {
      hint = hintFromBytes(t.value);
    }
  }
  return hint;
}

export function stringTag(tag: number, value: string): Tag {
  return { tag, value: new Writer().string(value).toBytes() };
}

/** Writes a tag block that holds the tenant, if it is not `default`. */
export function putTenant(w: Writer, tenant: string): void {
  if (tenant === DEFAULT_TENANT) {
    w.noTags();
    return;
  }
  w.tags([stringTag(TAG_TENANT, tenant)]);
}

export function getTenant(r: Reader): string {
  let tenant = DEFAULT_TENANT;
  for (const t of r.tags()) {
    if (t.tag === TAG_TENANT) {
      const vr = new Reader(t.value);
      tenant = vr.string();
      vr.finish();
    }
  }
  return tenant;
}

export function tenantOrDefault(tenant: string | null | undefined): string {
  return tenant ?? DEFAULT_TENANT;
}

/** Writes a tag block, empty if there are no tags. */
export function putTagList(w: Writer, tags: readonly Tag[]): void {
  if (tags.length === 0) {
    w.noTags();
  } else {
    w.tags(tags);
  }
}

// ---- shared structures ----

export interface Config {
  readonly key: string;
  readonly value: string;
}

export function putConfigs(w: Writer, configs: readonly Config[]): void {
  w.array(configs, (w1, c) => w1.string(c.key).string(c.value));
}

export function getConfigs(r: Reader): Config[] {
  return r.array(4, (r1) => ({ key: r1.string(), value: r1.string() }));
}

/** A half-open range of offsets, `[from, to)`. */
export interface Range {
  readonly from: bigint;
  readonly to: bigint;
}

export function putRanges(w: Writer, ranges: readonly Range[]): void {
  w.array(ranges, (w1, x) => w1.u64(x.from).u64(x.to));
}

export function getRanges(r: Reader): Range[] {
  return r.array(16, (r1) => ({ from: r1.u64(), to: r1.u64() }));
}

export interface HeaderMatch {
  readonly key: Uint8Array;
  /** `null` matches on the header's presence only. */
  readonly value: Uint8Array | null;
}

export function putHeaderMatch(w: Writer, h: HeaderMatch): void {
  w.bytes(h.key);
  w.bool(h.value !== null);
  if (h.value !== null) {
    w.bytes(h.value);
  }
}

export function getHeaderMatch(r: Reader): HeaderMatch {
  const key = r.bytes();
  return { key, value: r.bool() ? r.bytes() : null };
}

/** A resource and some of its partitions. */
export interface TxnResource {
  readonly resourceId: bigint;
  readonly partitions: readonly number[];
}

function putTxnResource(w: Writer, t: TxnResource): void {
  w.u64(t.resourceId);
  w.array(t.partitions, (w1, p) => w1.u32(p));
  w.noTags();
}

function getTxnResource(r: Reader): TxnResource {
  const t = { resourceId: r.u64(), partitions: r.array(4, (r1) => r1.u32()) };
  r.tags();
  return t;
}

export function putTxnResources(w: Writer, resources: readonly TxnResource[]): void {
  w.array(resources, putTxnResource);
}

export function getTxnResources(r: Reader): TxnResource[] {
  return r.array(10, getTxnResource);
}

export function putOptResources(w: Writer, resources: readonly TxnResource[] | null | undefined): void {
  w.bool(resources !== null && resources !== undefined);
  if (resources !== null && resources !== undefined) {
    putTxnResources(w, resources);
  }
}

export function getOptResources(r: Reader): TxnResource[] | null {
  return r.bool() ? getTxnResources(r) : null;
}

export interface TxnPartitionResult {
  readonly partition: number;
  readonly error: ErrorCodeValue;
}

export interface TxnResourceResult {
  readonly resourceId: bigint;
  readonly partitions: readonly TxnPartitionResult[];
}

export function putTxnResults(w: Writer, results: readonly TxnResourceResult[]): void {
  w.array(results, (w1, t) => {
    w1.u64(t.resourceId);
    w1.array(t.partitions, (w2, p) => {
      w2.u32(p.partition);
      w2.u16(p.error);
      w2.noTags();
    });
    w1.noTags();
  });
}

export function getTxnResults(r: Reader): TxnResourceResult[] {
  return r.array(10, (r1) => {
    const resourceId = r1.u64();
    const partitions = r1.array(7, (r2) => {
      const p = { partition: r2.u32(), error: r2.u16() };
      r2.tags();
      return p;
    });
    r1.tags();
    return { resourceId, partitions };
  });
}

// ---- metadata ----

export interface Broker {
  readonly nodeId: number;
  readonly host: string;
  readonly port: number;
  readonly rack: string;
}

export interface PartitionMetadata {
  readonly partition: number;
  /** 0 means unknown. */
  readonly leader: number;
  readonly leaderTerm: bigint;
  readonly voters: readonly number[];
  /** The node a cluster admin pinned its leader to; 0 or absent for none (tag 0, ADR 0121). */
  readonly pinnedLeader?: number;
  /** Whether a cluster admin placed it on named nodes (tag 1, ADR 0121). */
  readonly placed?: boolean;
}

/** Tag of a partition's pinned leader in a `Metadata` answer (ADR 0121). */
export const TAG_PINNED_LEADER = 0;
/** Tag of a placed partition in a `Metadata` answer: present, empty, when it is placed (ADR 0121). */
export const TAG_PLACED = 1;

export interface BindingMetadata {
  readonly queueId: bigint;
  readonly bindingKey: string;
  readonly headerMatch: readonly HeaderMatch[];
  readonly matchAll: boolean;
}

export interface ExchangeMetadata {
  readonly exchangeType: number;
  readonly bindings: readonly BindingMetadata[];
}

export interface ResourceMetadata {
  readonly error: ErrorCodeValue;
  readonly name: string;
  readonly tenant: string;
  readonly resourceId: bigint;
  readonly kind: number;
  readonly partitions: readonly PartitionMetadata[];
  readonly exchange: ExchangeMetadata | null;
}

// ---- produce, fetch, list offsets ----

export interface ProducePartition {
  readonly partition: number;
  readonly batch: Uint8Array;
}

export interface ProduceResource {
  readonly resourceId: bigint;
  readonly partitions: readonly ProducePartition[];
}

export interface ProducePartitionResponse {
  readonly partition: number;
  readonly error: ErrorCodeValue;
  readonly baseOffset: bigint;
  readonly appendTs: bigint;
  readonly errorMsg: string | null;
  readonly leaderHint: LeaderHint | null;
}

export interface ProduceResourceResponse {
  readonly resourceId: bigint;
  readonly partitions: readonly ProducePartitionResponse[];
}

export interface FetchPartition {
  readonly partition: number;
  readonly fetchOffset: bigint;
  readonly leaderTerm: bigint;
  readonly maxBytes: number;
}

export interface FetchResource {
  readonly resourceId: bigint;
  readonly partitions: readonly FetchPartition[];
}

export interface AbortedTxn {
  readonly producerId: bigint;
  readonly firstOffset: bigint;
}

export interface FetchPartitionResponse {
  readonly partition: number;
  readonly error: ErrorCodeValue;
  readonly highWatermark: bigint;
  readonly lastStableOffset: bigint;
  readonly logStartOffset: bigint;
  readonly abortedTxns: readonly AbortedTxn[];
  /** Whole batches, one after the other. */
  readonly records: Uint8Array;
  readonly leaderHint: LeaderHint | null;
}

export interface FetchResourceResponse {
  readonly resourceId: bigint;
  readonly partitions: readonly FetchPartitionResponse[];
}

export interface ListOffsetsPartition {
  readonly partition: number;
  readonly timestamp: bigint;
}

export interface ListOffsetsResource {
  readonly resourceId: bigint;
  readonly partitions: readonly ListOffsetsPartition[];
}

export interface ListOffsetsPartitionResponse {
  readonly partition: number;
  readonly error: ErrorCodeValue;
  readonly offset: bigint;
  readonly timestamp: bigint;
  readonly leaderHint: LeaderHint | null;
}

export interface ListOffsetsResourceResponse {
  readonly resourceId: bigint;
  readonly partitions: readonly ListOffsetsPartitionResponse[];
}

// ---- queues and publishing ----

export interface DeliveredOffset {
  readonly offset: bigint;
  readonly deliveryCount: number;
}

export interface PublishEntry {
  readonly exchangeId: bigint;
  readonly routingKey: string;
  readonly mandatory: boolean;
  readonly batch: Uint8Array;
}

export interface RoutedMessage {
  readonly queueId: bigint;
  readonly name: string;
  readonly error: ErrorCodeValue;
  readonly baseOffset: bigint;
}

export interface PublishResult {
  readonly error: ErrorCodeValue;
  readonly routed: readonly RoutedMessage[];
}

// ---- consumer groups ----

export interface OffsetCommitPartition {
  readonly partition: number;
  readonly offset: bigint;
  readonly leaderTerm: bigint;
  readonly metadata: string | null;
}

export interface OffsetCommitResource {
  readonly resourceId: bigint;
  readonly partitions: readonly OffsetCommitPartition[];
}

export function putCommits(w: Writer, resources: readonly OffsetCommitResource[]): void {
  w.array(resources, (w1, res) => {
    w1.u64(res.resourceId);
    w1.array(res.partitions, (w2, p) => {
      w2.u32(p.partition);
      w2.u64(p.offset);
      w2.u64(p.leaderTerm);
      w2.optString(p.metadata);
      w2.noTags();
    });
    w1.noTags();
  });
}

export function getCommits(r: Reader): OffsetCommitResource[] {
  return r.array(10, (r1) => {
    const resourceId = r1.u64();
    const partitions = r1.array(23, (r2) => {
      const p = { partition: r2.u32(), offset: r2.u64(), leaderTerm: r2.u64(), metadata: r2.optString() };
      r2.tags();
      return p;
    });
    r1.tags();
    return { resourceId, partitions };
  });
}

export interface OffsetFetchPartition {
  readonly partition: number;
  readonly error: ErrorCodeValue;
  /** `NO_OFFSET` when none was committed. */
  readonly offset: bigint;
  readonly leaderTerm: bigint;
  readonly metadata: string | null;
}

export interface OffsetFetchResource {
  readonly resourceId: bigint;
  readonly partitions: readonly OffsetFetchPartition[];
}

// ---- admin ----

export interface ConfigEntry {
  readonly key: string;
  readonly value: string;
  readonly source: number;
}

export interface ResourceConfigs {
  readonly resourceId: bigint;
  readonly error: ErrorCodeValue;
  readonly configs: readonly ConfigEntry[];
}

export interface ScramCredentialData {
  readonly iterations: number;
  readonly salt: Uint8Array;
  readonly storedKey: Uint8Array;
  readonly serverKey: Uint8Array;
}

export interface UserEntry {
  readonly name: string;
  readonly mechanism: string;
  readonly iterations: number;
  readonly source: number;
}

/** How a certificate stands in an inventory entry (ADR 0091). */
export const CertificateState = {
  /** Valid, and not ending within 30 days. */
  VALID: 0,
  /** Valid, and ending within 30 days. */
  EXPIRING: 1,
  /** Past its end. */
  EXPIRED: 2,
  /** Revoked by a cluster admin (`RevokeCertificate`). */
  REVOKED: 3,
} as const;

/** Who a session acts as, in {@link SessionEntry.principalKind} (ADR 0121). */
export const SessionPrincipal = {
  /** Nobody in particular: the node requires no authentication. */
  ANONYMOUS: 0,
  /** A user or service, by name. */
  USER: 1,
  /** A broker node, by its certificate; the name is its id. */
  NODE: 2,
} as const;

/** How a session signed in, in {@link SessionEntry.mechanism} (ADR 0121). */
export const SessionMechanism = {
  /** It did not: the node requires no authentication. */
  NONE: 0,
  /** With its TLS client certificate. */
  CERTIFICATE: 1,
  /** SASL SCRAM-SHA-512. */
  SCRAM_SHA_512: 2,
  /** SASL OAUTHBEARER. */
  OAUTHBEARER: 3,
  /** An access token (`LOTSE-TOKEN`, ADR 0133); the entry names it in {@link SessionEntry.tokenId}. */
  TOKEN: 4,
} as const;

/** The tag of {@link SessionEntry.tokenId} in a session entry's block. */
export const TAG_SESSION_TOKEN_ID = 0;

/** One open client session on a node (`DescribeSessions`, ADR 0121). */
export interface SessionEntry {
  /** See {@link SessionPrincipal}. */
  readonly principalKind: number;
  /** The user's name, or the node's id; empty for anonymous. */
  readonly name: string;
  /** Groups at the identity provider (OAUTHBEARER only). */
  readonly groups: readonly string[];
  /** See {@link SessionMechanism}. */
  readonly mechanism: number;
  /** The other end's address as the node sees it. */
  readonly peer: string;
  /** When the connection was made, ms since the Unix epoch. */
  readonly connectedMs: bigint;
  /** When its credential dates from: a token's issue time (0 without one), the SCRAM login, or the connection. */
  readonly credentialMs: bigint;
  /** When the session ends unless renewed; 0 for one that lasts the connection. */
  readonly expiresMs: bigint;
  /** The id of the access token the session signed in with; absent for every other mechanism (tag 0, ADR 0133). */
  readonly tokenId?: bigint;
}

/** One access token in `DescribeTokens` (ADR 0133): only what describes it, never a secret or hash. */
export interface TokenEntry {
  /** The token's id, a `u64`. */
  readonly id: bigint;
  /** Its label. */
  readonly name: string;
  /** The user it acts as. */
  readonly owner: string;
  /** When it was created, ms since the Unix epoch. */
  readonly createdAtMs: bigint;
  /** When it stops being valid, ms since the Unix epoch. */
  readonly expiresAtMs: bigint;
  /** Who created it. */
  readonly createdBy: string;
}

/** One client certificate in the inventory (`DescribeCertificates`). */
export interface CertificateEntry {
  /** SHA-256 of the issuer's DER name and authority key identifier (ADR 0096) (32 bytes). */
  readonly issuer: Uint8Array;
  /** The serial number, big-endian. */
  readonly serial: Uint8Array;
  /** The principal it names (its common name); empty for a certificate revoked before any node saw it. */
  readonly principal: string;
  /** First millisecond it is valid in, since the Unix epoch; 0 if unknown. */
  readonly notBeforeMs: bigint;
  /** Millisecond it stops being valid in, since the Unix epoch. */
  readonly notAfterMs: bigint;
  /** When a node first accepted it, ms since the Unix epoch; 0 if none did. */
  readonly firstSeenMs: bigint;
  /** When a node last accepted it, as far as nodes report: at most an hour late. */
  readonly lastSeenMs: bigint;
  /** A {@link CertificateState}. */
  readonly state: number;
  /** Why it was revoked; empty if it was not. */
  readonly reason: string;
  /** When it was revoked, ms since the Unix epoch; 0 if it was not. */
  readonly revokedAtMs: bigint;
}

/** Where a certificate request stands (ADR 0092). */
export const CertificateRequestState = {
  /** Waiting for the CA holder. */
  PENDING: 0,
  /** Signed; the certificate is in the entry. */
  SIGNED: 1,
  /** Unsigned past its seven days. */
  EXPIRED: 2,
} as const;

/** One certificate request (`DescribeCertificateRequests`). */
export interface CertificateRequestEntry {
  /** SHA-256 of the signing request (32 bytes). */
  readonly id: Uint8Array;
  /** The principal asked for. */
  readonly principal: string;
  /** The signing request, DER. */
  readonly csr: Uint8Array;
  /** Who asked. */
  readonly requestedBy: string;
  /** When, ms since the Unix epoch. */
  readonly requestedAtMs: bigint;
  /** When an unsigned request lapses, ms since the Unix epoch. */
  readonly expiresAtMs: bigint;
  /** A {@link CertificateRequestState}. */
  readonly state: number;
  /** The signed certificate, DER; empty until signed. */
  readonly certificate: Uint8Array;
  /** When it was signed, ms since the Unix epoch; 0 if not yet. */
  readonly signedAtMs: bigint;
}

/** One accepted client certificate, as a node reports it (`RecordCertificates`, internal). */
export interface SeenCertificate {
  readonly issuer: Uint8Array;
  readonly serial: Uint8Array;
  readonly principal: string;
  readonly notBeforeMs: bigint;
  readonly notAfterMs: bigint;
  /** When the node accepted it, by its clock. */
  readonly seenMs: bigint;
}

export interface TenantEntry {
  readonly name: string;
  readonly source: number;
  readonly resources: number;
}

export interface RoleBinding {
  readonly subjectKind: number;
  readonly subject: string;
  readonly role: number;
  readonly tenant: string;
  readonly prefix: string;
}

/** The fewest bytes one role binding takes: u8, string, u8, string, string. */
export const BINDING_MIN = 1 + 2 + 1 + 2 + 2;

export function putRoleBinding(w: Writer, b: RoleBinding): void {
  w.u8(b.subjectKind).string(b.subject).u8(b.role).string(b.tenant).string(b.prefix);
}

export function getRoleBinding(r: Reader): RoleBinding {
  return { subjectKind: r.u8(), subject: r.string(), role: r.u8(), tenant: r.string(), prefix: r.string() };
}

export interface SourcedBinding {
  readonly binding: RoleBinding;
  readonly source: number;
}

export interface NodeEntry {
  readonly nodeId: bigint;
  readonly peer: string;
  readonly client: string;
  readonly state: number;
  readonly replicas: number;
  readonly moving: number;
}

// ---- metrics ----

export interface HostMetrics {
  readonly cpuPermille: number | null;
  readonly memoryBytes: bigint | null;
  readonly memoryLimitBytes: bigint | null;
  readonly diskFreeBytes: bigint | null;
  readonly diskTotalBytes: bigint | null;
}

/** A node's free space limit (ADR 0122). */
export interface DiskMetrics {
  /** The leaders of the groups it votes in take no new data while its data volume has fewer free bytes than this; 0 for no limit. */
  readonly freeLimitBytes: bigint;
  /** Whether it is below the limit now: it goes below when free space falls under the limit and back once it is a tenth above it. */
  readonly low: boolean;
}

export interface ReplicaProgress {
  readonly nodeId: number;
  readonly logEnd: bigint;
}

export interface PartitionMetrics {
  readonly group: bigint;
  readonly resourceId: bigint;
  readonly partition: number;
  readonly leader: boolean;
  readonly term: bigint;
  readonly highWatermark: bigint;
  readonly lastStable: bigint;
  readonly logStart: bigint;
  readonly logEnd: bigint;
  readonly sizeBytes: bigint;
  readonly produceBytes: bigint;
  readonly produceRecords: bigint;
  readonly fetchBytes: bigint;
  readonly replicas: readonly ReplicaProgress[];
  /** When the oldest data record still stored was written, ms since the epoch; `null` for an empty log or a node's own groups (ADR 0122). */
  readonly oldestTimestampMs: bigint | null;
  /** Transactions open on this partition, as this replica knows them (ADR 0122). */
  readonly openTransactions: number;
}

/**
 * What a subscription does on its queue (ADR 0120): every subscription gets messages (`shared`), or the queue has a
 * single active consumer and this is it (`active`) or waits for its turn (`standby`).
 */
export type ConsumerState = "shared" | "active" | "standby";

/** The code of a consumer state on the wire. */
export function consumerStateCode(state: ConsumerState): number {
  return state === "active" ? 1 : state === "standby" ? 2 : 0;
}

/** The state a code stands for; a code from a newer version reads as `shared`. */
export function consumerStateOf(code: number): ConsumerState {
  return code === 1 ? "active" : code === 2 ? "standby" : "shared";
}

export interface ConsumerMetrics {
  readonly subscriptionId: number;
  readonly peer: string;
  readonly inFlight: number;
  readonly credit: number;
  /** Shared, active or standby (ADR 0120). */
  readonly state: ConsumerState;
  /** Bytes of the messages it holds, each message's share of its batch as stored (ADR 0122). */
  readonly inFlightBytes: bigint;
}

export interface QueueMetrics {
  readonly resourceId: bigint;
  readonly ready: bigint;
  readonly inFlight: bigint;
  readonly inOpenTxns: bigint;
  readonly delivered: bigint;
  readonly acked: bigint;
  readonly consumers: readonly ConsumerMetrics[];
  /** On a queue with priority levels: ready messages per level, level 0 first; empty otherwise (ADR 0120). */
  readonly readyByLevel: readonly bigint[];
  /** On a queue with priority levels: ready messages whose level the leader has not read yet; with `readyByLevel` they sum to `ready`. */
  readonly readyLevelUnread: bigint;
}

export interface GroupMemberMetrics {
  readonly memberId: string;
  readonly owned: readonly TxnResource[];
}

export interface CommittedOffset {
  readonly resourceId: bigint;
  readonly partition: number;
  readonly offset: bigint;
}

export interface GroupMetrics {
  readonly groupId: string;
  readonly tenant: string;
  readonly epoch: number;
  readonly members: readonly GroupMemberMetrics[];
  readonly offsets: readonly CommittedOffset[];
}
