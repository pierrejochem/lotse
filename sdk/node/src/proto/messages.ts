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
export const TAG_PRODUCER = 2;

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
}

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
}

export interface ConsumerMetrics {
  readonly subscriptionId: number;
  readonly peer: string;
  readonly inFlight: number;
  readonly credit: number;
}

export interface QueueMetrics {
  readonly resourceId: bigint;
  readonly ready: bigint;
  readonly inFlight: bigint;
  readonly inOpenTxns: bigint;
  readonly delivered: bigint;
  readonly acked: bigint;
  readonly consumers: readonly ConsumerMetrics[];
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
