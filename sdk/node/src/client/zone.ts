import { COORDINATOR_GROUP, type GroupIndex, METADATA_GROUP } from "../proto/frames.js";
import type { LeaderHint } from "../proto/messages.js";
import type { Response } from "../proto/response.js";
import { ErrorCode } from "../proto/error-code.js";
import { compareU64 } from "../record/unsigned.js";

/**
 * Reads in this client's zone (the zone reads design): the indexes of the metadata and coordinator groups this client
 * has seen, which a node in the zone must have applied before it answers a read, and the choice of that node.
 */

/**
 * The highest index of each group named in an answer's `applied`, shared by a client and its connections. An index
 * only rises: a follower further behind than an answer seen before must not make a later read accept older state.
 */
export class Indexes {
  // Both groups at 0: a read names them even before any answer did, so a node can tell a client that saw nothing from
  // one that sends no tag.
  private readonly known = new Map<bigint, bigint>([
    [METADATA_GROUP, 0n],
    [COORDINATOR_GROUP, 0n],
  ]);

  /** Takes in what an answer says its node had applied. */
  raise(applied: readonly GroupIndex[]): void {
    for (const e of applied) {
      const index = this.known.get(e.group);
      // Indexes are u64 bit patterns: compared as unsigned numbers.
      if (index === undefined || compareU64(index, e.index) < 0) {
        this.known.set(e.group, e.index);
      }
    }
  }

  /** What a zone read asks for: every group's index, in group order. */
  snapshot(): GroupIndex[] {
    return [...this.known.entries()]
      .sort((a, b) => compareU64(a[0], b[0]))
      .map(([group, index]) => ({ group, index }));
  }
}

/**
 * The lowest-id node of `candidates` in `zone`, as `zones` has it, or `null`: one fixed choice, so a client's reads meet
 * the same node's connection.
 */
export function zoneNode(zone: string, candidates: Iterable<number>, zones: ReadonlyMap<number, string>): number | null {
  let best: number | null = null;
  for (const n of candidates) {
    if (zones.get(n) === zone && (best === null || n < best)) {
      best = n;
    }
  }
  return best;
}

/**
 * Whether a zone node's answer to a zone read is no answer: an error, of the whole read or of a part. The leader is
 * asked instead, since a zone node refuses what it has not caught up on, and an older node refuses leader-only reads,
 * with the same `NOT_LEADER` the leader never gives.
 */
export function refused(resp: Response): boolean {
  switch (resp.type) {
    case "DescribeConfigs":
      return resp.error !== ErrorCode.NONE || resp.resources.some((r) => r.error !== ErrorCode.NONE);
    case "OffsetFetch":
      return resp.resources.some((r) => r.partitions.some((p) => p.error !== ErrorCode.NONE));
    case "DescribeNodes":
    case "DescribeUsers":
    case "DescribeTokens":
    case "DescribeCertificates":
    case "DescribeCertificateRequests":
    case "DescribeUpgrade":
    case "DescribeTenants":
    case "DescribeRoleBindings":
      return resp.error !== ErrorCode.NONE;
    default:
      return true;
  }
}

/** The metadata leader a refused admin read names, or `null`. */
export function leaderHintOf(resp: Response): LeaderHint | null {
  switch (resp.type) {
    case "DescribeConfigs":
    case "DescribeNodes":
    case "DescribeUsers":
    case "DescribeTokens":
    case "DescribeCertificates":
    case "DescribeCertificateRequests":
    case "DescribeUpgrade":
    case "DescribeTenants":
    case "DescribeRoleBindings":
      return resp.leaderHint;
    default:
      return null;
  }
}
