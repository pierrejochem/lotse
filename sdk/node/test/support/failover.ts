import { setTimeout as sleep } from "node:timers/promises";
import { BrokerResponseError, ErrorCode, type LotseClient } from "../../src/index.js";

/**
 * What the real-node tests do about leadership moving. A new group's leader is not settled in its first moments (the
 * node that wins the first election hands leadership to the group's preferred node, and a node that is starved of CPU
 * can lose an election later), and a deposed leader goes on saying it leads until it hears of the new term. Neither is a
 * fault: a client or an application meets both, so a test that depends on the leader staying where it is either says
 * so or does not assert what a move may change.
 */

/** Whether `e` is the broker saying this node does not lead the partition (any more). */
export const isNotLeader = (e: unknown): boolean => e instanceof BrokerResponseError && e.code === ErrorCode.NOT_LEADER;

/** Who leads partition `partition` of `name`, and since which term: `"leader@term"`, or `"none"` while there is none. */
export async function leaderTerm(client: LotseClient, name: string, partition = 0): Promise<string> {
  const m = await client.metadata(name);
  const p = m.resources.find((r) => r.name === name)?.partitions.find((x) => x.partition === partition);
  return p === undefined || p.leader === 0 ? "none" : `${p.leader}@${p.leaderTerm}`;
}

/** The node that leads partition `partition` of `name` now, or 0. */
export async function leaderOf(client: LotseClient, name: string, partition = 0): Promise<number> {
  const m = await client.metadata(name);
  return m.resources.find((r) => r.name === name)?.partitions.find((x) => x.partition === partition)?.leader ?? 0;
}

/**
 * Asks `read` until it gives the same answer twice in a row, and returns it: something a new leader appends (a note of
 * its own term) can fall between two reads, and the end of a log is only the end once nothing is appended meanwhile.
 */
export async function stable<T>(read: () => Promise<T>, attempts = 20): Promise<T> {
  let last = await read();
  for (let i = 0; i < attempts; i++) {
    const now = await read();
    if (now === last) {
      return now;
    }
    last = now;
    await sleep(50);
  }
  return last;
}

/** What a scenario watches: the leaders of the partitions it relies on, as they were when it began to rely on them. */
export class Steady {
  private readonly seen = new Map<string, string>();
  constructor(private readonly client: LotseClient) {}

  /** Starts watching `name`'s first partition: call it once the queue or stream has a leader the scenario relies on. */
  async watch(name: string): Promise<void> {
    this.seen.set(name, await leaderTerm(this.client, name));
  }

  /** Whether any watched partition has another leader or term than when it was first watched. */
  async moved(): Promise<boolean> {
    for (const [name, was] of this.seen) {
      if ((await leaderTerm(this.client, name)) !== was) {
        return true;
      }
    }
    return false;
  }
}

/**
 * Runs a scenario that relies on its leaders staying where they are, and runs it again, with new names (the body gets
 * the attempt number to add to them), if it fails after one of them moved. A failure with every leader where it was is
 * a failure: it is thrown at once, so the check stays as strict as it was.
 */
export async function retryIfLeaderMoved<T>(
  client: LotseClient,
  body: (steady: Steady, attempt: number) => Promise<T>,
  attempts = 4,
): Promise<T> {
  for (let attempt = 1; ; attempt++) {
    const steady = new Steady(client);
    try {
      return await body(steady, attempt);
    } catch (e) {
      if (attempt >= attempts || !((await steady.moved()) || isNotLeader(e))) {
        throw e;
      }
    }
  }
}
