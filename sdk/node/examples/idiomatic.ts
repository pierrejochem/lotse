import { LotseClient, ResourceExistsError } from "../src/index.js";
import { connectionOptions, run } from "./env.js";

/**
 * The Node.js shape of the client: `for await` over a stream's records and over a queue's deliveries, and
 * `await using` for a transaction that aborts unless it is committed. Needs nothing from the other examples.
 */
async function main(): Promise<void> {
  const client = new LotseClient(await connectionOptions({ transactionalId: "example-node-1" }));
  const text = (b: Uint8Array | null): string => Buffer.from(b ?? []).toString();
  try {
    for (const create of [() => client.createStream("example-node-ledger", 1, 3), () => client.createQueue("example-node-jobs", {})]) {
      try {
        await create();
      } catch (e) {
        if (!(e instanceof ResourceExistsError)) throw e; // else made by an earlier run
      }
    }

    // A transaction: the stream record and the queue message commit together, or neither exists.
    {
      await using txn = await client.beginTransaction();
      await txn.produce("example-node-ledger", 0, { key: "account-7", value: "debit 40" });
      await txn.produce("example-node-jobs", 0, { value: "send receipt for account-7" });
      await txn.commit();
      console.log("committed");
    }
    {
      await using txn = await client.beginTransaction();
      await txn.produce("example-node-ledger", 0, { key: "account-7", value: "never seen" });
      console.log("leaving the scope without a commit");
    } // aborted here

    // A stream: committed records only, to the end for now.
    for await (const record of client.readRecords("example-node-ledger", 0, { from: 0n, untilEnd: true })) {
      console.log(`ledger ${record.offset}: ${text(record.value)}`);
    }

    // A queue: the iterator waits for messages without end, so the loop leaves with `break` once it has what it came for.
    const jobs = await client.subscribe("example-node-jobs", { credit: 8 });
    try {
      for await (const job of jobs) {
        console.log(`job ${job.text() ?? ""}`);
        await jobs.ack(job);
        break;
      }
    } finally {
      jobs.close();
    }
  } finally {
    client.close();
  }
}

run(main);
