import { setTimeout as sleep } from "node:timers/promises";
import { BrokerResponseError, ClientError, LotseClient, ResourceExistsError } from "../src/index.js";
import { connectionOptions, run } from "./env.js";

/**
 * Consume, transform, produce, exactly once: a group member reads orders, and each batch of invoices it writes
 * commits in one transaction with the group's offsets. A crash between the two leaves neither.
 */
async function main(): Promise<void> {
  // One running client per transactional id: a second one fences the first.
  const client = new LotseClient(await connectionOptions({ transactionalId: "example-billing-1" }));
  try {
    try {
      await client.createStream("example-invoices", 1, 3);
    } catch (e) {
      if (!(e instanceof ResourceExistsError)) throw e; // else made by an earlier run
    }
    // Offsets go with the transaction, not with a timer.
    const group = await client.joinGroup("example-billing", ["example-orders"], { autoCommit: false });
    try {
      const deadline = Date.now() + 20_000;
      while (Date.now() < deadline) {
        const orders = await group.poll(500); // heartbeats too: call it often
        if (orders.length === 0) continue;
        try {
          await using txn = await client.beginTransaction(); // aborts at the end of the scope unless it is ended
          for (const order of orders) {
            await txn.produce("example-invoices", 0, { value: `invoice for ${Buffer.from(order.record.value ?? []).toString()}` });
          }
          await txn.sendOffsets(group);
          await txn.commit();
          console.log(`invoiced ${orders.length} orders`);
        } catch (e) {
          if (!(e instanceof ClientError || e instanceof BrokerResponseError)) throw e;
          // Not committed: read the same orders again from the group's committed offsets.
          console.log(`transaction failed, reading again: ${e.message}`);
          group.rewind();
          await sleep(100);
        }
      }
    } finally {
      await group.close();
    }
  } finally {
    client.close();
  }
}

run(main);
