import { LotseClient, ResourceExistsError, u64ToString } from "../src/index.js";
import { connectionOptions, javaHash, run } from "./env.js";

/** Creates a stream, writes ten messages to it and reads them back. */
async function main(): Promise<void> {
  const client = new LotseClient(await connectionOptions());
  try {
    try {
      await client.createStream("example-orders", 2, 3);
    } catch (e) {
      if (!(e instanceof ResourceExistsError)) throw e; // else made by an earlier run
    }
    for (let i = 0; i < 10; i++) {
      // The partition is the caller's to choose; here, by key.
      const customer = `customer-${i % 3}`;
      const partition = ((javaHash(customer) % 2) + 2) % 2;
      const r = await client.produce("example-orders", partition, { key: customer, value: `order-${i}` });
      console.log(`order-${i} at partition ${partition}, offset ${u64ToString(r.baseOffset)}`);
    }
    for (let partition = 0; partition < 2; partition++) {
      let offset = 0n;
      for (;;) {
        const f = await client.fetch("example-orders", partition, offset, 200);
        for (const record of f.committedMessages()) {
          console.log(`read ${Buffer.from(record.value ?? []).toString()} from partition ${partition}`);
        }
        const next = f.nextOffset(offset);
        if (next === offset) break; // nothing more for now
        offset = next;
      }
    }
  } finally {
    client.close();
  }
}

run(main);
