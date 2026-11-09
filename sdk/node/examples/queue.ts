import { LotseClient, type QueueOptions, ResourceExistsError } from "../src/index.js";
import { connectionOptions, run } from "./env.js";

/**
 * A work queue with a dead-letter queue: jobs that fail are tried again after a delay, and a job that fails three
 * times moves to the dead-letter queue.
 */
async function main(): Promise<void> {
  const client = new LotseClient(await connectionOptions());
  try {
    await createIfMissing(client, "example-jobs-dead", {});
    await createIfMissing(client, "example-jobs", { maxDeliveries: 3, deadLetterQueue: "example-jobs-dead", ackTimeoutMs: 30_000 });
    for (let i = 0; i < 5; i++) {
      await client.produce("example-jobs", 0, { value: `job-${i}` });
    }
    const jobs = await client.subscribe("example-jobs", { credit: 16 });
    try {
      for (;;) {
        const job = await jobs.poll(5_000);
        if (job === null) break; // nothing for 5 s: done
        const name = job.text();
        if (name === "job-3") {
          // Fails every time: back in one second, until max.deliveries retires it.
          console.log(`${name} failed (delivery ${job.deliveryCount})`);
          await jobs.nackAfter(job, 1_000);
          continue;
        }
        console.log(`done ${name}`);
        await jobs.ack(job);
      }
    } finally {
      jobs.close();
    }
    // The retired job, with where it came from and why.
    const dead = await client.subscribe("example-jobs-dead");
    try {
      const job = await dead.poll(5_000);
      if (job !== null) {
        const letter = job.deadLetter();
        console.log(`dead letter ${job.text()}: ${letter?.reason ?? "?"} after ${letter?.deliveries ?? 0} deliveries`);
        await dead.ack(job);
      }
    } finally {
      dead.close();
    }
  } finally {
    client.close();
  }
}

async function createIfMissing(client: LotseClient, name: string, options: QueueOptions): Promise<void> {
  try {
    await client.createQueue(name, options);
  } catch (e) {
    if (!(e instanceof ResourceExistsError)) throw e; // else made by an earlier run
  }
}

run(main);
