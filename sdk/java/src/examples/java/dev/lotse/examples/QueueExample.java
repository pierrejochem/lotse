package dev.lotse.examples;

import dev.lotse.client.ClientException;
import dev.lotse.client.DeadLetter;
import dev.lotse.client.Delivery;
import dev.lotse.client.LotseClient;
import dev.lotse.client.OutMessage;
import dev.lotse.client.QueueConsumer;
import dev.lotse.client.QueueOptions;
import dev.lotse.client.SubscribeOptions;
import java.time.Duration;

/**
 * A work queue with a dead-letter queue: jobs that fail are tried again after a delay, and a job that fails three
 * times moves to the dead-letter queue.
 */
public final class QueueExample {
    private QueueExample() {}

    public static void main(String[] args) {
        try (LotseClient client = new LotseClient(Env.config().build())) {
            createIfMissing(client, "example-jobs-dead", QueueOptions.defaults());
            createIfMissing(client, "example-jobs", QueueOptions.builder()
                    .maxDeliveries(3)
                    .deadLetterQueue("example-jobs-dead")
                    .ackTimeout(Duration.ofSeconds(30))
                    .build());
            for (int i = 0; i < 5; i++) {
                client.produce("example-jobs", 0, OutMessage.value("job-" + i));
            }
            try (QueueConsumer jobs = client.subscribe("example-jobs", SubscribeOptions.withCredit(16))) {
                while (true) {
                    Delivery job = jobs.poll(Duration.ofSeconds(5));
                    if (job == null) {
                        break; // nothing for 5 s: done
                    }
                    String name = job.value().toUtf8();
                    if (name.equals("job-3")) {
                        // Fails every time: back in one second, until max.deliveries retires it.
                        System.out.printf("%s failed (delivery %d)%n", name, job.deliveryCount());
                        jobs.nackAfter(job, Duration.ofSeconds(1));
                        continue;
                    }
                    System.out.println("done " + name);
                    jobs.ack(job);
                }
            }
            // The retired job, with where it came from and why.
            try (QueueConsumer dead = client.subscribe("example-jobs-dead")) {
                Delivery job = dead.poll(Duration.ofSeconds(5));
                if (job != null) {
                    System.out.printf("dead letter %s: %s after %d deliveries%n", job.value().toUtf8(),
                            job.deadLetter().map(DeadLetter::reason).orElse("?"),
                            job.deadLetter().map(DeadLetter::deliveries).orElse(0));
                    dead.ack(job);
                }
            }
        }
    }

    private static void createIfMissing(LotseClient client, String name, QueueOptions options) {
        try {
            client.createQueue(name, options);
        } catch (ClientException.ResourceExists e) {
            // made by an earlier run
        }
    }
}
