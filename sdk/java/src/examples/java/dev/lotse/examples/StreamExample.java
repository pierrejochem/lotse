package dev.lotse.examples;

import dev.lotse.client.ClientException;
import dev.lotse.client.Fetched;
import dev.lotse.client.LotseClient;
import dev.lotse.client.OutMessage;
import dev.lotse.client.ProduceResult;
import dev.lotse.record.BatchRecord;
import java.time.Duration;

/** Creates a stream, writes ten messages to it and reads them back. */
public final class StreamExample {
    private StreamExample() {}

    public static void main(String[] args) {
        try (LotseClient client = new LotseClient(Env.config().build())) {
            try {
                client.createStream("example-orders", 2, 3);
            } catch (ClientException.ResourceExists e) {
                // made by an earlier run
            }
            for (int i = 0; i < 10; i++) {
                // The partition is the caller's to choose; here, by key.
                String customer = "customer-" + (i % 3);
                int partition = Math.floorMod(customer.hashCode(), 2);
                ProduceResult r = client.produce(
                        "example-orders", partition, OutMessage.value("order-" + i).withKey(customer));
                System.out.printf("order-%d at partition %d, offset %s%n", i, partition,
                        Long.toUnsignedString(r.baseOffset()));
            }
            for (int partition = 0; partition < 2; partition++) {
                long offset = 0;
                while (true) {
                    Fetched f = client.fetch("example-orders", partition, offset, Duration.ofMillis(200));
                    for (BatchRecord record : f.committedMessages()) {
                        System.out.printf("read %s from partition %d%n", record.value().toUtf8(), partition);
                    }
                    long next = f.nextOffset(offset);
                    if (next == offset) {
                        break; // nothing more for now
                    }
                    offset = next;
                }
            }
        }
    }
}
