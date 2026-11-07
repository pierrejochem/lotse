package dev.lotse.examples;

import dev.lotse.client.ClientException;
import dev.lotse.client.GroupConfig;
import dev.lotse.client.GroupConsumer;
import dev.lotse.client.GroupRecord;
import dev.lotse.client.LotseClient;
import dev.lotse.client.OutMessage;
import dev.lotse.client.Transaction;
import java.time.Duration;
import java.util.List;

/**
 * Consume, transform, produce, exactly once: a group member reads orders, and each batch of invoices it writes
 * commits in one transaction with the group's offsets. A crash between the two leaves neither.
 */
public final class ExactlyOnceExample {
    private ExactlyOnceExample() {}

    public static void main(String[] args) {
        // One running client per transactional id: a second one fences the first.
        try (LotseClient client = new LotseClient(Env.config().transactionalId("example-billing-1").build())) {
            try {
                client.createStream("example-invoices", 1, 3);
            } catch (ClientException.ResourceExists e) {
                // made by an earlier run
            }
            GroupConfig config = GroupConfig.builder().autoCommit(false).build(); // offsets go with the transaction
            try (GroupConsumer group = client.joinGroup("example-billing", List.of("example-orders"), config)) {
                long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
                while (System.nanoTime() < deadline) {
                    List<GroupRecord> orders = group.poll(Duration.ofMillis(500)); // heartbeats too: call it often
                    if (orders.isEmpty()) {
                        continue;
                    }
                    try (Transaction txn = client.beginTransaction()) {
                        for (GroupRecord order : orders) {
                            txn.produce("example-invoices", 0,
                                    OutMessage.value("invoice for " + order.record().value().toUtf8()));
                        }
                        txn.sendOffsets(group);
                        txn.commit();
                        System.out.printf("invoiced %d orders%n", orders.size());
                    } catch (ClientException e) {
                        // Not committed: read the same orders again from the group's committed offsets.
                        System.out.println("transaction failed, reading again: " + e.getMessage());
                        group.rewind();
                    }
                }
            }
        }
    }
}
