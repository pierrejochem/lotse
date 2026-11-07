package dev.lotse.examples

import dev.lotse.client.ClientException
import dev.lotse.client.GroupRecord
import dev.lotse.kotlin.groupConfig
import dev.lotse.kotlin.lotse
import dev.lotse.kotlin.message
import java.time.Duration
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull

/**
 * Writes orders, then invoices them exactly once from a consumer group, each poll's orders in one transaction with
 * the group's offsets. Brokers from `LOTSE_BOOTSTRAP` (default `127.0.0.1:9092`), plain TCP on loopback.
 */
fun main(): Unit = runBlocking {
    val bootstrap = (System.getenv("LOTSE_BOOTSTRAP") ?: "127.0.0.1:9092").split(",").toTypedArray()
    lotse(*bootstrap) {
        plaintextOnLoopback()
        transactionalId("example-billing-kt")
    }.use { lotse ->
        for (stream in listOf("example-orders-kt", "example-invoices-kt")) {
            try {
                lotse.createStream(stream, partitions = 1, replication = 3)
            } catch (e: ClientException.ResourceExists) {
                // made by an earlier run
            }
        }
        repeat(10) { lotse.produce("example-orders-kt", 0, message("order-$it", key = "customer-${it % 3}")) }

        lotse.joinGroup("example-billing-kt", listOf("example-orders-kt"), groupConfig { autoCommit(false) }).use { group ->
            withTimeoutOrNull(Duration.ofSeconds(20).toMillis()) {
                var invoiced = 0
                while (invoiced < 10) {
                    val orders: List<GroupRecord> = group.poll(Duration.ofMillis(500))
                    if (orders.isEmpty()) continue
                    try {
                        lotse.transaction {
                            for (order in orders) {
                                produce("example-invoices-kt", 0, message("invoice for ${order.record().value().toUtf8()}"))
                            }
                            sendOffsets(group)
                        }
                        invoiced += orders.size
                        println("invoiced $invoiced orders")
                    } catch (e: ClientException) {
                        group.rewind() // not committed: read them again
                    }
                }
            }
        }

        lotse.records("example-invoices-kt", 0, from = 0).take(10).collect { println(it.value().toUtf8()) }
    }
}
