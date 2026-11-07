package dev.lotse.kotlin

import dev.lotse.client.ClientException
import dev.lotse.proto.ErrorCode
import dev.lotse.proto.Messages
import dev.lotse.proto.Request
import dev.lotse.proto.Response
import dev.lotse.record.Batch
import dev.lotse.record.BatchBuilder
import dev.lotse.record.ByteString
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/** What the Kotlin layer adds to the Java core: suspending without blocking, transactions in a block, flows. */
class LotseTest {
    private val offsets = AtomicLong()

    /** A broker that serves stream `s` and transactions; [produce] answers produces. */
    private fun broker(produce: (Request.Produce) -> Response? = { FakeBroker.produced(offsets.getAndIncrement()) }): FakeBroker {
        lateinit var b: FakeBroker
        b = FakeBroker { r ->
            when (r) {
                is Request.Metadata -> FakeBroker.metadata(b.port)
                is Request.FindCoordinator -> Response.FindCoordinator(ErrorCode.NONE, 1, "127.0.0.1", b.port, null)
                is Request.InitProducer -> Response.InitProducer(ErrorCode.NONE, 100, 1, null)
                is Request.AddPartitionsToTxn -> FakeBroker.added(r)
                is Request.Produce -> produce(r)
                is Request.EndTxn -> Response.EndTxn(ErrorCode.NONE, r.producerId(), r.producerEpoch() + 1)
                else -> throw AssertionError("unexpected $r")
            }
        }
        return b
    }

    private fun client(b: FakeBroker): Lotse = lotse(b.hostPort) {
        plaintextOnLoopback()
        transactionalId("t")
        retryFor(Duration.ofMillis(500))
        requestTimeout(Duration.ofSeconds(5))
        backoff(Duration.ofMillis(5))
    }

    private fun ends(b: FakeBroker): List<Boolean> = b.requests<Request.EndTxn>().map { it.commit() }

    @Test
    fun aSuspendedCallLeavesTheCallersThreadFree() = runBlocking {
        // The produce is answered after 300 ms; runBlocking has one thread, which must keep running other coroutines.
        broker { Thread.sleep(300); FakeBroker.produced(0) }.use { b ->
            client(b).use { lotse ->
                lotse.metadata()
                val ticks = AtomicInteger()
                val ticker = launch {
                    while (true) {
                        delay(10)
                        ticks.incrementAndGet()
                    }
                }
                lotse.produce("s", 0, message("a"))
                ticker.cancel()
                assertTrue(ticks.get() >= 10, "the caller's thread ran other work meanwhile: ${ticks.get()} ticks")
            }
        }
    }

    @Test
    fun aTransactionBlockCommitsAndReturnsItsResult() = runBlocking {
        broker().use { b ->
            client(b).use { lotse ->
                val offset = lotse.transaction {
                    produce("s", 0, message("a"))
                    produce("s", 0, message("b")).baseOffset()
                }
                assertEquals(1L, offset)
                assertEquals(listOf(true), ends(b))
                assertTrue(b.requests<Request.Produce>().all { it.txnId() == "t" })
            }
        }
    }

    @Test
    fun aTransactionBlockThatThrowsAborts() = runBlocking {
        broker().use { b ->
            client(b).use { lotse ->
                val e = runCatching {
                    lotse.transaction {
                        produce("s", 0, message("a"))
                        throw IllegalStateException("the work failed")
                    }
                }.exceptionOrNull()
                assertEquals("the work failed", e?.message)
                assertEquals(listOf(false), ends(b))
                // The client is free for the next one.
                lotse.transaction { produce("s", 0, message("b")) }
                assertEquals(listOf(false, true), ends(b))
            }
        }
    }

    @Test
    fun aCommitRefusedAsMustAbortIsAborted() = runBlocking {
        broker { FakeBroker.produced(0, ErrorCode.INVALID_REQUEST) }.use { b ->
            client(b).use { lotse ->
                val e = runCatching {
                    lotse.transaction {
                        // The block swallows the error; the commit then refuses.
                        runCatching { produce("s", 0, message("a")) }
                    }
                }.exceptionOrNull()
                assertTrue(e is ClientException.MustAbort, "got $e")
                assertEquals(listOf(false), ends(b))
            }
        }
    }

    @Test
    fun aCancelledTransactionIsAborted() = runBlocking {
        broker().use { b ->
            client(b).use { lotse ->
                val inside = CompletableDeferred<Unit>()
                val job = launch {
                    lotse.transaction {
                        produce("s", 0, message("a"))
                        inside.complete(Unit)
                        delay(60_000)
                    }
                }
                inside.await()
                job.cancel()
                job.join()
                assertEquals(listOf(false), ends(b))
                lotse.transaction { produce("s", 0, message("b")) }
                assertEquals(listOf(false, true), ends(b))
            }
        }
    }

    @Test
    fun recordsFollowThePartitionFromFetchToFetch() = runBlocking {
        // Each fetch returns two records from the offset asked for.
        lateinit var b: FakeBroker
        b = FakeBroker { r ->
            when (r) {
                is Request.Metadata -> FakeBroker.metadata(b.port)
                is Request.Fetch -> {
                    val from = r.resources()[0].partitions()[0].fetchOffset()
                    val bytes = BatchBuilder().push(1, null, "r$from".toByteArray()).push(1, null, "r${from + 1}".toByteArray())
                        .finish().toByteArray()
                    Batch.stamp(bytes, from, 1)
                    Response.Fetch(
                        0,
                        listOf(
                            Messages.FetchResourceResponse(
                                FakeBroker.STREAM_ID,
                                listOf(
                                    Messages.FetchPartitionResponse(
                                        0, ErrorCode.NONE, 100, 100, 0, listOf(), ByteString.view(bytes, 0, bytes.size), null,
                                    ),
                                ),
                            ),
                        ),
                    )
                }
                else -> throw AssertionError("unexpected $r")
            }
        }
        b.use {
            client(b).use { lotse ->
                val values = lotse.records("s", 0, from = 5).take(5).toList().map { it.value().toUtf8() }
                assertEquals(listOf("r5", "r6", "r7", "r8", "r9"), values)
                assertEquals(listOf(5L, 7L, 9L), b.requests<Request.Fetch>().map { it.resources()[0].partitions()[0].fetchOffset() })
            }
        }
    }

    @Test
    fun buildersGiveTheJavaConfigurations() {
        val g = groupConfig {
            autoCommit(false)
            sessionTimeout(Duration.ofSeconds(10))
        }
        assertFalse(g.autoCommit())
        assertEquals(Duration.ofSeconds(10), g.sessionTimeout())
        val q = queueOptions {
            replication(1)
            deadLetterQueue("dead")
        }
        assertEquals(1, q.replication())
        assertEquals("dead", q.deadLetterQueue())
        val m = message("v", key = "k", headers = mapOf("x-lotse-priority" to "2"))
        assertEquals("k", m.key().toUtf8())
        assertEquals("2", m.headers()[0].value().toUtf8())
    }
}
