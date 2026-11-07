package dev.lotse.kotlin

import dev.lotse.proto.ErrorCode
import dev.lotse.proto.Frames
import dev.lotse.proto.Messages
import dev.lotse.proto.Request
import dev.lotse.proto.Response
import java.io.DataInputStream
import java.io.EOFException
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.concurrent.thread

/**
 * One scripted node on loopback: answers each request with [answer], or not at all for `null`. It is metadata leader,
 * coordinator and the leader of everything, as node 1.
 */
internal class FakeBroker(private val answer: (Request) -> Response?) : AutoCloseable {
    private val server = ServerSocket(0, 50, InetAddress.getLoopbackAddress())
    private val sockets = CopyOnWriteArrayList<Socket>()

    /** Every request received, in order. */
    val received = CopyOnWriteArrayList<Request>()

    val port: Int
        get() = server.localPort

    val hostPort: String
        get() = "127.0.0.1:$port"

    init {
        thread(isDaemon = true, name = "fake-broker-accept") {
            while (!server.isClosed) {
                val s = try {
                    server.accept()
                } catch (e: Exception) {
                    break
                }
                sockets += s
                thread(isDaemon = true, name = "fake-broker-peer") { serve(s) }
            }
        }
    }

    private fun serve(socket: Socket) {
        try {
            DataInputStream(socket.getInputStream()).use { input ->
                while (true) {
                    val length = try {
                        input.readInt()
                    } catch (e: EOFException) {
                        return
                    }
                    val frame = ByteArray(length)
                    input.readFully(frame)
                    val decoded = Frames.decodeRequest(frame)
                    received += decoded.request()
                    thread(isDaemon = true) {
                        val response = answer(decoded.request()) ?: return@thread
                        val wire = Frames.withLengthPrefix(Frames.encodeResponse(decoded.header().correlationId(), response))
                        synchronized(socket) {
                            socket.getOutputStream().write(wire)
                            socket.getOutputStream().flush()
                        }
                    }
                }
            }
        } catch (e: Exception) {
            // closed
        }
    }

    inline fun <reified T : Request> requests(): List<T> = received.filterIsInstance<T>()

    override fun close() {
        server.close()
        sockets.forEach { runCatching { it.close() } }
    }

    companion object {
        const val STREAM_ID = 7L

        fun metadata(port: Int): Response.Metadata = Response.Metadata(
            listOf(Messages.Broker(1, "127.0.0.1", port, "")),
            listOf(
                Messages.ResourceMetadata(
                    ErrorCode.NONE, "s", "default", STREAM_ID, Messages.ResourceKind.STREAM,
                    listOf(Messages.PartitionMetadata(0, 1, 1, listOf(1L))), null,
                ),
            ),
        )

        fun added(r: Request.AddPartitionsToTxn): Response = Response.AddPartitionsToTxn(
            r.resources().map { res ->
                Messages.TxnResourceResult(res.resourceId(), res.partitions().map { Messages.TxnPartitionResult(it, ErrorCode.NONE) })
            },
        )

        fun produced(offset: Long, code: ErrorCode = ErrorCode.NONE): Response = Response.Produce(
            0,
            listOf(
                Messages.ProduceResourceResponse(
                    STREAM_ID, listOf(Messages.ProducePartitionResponse(0, code, offset, -1, null, null)),
                ),
            ),
        )
    }
}
