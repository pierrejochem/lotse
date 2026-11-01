package dev.lotse.net;

import dev.lotse.proto.Frames;

/**
 * Receives the pushes of one subscription on a connection.
 *
 * <p>Both methods run on the connection's reader thread: return quickly, hand the work to another thread, and never
 * wait for a response of the same connection (the reader would be the one to deliver it). Pushes of one
 * subscription arrive in order, also those buffered before {@link Connection#register} was called.
 */
public interface PushListener {
    /** A delivery, or the end of the subscription ({@code error} set). */
    void onPush(Frames.DeliverPush push);

    /** The connection broke or was closed; no more pushes will come. */
    void onClose(ConnectionException reason);
}
