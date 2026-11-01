package dev.lotse.client;

import dev.lotse.net.Address;
import dev.lotse.proto.Frames;
import dev.lotse.proto.Request;
import dev.lotse.proto.Response;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A broker in this process, on loopback, that answers as a test tells it to. Plain TCP. Every request is handled on
 * a thread of its own, so a handler that waits does not hold up the requests behind it on the same connection.
 */
final class FakeNode implements AutoCloseable {
    /** What a test does with a request. Return {@code null} to stay silent; throw {@link Drop} to close the connection. */
    interface Handler {
        Response handle(FakeNode node, Request request) throws Exception;
    }

    /** Close the connection without answering. */
    static final class Drop extends RuntimeException {
        private static final long serialVersionUID = 1L;

        Drop() {
            super("drop the connection", null, false, false);
        }
    }

    final long id;
    private final ServerSocket server;
    private final ExecutorService workers = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "fake-node-worker");
        t.setDaemon(true);
        return t;
    });
    private final List<Socket> sockets = new CopyOnWriteArrayList<>();
    private final List<Request> received = new CopyOnWriteArrayList<>();
    private volatile Handler handler = (node, request) -> {
        throw new AssertionError("node " + node.id + " was not expected to get " + request);
    };

    FakeNode(long id) throws IOException {
        this.id = id;
        this.server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        Thread t = new Thread(this::acceptLoop, "fake-node-accept");
        t.setDaemon(true);
        t.start();
    }

    FakeNode on(Handler handler) {
        this.handler = handler;
        return this;
    }

    Address address() {
        return new Address("127.0.0.1", server.getLocalPort());
    }

    String hostPort() {
        return address().toString();
    }

    /** Every request this node received, in arrival order. */
    List<Request> requests() {
        return new ArrayList<>(received);
    }

    /** The requests of one type, in arrival order. */
    <T extends Request> List<T> requests(Class<T> type) {
        List<T> out = new ArrayList<>();
        for (Request r : received) {
            if (type.isInstance(r)) {
                out.add(type.cast(r));
            }
        }
        return out;
    }

    int count(Class<? extends Request> type) {
        return requests(type).size();
    }

    private void acceptLoop() {
        while (!server.isClosed()) {
            try {
                Socket s = server.accept();
                sockets.add(s);
                Thread t = new Thread(() -> serve(s), "fake-node-peer");
                t.setDaemon(true);
                t.start();
            } catch (IOException e) {
                return;
            }
        }
    }

    private void serve(Socket socket) {
        try (DataInputStream in = new DataInputStream(socket.getInputStream())) {
            while (true) {
                int length;
                try {
                    length = in.readInt();
                } catch (EOFException e) {
                    return;
                }
                byte[] frame = new byte[length];
                in.readFully(frame);
                Frames.DecodedRequest decoded = Frames.decodeRequest(frame);
                received.add(decoded.request());
                workers.execute(() -> answer(socket, decoded));
            }
        } catch (Exception e) {
            // the peer or the test closed the socket
        }
    }

    private void answer(Socket socket, Frames.DecodedRequest decoded) {
        try {
            Response response = handler.handle(this, decoded.request());
            if (response == null) {
                return;
            }
            byte[] wire = Frames.withLengthPrefix(Frames.encodeResponse(decoded.header().correlationId(), response));
            synchronized (socket) {
                OutputStream out = socket.getOutputStream();
                out.write(wire);
                out.flush();
            }
        } catch (Drop d) {
            closeQuietly(socket);
        } catch (AssertionError e) {
            e.printStackTrace();
            closeQuietly(socket);
        } catch (Exception e) {
            closeQuietly(socket);
        }
    }

    private static void closeQuietly(Socket s) {
        try {
            s.close();
        } catch (IOException e) {
            // closing
        }
    }

    /** Stops the node: it no longer accepts, and its connections are closed. */
    void kill() {
        try {
            server.close();
        } catch (IOException e) {
            // closing
        }
        for (Socket s : sockets) {
            closeQuietly(s);
        }
    }

    @Override
    public void close() {
        kill();
        workers.shutdownNow();
    }
}
