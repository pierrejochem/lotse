package dev.lotse.net;

import dev.lotse.proto.Frames;
import dev.lotse.proto.Response;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/** A broker in this process, on loopback, that answers as a test tells it to. Plain TCP. */
final class FakeBroker implements AutoCloseable {
    /** What a test does with each request. */
    interface Handler {
        void onRequest(Peer peer, Frames.DecodedRequest request) throws Exception;
    }

    /** One accepted connection. */
    static final class Peer {
        private final Socket socket;
        final List<Frames.DecodedRequest> received = new CopyOnWriteArrayList<>();

        Peer(Socket socket) {
            this.socket = socket;
        }

        synchronized void reply(long correlationId, Response response) throws IOException {
            raw(Frames.encodeResponse(correlationId, response));
        }

        synchronized void push(Frames.DeliverPush push) throws IOException {
            raw(Frames.encodePush(push));
        }

        synchronized void raw(byte[] frame) throws IOException {
            OutputStream out = socket.getOutputStream();
            out.write(Frames.withLengthPrefix(frame));
            out.flush();
        }

        synchronized void rawBytes(byte[] bytes) throws IOException {
            OutputStream out = socket.getOutputStream();
            out.write(bytes);
            out.flush();
        }

        void closeSocket() throws IOException {
            socket.close();
        }
    }

    private final ServerSocket server;
    private final Handler handler;
    final AtomicInteger accepted = new AtomicInteger();
    final List<Peer> peers = new CopyOnWriteArrayList<>();

    FakeBroker(Handler handler) throws IOException {
        this.server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        this.handler = handler;
        Thread t = new Thread(this::acceptLoop, "fake-broker-accept");
        t.setDaemon(true);
        t.start();
    }

    Address address() {
        return new Address("127.0.0.1", server.getLocalPort());
    }

    private void acceptLoop() {
        while (!server.isClosed()) {
            try {
                Socket s = server.accept();
                accepted.incrementAndGet();
                Peer peer = new Peer(s);
                peers.add(peer);
                Thread t = new Thread(() -> serve(peer), "fake-broker-peer");
                t.setDaemon(true);
                t.start();
            } catch (IOException e) {
                return;
            }
        }
    }

    private void serve(Peer peer) {
        try (DataInputStream in = new DataInputStream(peer.socket.getInputStream())) {
            while (true) {
                int length;
                try {
                    length = in.readInt();
                } catch (EOFException e) {
                    return;
                }
                byte[] frame = new byte[length];
                in.readFully(frame);
                Frames.DecodedRequest request = Frames.decodeRequest(frame);
                peer.received.add(request);
                handler.onRequest(peer, request);
            }
        } catch (Exception e) {
            // the peer or the test closed the socket
        }
    }

    @Override
    public void close() throws IOException {
        server.close();
        for (Peer p : peers) {
            p.closeSocket();
        }
    }
}
