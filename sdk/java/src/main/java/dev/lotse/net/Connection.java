package dev.lotse.net;

import dev.lotse.auth.Authenticator;
import dev.lotse.auth.Credentials;
import dev.lotse.proto.ApiKey;
import dev.lotse.proto.DecodeException;
import dev.lotse.proto.Frames;
import dev.lotse.proto.Request;
import dev.lotse.proto.Response;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;

/**
 * One persistent connection to one broker address.
 *
 * <p>A reader thread decodes the frames the broker sends. Requests are pipelined: {@link #send} writes the request
 * and returns a future, and the response completes it whenever it arrives, in any order, matched by correlation id.
 * Correlation ids start at 1 and wrap past 0 (0 marks pushes). Pushes go to the {@link PushListener} registered for
 * their subscription; a push that arrives before its subscription is registered is kept, for at most
 * {@value #ORPHAN_SUBSCRIPTIONS} subscriptions, and delivered on registration.
 *
 * <p>A response with the {@code ERROR} flag fails the future with {@link BrokerException} before any typed
 * decoding. A connection that breaks, is closed, or receives something it cannot decode fails every pending
 * future with {@link ConnectionException}, and cannot be used again. Frames larger than
 * {@link Frames#MAX_FRAME} end the connection.
 *
 * <p>Thread-safe. Writes are serialized; one frame is written whole.
 */
public final class Connection implements AutoCloseable {
    /** Subscriptions with pushes waiting for {@link #register} that are kept; the one with the lowest id is dropped for more. */
    public static final int ORPHAN_SUBSCRIPTIONS = 64;

    private static final System.Logger LOG = System.getLogger("dev.lotse.net");
    private static final int HANDSHAKE_TIMEOUT_MS = 10_000;
    /** Longest lifetime kept, so that arithmetic on it in nanoseconds cannot overflow: about 36 years. */
    private static final long MAX_LIFETIME_MS = 1_000_000_000_000L;

    /** Where a connection's session stands. */
    public enum SessionState {
        /** The session lasts as long as the connection (a client certificate, or no authentication required). */
        UNLIMITED,
        /** Valid, and not due for renewal yet. */
        ACTIVE,
        /** Valid, but past four fifths of its lifetime: authenticate again on this connection. */
        RENEW_DUE,
        /** Over. The node answers {@code UNAUTHORIZED}; open a new connection and authenticate. */
        ENDED
    }

    private record Slot(ApiKey key, CompletableFuture<Response> future) {}

    private record SessionTimes(long renewAt, long endsAt) {}

    private final Address address;
    private final Socket socket;
    private final boolean secure;
    private final InputStream in;
    private final OutputStream out;
    private final ConnectionOptions options;
    private final Ticker ticker;
    private final Object writeLock = new Object();
    private final Object lock = new Object();
    private final Map<Long, Slot> slots = new HashMap<>();
    private final Map<Long, PushListener> subscriptions = new HashMap<>();
    private final TreeMap<Long, List<Frames.DeliverPush>> orphans = new TreeMap<>();
    private long lastId;
    private ConnectionException broken;
    private volatile SessionTimes session;

    private Connection(Address address, Socket socket, boolean secure, ConnectionOptions options) throws IOException {
        this.address = address;
        this.socket = socket;
        this.secure = secure;
        this.in = socket.getInputStream();
        this.out = new BufferedOutputStream(socket.getOutputStream(), 8192);
        this.options = options;
        this.ticker = options.ticker();
    }

    // ---- opening ----

    /**
     * Connects to {@code address} and, if the options carry credentials, authenticates. Blocks.
     *
     * @throws InsecureTransportException if the options name no transport, or plain TCP is asked for an address that is
     *     not loopback; nothing is sent
     * @throws ConnectionException if the broker cannot be reached or the TLS handshake fails (the certificate is not
     *     signed by the given CA, or is not valid for the host)
     * @throws dev.lotse.auth.AuthenticationException if the broker does not accept the credentials
     */
    public static Connection open(Address address, ConnectionOptions options) {
        boolean tls = options.tls() != null;
        if (!tls && !options.plaintextOnLoopback()) {
            throw new InsecureTransportException(
                    "the transport is not configured: give the options TLS (tls), or allow plain TCP on loopback "
                            + "(plaintextOnLoopback)");
        }
        InetAddress[] resolved;
        try {
            resolved = InetAddress.getAllByName(address.host());
        } catch (UnknownHostException e) {
            throw new ConnectionException("cannot resolve " + address.host(), false, e);
        }
        if (!tls) {
            for (InetAddress a : resolved) {
                if (!a.isLoopbackAddress()) {
                    throw new InsecureTransportException(address + " is not a loopback address; plain TCP is only "
                            + "allowed on loopback, configure TLS");
                }
            }
        }
        Connection connection = null;
        IOException last = null;
        for (InetAddress target : resolved) {
            Socket raw = new Socket();
            try {
                raw.setTcpNoDelay(true);
                raw.setKeepAlive(true);
                raw.connect(new InetSocketAddress(target, address.port()), (int) options.connectTimeout().toMillis());
                Socket stream = raw;
                if (tls) {
                    stream = handshake(raw, address, options.tls());
                }
                connection = new Connection(address, stream, tls, options);
                break;
            } catch (IOException e) {
                last = e;
                closeQuietly(raw);
            }
        }
        if (connection == null) {
            throw new ConnectionException("cannot connect to " + address + ": " + describe(last), false, last);
        }
        connection.startReader();
        Credentials credentials = options.credentials();
        if (credentials != null) {
            try {
                connection.authenticate(credentials);
            } catch (ConnectionException e) {
                connection.close();
                // Nothing else went out on this connection.
                throw new ConnectionException(e.getMessage(), false, e);
            } catch (RuntimeException e) {
                connection.close();
                throw e;
            }
        }
        return connection;
    }

    private static String describe(IOException e) {
        return e == null ? "no address" : e.getMessage() != null ? e.getMessage() : e.toString();
    }

    private static SSLSocket handshake(Socket raw, Address address, TlsConfig tls) throws IOException {
        SSLSocket ssl = (SSLSocket) tls.context().getSocketFactory().createSocket(raw, address.host(), address.port(), true);
        SSLParameters params = ssl.getSSLParameters();
        params.setProtocols(new String[] {TlsConfig.PROTOCOL});
        // Checks the certificate against the host dialled: its DNS names, or, for an IP address, its IP entries.
        params.setEndpointIdentificationAlgorithm("HTTPS");
        ssl.setSSLParameters(params);
        ssl.setSoTimeout(HANDSHAKE_TIMEOUT_MS);
        ssl.startHandshake();
        ssl.setSoTimeout(0);
        return ssl;
    }

    private static void closeQuietly(Socket s) {
        try {
            s.close();
        } catch (IOException e) {
            // already gone
        }
    }

    private void startReader() {
        Thread reader = new Thread(this::readLoop, "lotse-reader-" + address);
        reader.setDaemon(true);
        reader.start();
    }

    // ---- state ----

    /** The address this connection is to. */
    public Address address() {
        return address;
    }

    /** Whether the connection is encrypted with TLS. */
    public boolean isSecure() {
        return secure;
    }

    /** Whether the connection is usable: not closed and not broken. */
    public boolean isOpen() {
        synchronized (lock) {
            return broken == null;
        }
    }

    /** Where the session stands, by the options' {@link Ticker}. */
    public SessionState sessionState() {
        SessionTimes s = session;
        if (s == null) {
            return SessionState.UNLIMITED;
        }
        long now = ticker.nanos();
        if (now - s.endsAt() >= 0) {
            return SessionState.ENDED;
        }
        return now - s.renewAt() >= 0 ? SessionState.RENEW_DUE : SessionState.ACTIVE;
    }

    // ---- authentication ----

    /**
     * Authenticates this connection, or authenticates again before the session ends (the node insists on the same
     * principal). Blocks for the round trips. The session lifetime is taken from the last response, and counted from
     * before the first request, so the connection never believes in a session longer than the node does.
     *
     * @throws dev.lotse.auth.AuthenticationException if the node refuses
     * @throws ConnectionException if the connection fails meanwhile
     */
    public void authenticate(Credentials credentials) {
        long start = ticker.nanos();
        SecureRandom random = options.random();
        long lifetimeMs = Authenticator.authenticate(this::call, credentials, random);
        // u64 on the wire: a value with the top bit set is far beyond any real lifetime.
        long clamped = lifetimeMs < 0 || lifetimeMs > MAX_LIFETIME_MS ? MAX_LIFETIME_MS : lifetimeMs;
        if (lifetimeMs == 0) {
            session = null;
        } else {
            long nanos = clamped * 1_000_000L;
            session = new SessionTimes(start + nanos / 5 * 4, start + nanos);
        }
    }

    // ---- requests ----

    /**
     * Sends a request and returns the future of its response. The future completes with {@link BrokerException}
     * if the node refuses the request as a whole, with {@link RequestTimeoutException} if no response comes within
     * the request timeout, and with {@link ConnectionException} if the connection fails first.
     *
     * @throws IllegalArgumentException for {@code Credit}, which is never answered: use {@link #sendOnly}
     */
    public CompletableFuture<Response> send(Request request) {
        if (request.apiKey().equals(ApiKey.CREDIT)) {
            throw new IllegalArgumentException("Credit is never answered; use sendOnly");
        }
        CompletableFuture<Response> future = new CompletableFuture<>();
        long id;
        synchronized (lock) {
            if (broken != null) {
                future.completeExceptionally(notSent(broken));
                return future;
            }
            id = nextId();
        }
        byte[] wire;
        try {
            wire = Frames.withLengthPrefix(Frames.encodeRequest(id, request));
        } catch (RuntimeException e) {
            future.completeExceptionally(new ConnectionException("cannot encode the request: " + e.getMessage(), false, e));
            return future;
        }
        synchronized (lock) {
            if (broken != null) {
                future.completeExceptionally(notSent(broken));
                return future;
            }
            slots.put(id, new Slot(request.apiKey(), future));
        }
        long timeoutNanos = options.requestTimeout().toNanos();
        final long key = id;
        ScheduledFuture<?> timer = Timers.after(timeoutNanos, () -> {
            Slot slot;
            synchronized (lock) {
                slot = slots.remove(key);
            }
            if (slot != null) {
                slot.future().completeExceptionally(new RequestTimeoutException(options.requestTimeout()));
            }
        });
        future.whenComplete((r, e) -> {
            timer.cancel(false);
            synchronized (lock) {
                slots.remove(key);
            }
        });
        try {
            write(wire);
        } catch (IOException e) {
            fail(new ConnectionException("send failed: " + describe(e), true, e));
        }
        return future;
    }

    /**
     * Sends a request nobody waits for: {@code Credit}, which the broker never answers. An answer that comes
     * anyway is dropped.
     *
     * @throws ConnectionException if the connection is broken or closed; the request was not sent
     */
    public void sendOnly(Request request) {
        long id;
        synchronized (lock) {
            if (broken != null) {
                throw notSent(broken);
            }
            id = nextId();
        }
        byte[] wire = Frames.withLengthPrefix(Frames.encodeRequest(id, request));
        try {
            write(wire);
        } catch (IOException e) {
            ConnectionException failure = new ConnectionException("send failed: " + describe(e), true, e);
            fail(failure);
            throw failure;
        }
    }

    /** Sends a request and waits for the response; the blocking form of {@link #send}. */
    public Response call(Request request) {
        return await(send(request));
    }

    /**
     * Sends a request and waits for a response of type {@code type}.
     *
     * @throws ConnectionException if the response is of another type
     */
    public <T extends Response> T call(Request request, Class<T> type) {
        Response r = call(request);
        if (!type.isInstance(r)) {
            throw new ConnectionException("expected " + type.getSimpleName() + " but got " + r.getClass().getSimpleName(), true);
        }
        return type.cast(r);
    }

    /** Asks the node for the API versions it supports. The client does not need the answer to work; it is there to look at. */
    public CompletableFuture<Response.ApiVersions> apiVersions() {
        return send(new Request.ApiVersions()).thenApply(r -> (Response.ApiVersions) r);
    }

    private static Response await(CompletableFuture<Response> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ConnectionException("interrupted while waiting for the response", true, e);
        } catch (ExecutionException | CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException r) {
                throw r;
            }
            throw new ConnectionException("request failed: " + cause, true, cause);
        }
    }

    private long nextId() {
        // u32, never 0.
        lastId = (lastId + 1) & 0xFFFFFFFFL;
        if (lastId == 0) {
            lastId = 1;
        }
        return lastId;
    }

    private static ConnectionException notSent(ConnectionException why) {
        return new ConnectionException(why.getMessage(), false, why);
    }

    private void write(byte[] wire) throws IOException {
        synchronized (writeLock) {
            out.write(wire);
            out.flush();
        }
    }

    // ---- pushes ----

    /**
     * Registers the listener of subscription {@code subscriptionId}. Pushes that arrived earlier are delivered to it
     * first, in order, before this method returns; later ones follow.
     *
     * @throws ConnectionException if the connection is broken or closed
     */
    public void register(long subscriptionId, PushListener listener) {
        synchronized (lock) {
            if (broken != null) {
                throw notSent(broken);
            }
            subscriptions.put(subscriptionId, listener);
            List<Frames.DeliverPush> early = orphans.remove(subscriptionId);
            if (early != null) {
                for (Frames.DeliverPush push : early) {
                    deliver(listener, push);
                }
            }
        }
    }

    /** Stops delivering pushes of the subscription and forgets any that were kept for it. */
    public void unregister(long subscriptionId) {
        synchronized (lock) {
            subscriptions.remove(subscriptionId);
            orphans.remove(subscriptionId);
        }
    }

    private void route(Frames.DeliverPush push) {
        synchronized (lock) {
            PushListener listener = subscriptions.get(push.subscriptionId());
            if (listener != null) {
                deliver(listener, push);
                return;
            }
            if (!orphans.containsKey(push.subscriptionId()) && orphans.size() >= ORPHAN_SUBSCRIPTIONS) {
                orphans.pollFirstEntry();
            }
            orphans.computeIfAbsent(push.subscriptionId(), k -> new ArrayList<>()).add(push);
        }
    }

    private static void deliver(PushListener listener, Frames.DeliverPush push) {
        try {
            listener.onPush(push);
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "a push listener threw", e);
        }
    }

    // ---- reading ----

    private void readLoop() {
        byte[] prefix = new byte[4];
        try {
            while (true) {
                int first = in.read();
                if (first < 0) {
                    fail(new ConnectionException("the broker closed the connection", true));
                    return;
                }
                prefix[0] = (byte) first;
                if (in.readNBytes(prefix, 1, 3) < 3) {
                    throw new EOFException("the broker closed the connection inside a frame");
                }
                int length = Frames.readLength(prefix);
                byte[] frame = in.readNBytes(length);
                if (frame.length < length) {
                    throw new EOFException("the broker closed the connection inside a frame");
                }
                handle(frame);
            }
        } catch (DecodeException e) {
            fail(new ConnectionException("undecodable frame: " + e.getMessage(), true, e));
        } catch (IOException e) {
            fail(new ConnectionException("connection broke: " + describe(e), true, e));
        } catch (RuntimeException e) {
            fail(new ConnectionException("reader failed: " + e, true, e));
        }
    }

    private void handle(byte[] frame) {
        if (Frames.isPush(frame)) {
            route(Frames.decodePush(frame));
            return;
        }
        long id = Frames.responseCorrelationId(frame);
        Slot slot;
        synchronized (lock) {
            slot = slots.remove(id);
        }
        if (slot == null) {
            // Late: the request timed out, or it was sent with sendOnly.
            return;
        }
        Response response;
        try {
            response = Frames.decodeResponse(frame, slot.key()).response();
        } catch (DecodeException e) {
            slot.future().completeExceptionally(new ConnectionException("undecodable response: " + e.getMessage(), true, e));
            throw e;
        }
        if (response instanceof Response.Error error) {
            slot.future().completeExceptionally(new BrokerException(error.code(), null));
        } else if (response instanceof Response.Refused refused) {
            slot.future().completeExceptionally(new BrokerException(refused.code(), refused.message()));
        } else {
            slot.future().complete(response);
        }
    }

    // ---- ending ----

    /** Closes the connection. Every pending request fails with {@link ConnectionException}. */
    @Override
    public void close() {
        fail(new ConnectionException("connection closed", true));
    }

    private void fail(ConnectionException why) {
        List<Slot> pending;
        List<PushListener> listeners;
        synchronized (lock) {
            if (broken != null) {
                return;
            }
            broken = why;
            pending = new ArrayList<>(slots.values());
            slots.clear();
            listeners = new ArrayList<>(subscriptions.values());
            subscriptions.clear();
            orphans.clear();
        }
        closeQuietly(socket);
        for (Slot slot : pending) {
            slot.future().completeExceptionally(why);
        }
        for (PushListener listener : listeners) {
            try {
                listener.onClose(why);
            } catch (RuntimeException e) {
                LOG.log(System.Logger.Level.WARNING, "a push listener threw", e);
            }
        }
    }

    @Override
    public String toString() {
        return "Connection[" + address + (secure ? ", TLS" : ", plain") + (isOpen() ? "" : ", closed") + "]";
    }
}
