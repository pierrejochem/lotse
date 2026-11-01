package dev.lotse.net;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * One connection per broker address, authenticated, kept for reuse.
 *
 * <p>{@link #get} returns the live connection to an address, opening it on first use. A connection whose session
 * is due for renewal (four fifths of its lifetime) authenticates again on the same connection, as the same
 * principal; one caller renews while the others go on with the session, which still holds. A connection whose
 * session has ended, or that broke, is replaced by a new one with a fresh login. A connection that failed once
 * should be handed to {@link #discard}: it is not trusted again.
 *
 * <p>Thread-safe. {@link #get} blocks while connecting or renewing.
 */
public final class ConnectionPool implements AutoCloseable {
    private final ConnectionOptions options;
    private final Map<Address, Connection> connections = new ConcurrentHashMap<>();
    private final Map<Connection, AtomicBoolean> renewing = new ConcurrentHashMap<>();
    private volatile boolean closed;

    /** Creates a pool whose connections are made with {@code options}. */
    public ConnectionPool(ConnectionOptions options) {
        this.options = options;
    }

    /** The connection to {@code address}; see the class description for when it is opened, renewed or replaced. */
    public Connection get(Address address) {
        if (closed) {
            throw new ConnectionException("the pool is closed", false);
        }
        Connection cached = connections.get(address);
        if (cached != null) {
            if (!cached.isOpen()) {
                discard(cached);
            } else {
                switch (cached.sessionState()) {
                    case UNLIMITED, ACTIVE -> {
                        return cached;
                    }
                    case ENDED -> discard(cached); // requests would be refused: start over
                    case RENEW_DUE -> {
                        AtomicBoolean flag = renewing.computeIfAbsent(cached, c -> new AtomicBoolean());
                        if (flag.getAndSet(true)) {
                            return cached;
                        }
                        try {
                            cached.authenticate(options.credentials());
                            return cached;
                        } catch (RuntimeException e) {
                            discard(cached);
                            if (e instanceof dev.lotse.auth.AuthenticationException) {
                                throw e;
                            }
                            // a failure of the connection itself: log in on a new one below
                        } finally {
                            flag.set(false);
                        }
                    }
                    default -> throw new IllegalStateException();
                }
            }
        }
        Connection fresh = Connection.open(address, options);
        Connection winner = connections.putIfAbsent(address, fresh);
        if (winner != null && winner != fresh) {
            fresh.close();
            return winner;
        }
        return fresh;
    }

    /** Forgets and closes {@code connection}, if the pool still holds it. */
    public void discard(Connection connection) {
        connections.remove(connection.address(), connection);
        renewing.remove(connection);
        connection.close();
    }

    /** Closes every connection. */
    @Override
    public void close() {
        closed = true;
        for (Connection c : connections.values()) {
            c.close();
        }
        connections.clear();
        renewing.clear();
    }
}
