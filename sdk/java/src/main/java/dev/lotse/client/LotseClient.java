package dev.lotse.client;

import dev.lotse.auth.AccessToken;
import dev.lotse.auth.AuthenticationException;
import dev.lotse.auth.ScramCredential;
import dev.lotse.auth.TokenLifetime;
import dev.lotse.net.Address;
import dev.lotse.net.BrokerException;
import dev.lotse.net.Connection;
import dev.lotse.net.ConnectionException;
import dev.lotse.net.ConnectionPool;
import dev.lotse.net.InsecureTransportException;
import dev.lotse.proto.ErrorCode;
import dev.lotse.proto.GroupIndex;
import dev.lotse.proto.LeaderHint;
import dev.lotse.proto.Messages;
import dev.lotse.proto.Messages.Config;
import dev.lotse.proto.Messages.FetchPartition;
import dev.lotse.proto.Messages.FetchPartitionResponse;
import dev.lotse.proto.Messages.FetchResource;
import dev.lotse.proto.Messages.HeaderMatch;
import dev.lotse.proto.Messages.PartitionMetadata;
import dev.lotse.proto.Messages.ProducePartition;
import dev.lotse.proto.Messages.ProducePartitionResponse;
import dev.lotse.proto.Messages.ProduceResource;
import dev.lotse.proto.Messages.ResourceMetadata;
import dev.lotse.proto.Request;
import dev.lotse.proto.Response;
import dev.lotse.record.Batch;
import dev.lotse.record.BatchBuilder;
import dev.lotse.record.ByteString;
import dev.lotse.record.Header;
import dev.lotse.record.RecordFormatException;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.WeakHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * A client for Lotse streams and queues, written to behave like the Rust client ({@code lotse-client}).
 *
 * <p>It looks up streams with {@code Metadata}, sends {@code Produce} and {@code Fetch} straight to each partition's
 * leader, and follows {@code NOT_LEADER} hints and metadata changes on its own. It connects on first use, keeps one
 * connection per broker address, and is safe to share between threads.
 *
 * <p><b>Produce is idempotent by default</b> ({@link ClientConfig.Builder#idempotent}). The client gets a producer
 * id from the coordinator ({@code FindCoordinator}, {@code InitProducer}), stamps every batch with it and a sequence
 * number per partition, and retries a batch whose outcome is unknown (the connection broke after sending, or
 * {@code REQUEST_TIMED_OUT}) with the <em>same</em> sequence; the leader recognises a batch it already has and
 * answers with its offset. Produces to one partition go out one at a time, so sequences arrive in order. Only when
 * retrying runs out of time with the outcome still unknown does the caller get {@link ClientException.Unknown}; the
 * client then starts over with a new producer id, so later batches do not wait for a gap that may never fill.
 * Without idempotence an unknown outcome is never retried, since a retry could write the batch twice.
 *
 * <p>Every call exists twice: blocking, and as a {@code ...Async} method that returns a {@link CompletableFuture}
 * and runs the blocking call on a thread of the client's own. Two async produces to the same partition run one after
 * the other, but in no promised order: await the first before starting the second if the order matters.
 *
 * <p>An {@link InsecureTransportException} (no TLS configured, or plain TCP to an address that is not loopback) is
 * a mistake in the configuration and is thrown as it is, not retried.
 */
public final class LotseClient implements AutoCloseable {
    private static final long U32_MAX = 0xFFFFFFFFL;

    /** One partition's place in the cluster, as metadata last said. */
    record Route(long resourceId, long leader, long term) {}

    /** A route and the leader's address. */
    record Routed(Route route, Address address) {}

    private record PartitionKey(String stream, int partition) {}

    /** A partition of one incarnation of a stream: sequences belong to it (ADR 0107). */
    private record IncarnationKey(long resourceId, int partition) {}

    record ProducerIds(long producerId, int epoch) {}

    /** The producer id this client writes with. */
    private static final class Session {
        final long producerId;
        final int epoch;
        /** By resource id: a stream deleted and created again under its name has sequences of its own. */
        final Map<IncarnationKey, Integer> next = new HashMap<>();

        Session(long producerId, int epoch) {
            this.producerId = producerId;
            this.epoch = epoch;
        }
    }

    /** What a produce is stamped with. */
    private record Stamp(long producerId, int epoch, int sequence) {}

    private final ClientConfig cfg;
    private final ConnectionPool pool;
    private final ExecutorService executor;
    private volatile boolean closed;

    /** Guards {@link #brokers}, {@link #routes} and {@link #names}. */
    private final Object state = new Object();

    private final Map<Long, Address> brokers = new TreeMap<>();
    private final Map<PartitionKey, Route> routes = new HashMap<>();
    /** Each node's zone and each partition's voters, for fetching in this client's zone (ADR 0104). */
    private final Map<Long, String> zones = new TreeMap<>();
    private final Map<PartitionKey, List<Long>> voters = new HashMap<>();
    /** Partitions whose replica in the zone failed: read from the leader until metadata is asked for again. */
    private final java.util.Set<PartitionKey> notInZone = new java.util.HashSet<>();
    /**
     * The nodes the latest {@code Metadata} answer listed: the only candidates for a zone read. The maps above only
     * grow, and a node that left would otherwise cost every zone read a failed call first.
     */
    private final java.util.Set<Long> listed = new TreeSet<>();
    /** The replica in this client's zone a partition's leader named as current, until metadata is asked for again. */
    private final Map<PartitionKey, Long> zoneReplica = new HashMap<>();
    /** The metadata and coordinator groups' indexes the answers named, which a zone read asks a node to have applied. */
    private final Zone.Indexes indexes = new Zone.Indexes();
    private final Map<Long, String> names = new HashMap<>();
    /** Exchanges by name, and when metadata said so ({@link System#nanoTime}). Guarded by {@link #state}. */
    private final Map<String, Exchange> exchanges = new HashMap<>();
    private final Map<String, Long> exchangesAt = new HashMap<>();

    /** One transaction at a time; the producer id and epoch for the next one, {@code null} to ask anew. */
    private final Semaphore txnTurn = new Semaphore(1, true);
    private final AtomicReference<ProducerIds> txnProducer = new AtomicReference<>();

    private final Object sessionLock = new Object();
    private Session session;
    private volatile Address coordinatorAddress;
    /** The coordinator group's voters, as the last {@code FindCoordinator} named them: where a zone offset read may go. */
    private volatile List<Long> coordinatorVoters = List.of();
    private final Map<PartitionKey, ReentrantLock> partitionLocks = new ConcurrentHashMap<>();

    private volatile Long metadataLeader;
    private final AtomicInteger adminTurn = new AtomicInteger();
    private final AtomicReference<ClientException> fatal = new AtomicReference<>();
    private final Set<Connection> seen = Collections.synchronizedSet(Collections.newSetFromMap(new WeakHashMap<>()));

    /** Creates a client. It connects on first use. */
    public LotseClient(ClientConfig config) {
        this.cfg = config;
        this.pool = new ConnectionPool(config.connectionOptions(indexes::raise));
        this.executor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "lotse-client");
            t.setDaemon(true);
            return t;
        });
        if (config.renewCertificateFiles() != null) {
            executor.execute(this::keepRenewed);
        }
    }

    /** Closes every connection and stops the client's threads. Calls in progress fail. */
    @Override
    public void close() {
        closed = true;
        pool.close();
        executor.shutdownNow();
    }

    // ---- metadata ----

    /**
     * Asks any reachable broker for metadata and remembers the leaders.
     *
     * @param names the resources to ask for; none asks for all
     * @throws ClientException.GaveUp if no broker answers within {@link ClientConfig#retryFor()}
     * @throws ClientException.Authentication if a broker does not accept the credentials
     */
    public Response.Metadata metadata(String... names) {
        List<String> asked = List.of(names);
        long start = System.nanoTime();
        String why = "no broker known";
        while (true) {
            for (Address addr : candidates()) {
                try {
                    Response r = call(addr, new Request.Metadata(asked));
                    if (r instanceof Response.Metadata m) {
                        learn(m);
                        return m;
                    }
                    why = "unexpected response " + r;
                } catch (CallFailure e) {
                    if (e.kind == CallFailure.Kind.DENIED) {
                        throw new ClientException.Authentication(e.getMessage());
                    }
                    why = addr + ": " + e.describe();
                }
            }
            if (elapsed(start).compareTo(cfg.retryFor()) >= 0) {
                throw new ClientException.GaveUp(cfg.retryFor(), why);
            }
            if (!pause()) {
                throw new ClientException.GaveUp(elapsed(start), "interrupted");
            }
        }
    }

    /** {@link #metadata} as a future. */
    public CompletableFuture<Response.Metadata> metadataAsync(String... names) {
        return CompletableFuture.supplyAsync(() -> metadata(names), executor);
    }

    /** Brokers known so far, then the bootstrap list, without repeats. */
    private List<Address> candidates() {
        Set<Address> out = new LinkedHashSet<>();
        synchronized (state) {
            out.addAll(brokers.values());
        }
        out.addAll(cfg.bootstrap());
        return new ArrayList<>(out);
    }

    /** Asks every known broker and merges what they know: per name, the newest resource any of them knows. */
    private List<ResourceMetadata> metadataFromAll(String... asked) {
        return metadataFromEvery(asked).resources();
    }

    /** What every broker knows, as {@link #metadataFromAll}, and whether every broker answered. */
    private record Merged(List<ResourceMetadata> resources, boolean allAnswered) {}

    private Merged metadataFromEvery(String... asked) {
        Set<Address> sorted = new TreeSet<>(java.util.Comparator.comparing(Address::toString));
        sorted.addAll(candidates());
        List<ResourceMetadata> merged = new ArrayList<>();
        boolean all = true;
        for (Address addr : sorted) {
            try {
                if (call(addr, new Request.Metadata(List.of(asked))) instanceof Response.Metadata m) {
                    learn(m);
                    mergeNewest(merged, m.resources());
                } else {
                    all = false;
                }
            } catch (CallFailure e) {
                all = false; // a broker that does not answer knows nothing
            }
        }
        return new Merged(merged, all);
    }

    /**
     * Adds what one broker knows to {@code merged}, keeping per name the newest resource: a name deleted and
     * created again has a new, higher id (ids grow with the metadata log), and a broker that lags may still report
     * the old one. A resource with an error names nothing.
     */
    static void mergeNewest(List<ResourceMetadata> merged, List<ResourceMetadata> more) {
        for (ResourceMetadata r : more) {
            if (!r.error().isOk()) {
                continue;
            }
            int at = -1;
            for (int i = 0; i < merged.size(); i++) {
                if (merged.get(i).name().equals(r.name())) {
                    at = i;
                    break;
                }
            }
            if (at < 0) {
                merged.add(r);
            } else if (Long.compareUnsigned(merged.get(at).resourceId(), r.resourceId()) < 0) {
                merged.set(at, r);
            }
        }
    }

    private void learn(Response.Metadata m) {
        synchronized (state) {
            listed.clear();
            for (Messages.Broker b : m.brokers()) {
                brokers.put(b.nodeId(), new Address(b.host(), b.port()));
                zones.put(b.nodeId(), b.rack());
                listed.add(b.nodeId());
            }
            for (ResourceMetadata r : m.resources()) {
                if (!r.error().isOk()) {
                    continue;
                }
                names.put(r.resourceId(), r.name());
                if (r.exchange() != null) {
                    exchanges.put(r.name(), Exchange.of(r.name(), r.resourceId(), r.exchange()));
                    exchangesAt.put(r.name(), System.nanoTime());
                }
                for (PartitionMetadata p : r.partitions()) {
                    PartitionKey key = new PartitionKey(r.name(), (int) p.partition());
                    routes.put(key, new Route(r.resourceId(), p.leader(), p.leaderTerm()));
                    voters.put(key, List.copyOf(p.voters()));
                    notInZone.remove(key);
                    zoneReplica.remove(key);
                }
            }
        }
    }

    // ---- routing ----

    /** The leader's address, refreshing metadata if none is known. */
    Routed route(String stream, int partition) {
        Route known;
        synchronized (state) {
            known = routes.get(new PartitionKey(stream, partition));
        }
        Route route = known != null && known.leader() != 0 ? known : resolve(stream, partition);
        Address addr;
        synchronized (state) {
            addr = brokers.get(route.leader());
        }
        if (addr == null) {
            forget(stream, partition);
            throw Attempt.retry("no address for node " + route.leader());
        }
        return new Routed(route, addr);
    }

    private Route resolve(String stream, int partition) {
        Response.Metadata m;
        try {
            m = metadata(stream);
        } catch (ClientException e) {
            throw Attempt.fail(e);
        }
        List<ResourceMetadata> resources = m.resources();
        boolean allAnswered = true;
        if (find(resources, stream) == null) {
            // A resource created a moment ago may not have reached that broker yet: any broker that knows it will do.
            Merged merged = metadataFromEvery(stream);
            resources = merged.resources();
            allAnswered = merged.allAnswered();
        }
        ResourceMetadata res = find(resources, stream);
        PartitionMetadata p = null;
        if (res != null) {
            for (PartitionMetadata candidate : res.partitions()) {
                if (candidate.partition() == partition) {
                    p = candidate;
                }
            }
        }
        if (p == null) {
            // Only every broker saying so makes it unknown: the one that did not answer may be the metadata leader,
            // the only one that has applied a creation just answered.
            if (!allAnswered) {
                throw Attempt.retry("no broker that answered knows " + stream + " yet");
            }
            throw Attempt.fail(new ClientException.UnknownStream(stream, partition));
        }
        long leader = p.leader();
        long term = p.leaderTerm();
        if (leader == 0) {
            // Partitions created at runtime live on some nodes only: their voters know the leader.
            for (long voter : p.voters()) {
                Address addr;
                synchronized (state) {
                    addr = brokers.get(voter);
                }
                if (addr == null) {
                    continue;
                }
                try {
                    if (!(call(addr, new Request.Metadata(List.of(stream))) instanceof Response.Metadata vm)) {
                        continue;
                    }
                    learn(vm);
                    ResourceMetadata vr = find(vm.resources(), stream, false);
                    if (vr == null) {
                        continue;
                    }
                    for (PartitionMetadata vp : vr.partitions()) {
                        if (vp.partition() == partition && vp.leader() != 0) {
                            leader = vp.leader();
                            term = vp.leaderTerm();
                        }
                    }
                    if (leader != 0) {
                        break;
                    }
                } catch (CallFailure e) {
                    // the next voter may know
                }
            }
        }
        if (leader == 0) {
            throw Attempt.retry("the partition has no leader");
        }
        return new Route(res.resourceId(), leader, term);
    }

    private static ResourceMetadata find(List<ResourceMetadata> resources, String name) {
        return find(resources, name, true);
    }

    private static ResourceMetadata find(List<ResourceMetadata> resources, String name, boolean mustBeOk) {
        for (ResourceMetadata r : resources) {
            if (r.name().equals(name) && (!mustBeOk || r.error().isOk())) {
                return r;
            }
        }
        return null;
    }

    void forget(String stream, int partition) {
        synchronized (state) {
            routes.remove(new PartitionKey(stream, partition));
        }
    }

    /**
     * Follows a leader hint, or forgets the route so metadata is reloaded. A hint is followed only if its term is
     * not older than the route's and it names another leader: an older hint comes from a node that lags.
     */
    void redirect(String stream, int partition, LeaderHint hint) {
        PartitionKey key = new PartitionKey(stream, partition);
        synchronized (state) {
            Route r = routes.get(key);
            if (hint != null && r != null && Long.compareUnsigned(hint.term(), r.term()) >= 0 && hint.leader() != r.leader()) {
                routes.put(key, new Route(r.resourceId(), hint.leader(), hint.term()));
            } else {
                routes.remove(key);
            }
        }
    }

    // ---- the retry loop ----

    <T> T retrying(Supplier<T> attempt) {
        long start = System.nanoTime();
        boolean maybeDone = false;
        while (true) {
            String why;
            ErrorCode refused = null;
            try {
                return attempt.get();
            } catch (Attempt a) {
                switch (a.kind) {
                    case FAIL -> {
                        // A failed login or a refusal now says nothing of an earlier attempt whose answer was lost:
                        // that one may have been carried out. So does a full queue (ADR 0118): an earlier attempt
                        // may commit at a leader that has it in its log. (An answer about the request itself, such
                        // as RESOURCE_EXISTS, is the caller's to read.)
                        ClientException e = a.error;
                        boolean refusal = e instanceof ClientException.Authentication
                                || (e instanceof ClientException.Broker b
                                        && (b.code().equals(ErrorCode.UNAUTHORIZED)
                                                || b.code().equals(ErrorCode.QUEUE_FULL)));
                        if (maybeDone && refusal) {
                            throw new ClientException.Unknown(
                                    "an earlier attempt may have been carried out; then " + e.getMessage());
                        }
                        throw e;
                    }
                    case RETRY -> why = a.getMessage();
                    case RETRY_MAYBE_DONE -> {
                        maybeDone = true;
                        why = a.getMessage();
                    }
                    case REFUSED -> {
                        refused = a.code;
                        why = a.getMessage();
                    }
                    default -> throw new IllegalStateException();
                }
            }
            // A connection that could not authenticate: the same credentials will not do better on the next try.
            ClientException failedLogin = fatal.getAndSet(null);
            if (failedLogin != null) {
                throw maybeDone
                        ? new ClientException.Unknown(
                                "an earlier attempt may have been carried out; then " + failedLogin.getMessage())
                        : failedLogin;
            }
            if (elapsed(start).compareTo(cfg.retryFor()) >= 0) {
                if (maybeDone) {
                    throw new ClientException.Unknown("still unknown after " + cfg.retryFor() + ": " + why);
                }
                throw refused != null
                        ? new ClientException.Broker(refused, why)
                        : new ClientException.GaveUp(cfg.retryFor(), why);
            }
            if (!pause()) {
                throw maybeDone
                        ? new ClientException.Unknown("interrupted while the outcome was unknown: " + why)
                        : new ClientException.GaveUp(elapsed(start), "interrupted");
            }
        }
    }

    private static Duration elapsed(long startNanos) {
        return Duration.ofNanos(System.nanoTime() - startNanos);
    }

    /** Sleeps one backoff; {@code false} if the thread was interrupted (the flag stays set). */
    private boolean pause() {
        try {
            Thread.sleep(cfg.backoff().toMillis(), (int) (cfg.backoff().toNanos() % 1_000_000));
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    // ---- produce ----

    /**
     * Appends messages to a partition as one batch and returns where it went once it is committed.
     *
     * @param partition the partition; Lotse's client does not choose one for you
     * @throws ClientException.Unknown the outcome is unknown after retrying; see the class description
     * @throws ClientException.GaveUp nothing was written and no broker could be reached in time
     * @throws ClientException.UnknownStream the stream or partition does not exist
     */
    public ProduceResult produce(String stream, int partition, List<OutMessage> messages) {
        return produceBatch(stream, partition, build(messages));
    }

    /** Appends one message to a partition; see {@link #produce(String, int, List)}. */
    public ProduceResult produce(String stream, int partition, OutMessage message) {
        return produce(stream, partition, List.of(message));
    }

    /** {@link #produce(String, int, List)} as a future. */
    public CompletableFuture<ProduceResult> produceAsync(String stream, int partition, List<OutMessage> messages) {
        ByteString batch = build(messages);
        return CompletableFuture.supplyAsync(() -> produceBatch(stream, partition, batch), executor);
    }

    /** {@link #produce(String, int, OutMessage)} as a future. */
    public CompletableFuture<ProduceResult> produceAsync(String stream, int partition, OutMessage message) {
        return produceAsync(stream, partition, List.of(message));
    }

    /** {@link #produceBatch} as a future. */
    public CompletableFuture<ProduceResult> produceBatchAsync(String stream, int partition, ByteString batch) {
        return CompletableFuture.supplyAsync(() -> produceBatch(stream, partition, batch), executor);
    }

    static ByteString build(List<OutMessage> messages) {
        if (messages.isEmpty()) {
            throw new IllegalArgumentException("there is no message to produce");
        }
        long now = System.currentTimeMillis();
        BatchBuilder b = new BatchBuilder();
        for (OutMessage m : messages) {
            b.push(m.timestamp() == null ? now : m.timestamp(), m.key(), m.value(), m.headers());
        }
        try {
            return b.finish();
        } catch (RecordFormatException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    /**
     * Appends a record batch built with {@link BatchBuilder} to a partition. Returns the offset of its first
     * record once the batch is committed. The batch is sent as it is, with the producer fields of this client
     * written into a copy.
     */
    public ProduceResult produceBatch(String stream, int partition, ByteString batch) {
        checkOpen();
        if (!cfg.idempotent()) {
            return retrying(() -> produceOnce(stream, partition, batch, false, null));
        }
        long records;
        try {
            records = Batch.parse(batch).batch().recordCount();
        } catch (RecordFormatException e) {
            throw new ClientException.Broker(ErrorCode.CORRUPT_BATCH, e.getMessage());
        }
        PartitionKey key = new PartitionKey(stream, partition);
        // One produce per partition at a time, so sequence numbers reach the leader in order.
        ReentrantLock lock = partitionLocks.computeIfAbsent(key, k -> new ReentrantLock(true));
        lock.lock();
        try {
            // The sequence belongs to the incarnation the stream has now; the produce stays with it (ADR 0107).
            long resourceId = retrying(() -> route(stream, partition).route().resourceId());
            IncarnationKey incarnation = new IncarnationKey(resourceId, partition);
            Stamp stamp = nextSequence(incarnation);
            byte[] bytes = batch.toByteArray();
            Batch.setProducer(bytes, stamp.producerId(), stamp.epoch(), stamp.sequence());
            ByteString stamped = ByteString.view(bytes, 0, bytes.length);
            ProduceResult result;
            boolean[] inDoubt = {false};
            try {
                // Every attempt sends the same bytes: the same producer id and the same sequence.
                result = retrying(() -> {
                    try {
                        return produceOnce(stream, partition, stamped, true, null, resourceId);
                    } catch (Attempt a) {
                        if (a.kind == Attempt.Kind.RETRY_MAYBE_DONE) {
                            inDoubt[0] = true;
                        }
                        throw a;
                    }
                });
            } catch (ClientException e) {
                synchronized (sessionLock) {
                    // Written or not, nobody can tell: later batches would wait behind a gap that may never fill.
                    // Start over with a new producer id. Anything else was definitely not written, and the
                    // sequence is free for the next batch.
                    // A named producer that is fenced has a newer instance: it keeps its fenced id rather
                    // than start over, which would fence that one in turn (ADR 0071).
                    boolean fencedByName = cfg.producerName() != null
                            && e instanceof ClientException.Broker f
                            && f.code().equals(ErrorCode.PRODUCER_FENCED);
                    // The stream went to another incarnation after an attempt that may have been written to this one:
                    // in doubt as well.
                    boolean startOver = !fencedByName
                            && (e instanceof ClientException.Unknown
                                    || (e instanceof ClientException.UnknownStream && inDoubt[0])
                                    || (e instanceof ClientException.Broker b
                                            && (b.code().equals(ErrorCode.OUT_OF_ORDER_SEQUENCE)
                                                    || b.code().equals(ErrorCode.PRODUCER_FENCED))));
                    if (startOver && session != null && session.producerId == stamp.producerId()) {
                        session = null;
                    }
                }
                throw e;
            }
            synchronized (sessionLock) {
                if (session != null && session.producerId == stamp.producerId()) {
                    session.next.put(incarnation, stamp.sequence() + (int) records);
                }
            }
            return result;
        } finally {
            lock.unlock();
        }
    }

    /** The producer id, epoch and next sequence for a partition, getting a producer id first if there is none. */
    private Stamp nextSequence(IncarnationKey key) {
        synchronized (sessionLock) {
            if (session != null) {
                return stampOf(session, key);
            }
        }
        ProducerIds ids = retrying(() -> initProducerOnce(cfg.producerName()));
        synchronized (sessionLock) {
            // Another partition's produce may have started a session meanwhile.
            if (session == null) {
                session = new Session(ids.producerId(), ids.epoch());
            }
            return stampOf(session, key);
        }
    }

    private static Stamp stampOf(Session s, IncarnationKey key) {
        return new Stamp(s.producerId, s.epoch, s.next.getOrDefault(key, 0));
    }

    ProduceResult produceOnce(String stream, int partition, ByteString batch, boolean idempotent, String txnId) {
        return produceOnce(stream, partition, batch, idempotent, txnId, -1);
    }

    /**
     * {@link #produceOnce} to the incarnation {@code pin} if it is not -1: a route to another one (the stream was
     * deleted and created again) ends the produce with {@link ClientException.UnknownStream}, since its sequence
     * belongs to the one it was taken for (ADR 0107).
     */
    ProduceResult produceOnce(
            String stream, int partition, ByteString batch, boolean idempotent, String txnId, long pin) {
        // An idempotent batch may be sent again whatever happened to it.
        Routed routed = route(stream, partition);
        if (pin != -1 && routed.route().resourceId() != pin) {
            throw Attempt.fail(new ClientException.UnknownStream(stream, partition));
        }
        Request.Produce req = new Request.Produce(
                txnId,
                Math.min(cfg.produceTimeout().toMillis(), U32_MAX),
                List.of(new ProduceResource(routed.route().resourceId(), List.of(new ProducePartition(partition, batch)))),
                cfg.tenant());
        Response resp;
        try {
            resp = call(routed.address(), req);
        } catch (CallFailure e) {
            switch (e.kind) {
                case NOT_SENT -> {
                    // Not sent at all: safe to try again.
                    forget(stream, partition);
                    throw Attempt.retry(e.getMessage());
                }
                case LOST -> {
                    forget(stream, partition);
                    throw unknown(idempotent, e.getMessage());
                }
                default -> throw e.toAttempt();
            }
        }
        if (!(resp instanceof Response.Produce produced)) {
            throw unknown(idempotent, "unexpected response " + resp);
        }
        if (produced.resources().isEmpty() || produced.resources().get(0).partitions().isEmpty()) {
            throw unknown(idempotent, "empty produce response");
        }
        ProducePartitionResponse p = produced.resources().get(0).partitions().get(0);
        ErrorCode code = p.error();
        if (code.isOk()) {
            return new ProduceResult(p.baseOffset(), p.appendTs());
        }
        if (code.equals(ErrorCode.NOT_LEADER) || code.equals(ErrorCode.FENCED_LEADER_TERM)) {
            // Refused before it was proposed, or definitely not written.
            redirect(stream, partition, p.leaderHint());
            throw Attempt.retry(code.toString());
        }
        if (code.equals(ErrorCode.PRODUCER_LIMIT)) {
            // Not written either: the partition takes a new producer once it has forgotten an idle one (ADR 0110).
            throw Attempt.retry(p.errorMsg() != null ? p.errorMsg() : code.toString());
        }
        if (code.equals(ErrorCode.STORAGE_FULL)) {
            // Not written: the partition takes new data once its voters have room again (ADR 0122).
            throw Attempt.refused(code, p.errorMsg() != null ? p.errorMsg() : code.toString());
        }
        if (code.equals(ErrorCode.REQUEST_TIMED_OUT)) {
            forget(stream, partition);
            throw unknown(idempotent, p.errorMsg() != null ? p.errorMsg() : code.toString());
        }
        if (code.equals(ErrorCode.UNKNOWN_RESOURCE)) {
            // Deleted: the name may come back as a new resource.
            forget(stream, partition);
        }
        throw Attempt.fail(new ClientException.Broker(code, p.errorMsg()));
    }

    private static Attempt unknown(boolean idempotent, String why) {
        return idempotent ? Attempt.retryMaybeDone(why) : Attempt.fail(new ClientException.Unknown(why));
    }

    // ---- the producer id ----

    /** The coordinator's address, asking any broker for it if needed. */
    private Address coordinator(String key) {
        Address known = coordinatorAddress;
        if (known != null) {
            return known;
        }
        Request find = new Request.FindCoordinator(Messages.CoordinatorType.PRODUCER, key);
        String why = "no broker answered";
        for (Address addr : candidates()) {
            try {
                Response r = call(addr, find);
                if (r instanceof Response.FindCoordinator f) {
                    if (f.error().isOk()) {
                        Address a = new Address(f.host(), f.port());
                        coordinatorVoters = f.voters();
                        coordinatorAddress = a;
                        return a;
                    }
                    why = f.errorMsg() != null ? f.errorMsg() : f.error().toString();
                    if (!f.error().retriable()) {
                        throw Attempt.fail(new ClientException.Broker(f.error(), why));
                    }
                } else {
                    why = "unexpected response " + r;
                }
            } catch (CallFailure e) {
                why = e.describe();
            }
        }
        throw Attempt.retry(why);
    }

    void forgetCoordinator() {
        coordinatorAddress = null;
    }

    /** One request to the coordinator; failures become {@code Attempt}s (every coordinator request may be repeated). */
    Response callCoordinator(String key, Request req) {
        return callCoordinator(key, req, List.of());
    }

    /** {@link #callCoordinator(String, Request)} with the request header's {@code read_at_least}; empty, no tag. */
    private Response callCoordinator(String key, Request req, List<GroupIndex> readAtLeast) {
        Address addr = coordinator(key);
        try {
            return call(addr, req, readAtLeast);
        } catch (CallFailure e) {
            forgetCoordinator();
            throw e.toAttempt();
        }
    }

    /**
     * A read of the coordinator group's state ({@code OffsetFetch}): from the lowest-id voter in this client's zone,
     * which answers once it has applied what this client saw (the zone reads design). A refusal, an error or a failed
     * call sends that one read to the coordinator with the same indexes, which a coordinator deposed without knowing
     * it refuses (ADR 0141); the next read tries the zone again.
     */
    Response callCoordinatorRead(String key, Request req) {
        String zone = cfg.zone();
        List<GroupIndex> atLeast = List.of();
        if (zone != null) {
            // Learns the voters, if no FindCoordinator did yet.
            coordinator(key);
            atLeast = indexes.snapshot();
            Address addr;
            synchronized (state) {
                List<Long> candidates = new ArrayList<>(coordinatorVoters);
                candidates.retainAll(listed);
                Long node = Zone.zoneNode(zone, candidates, zones);
                addr = node == null ? null : brokers.get(node);
            }
            if (addr != null) {
                try {
                    Response resp = call(addr, req, atLeast);
                    if (!Zone.refused(resp)) {
                        return resp;
                    }
                } catch (CallFailure e) {
                    // the coordinator answers instead
                }
            }
        }
        return callCoordinator(key, req, atLeast);
    }

    ProducerIds initProducerOnce(String txn) {
        Request init = new Request.InitProducer(txn, Math.min(cfg.txnTimeout().toMillis(), U32_MAX), cfg.tenant());
        Response resp = callCoordinator(txn == null ? "" : txn, init);
        if (!(resp instanceof Response.InitProducer r)) {
            throw Attempt.retry("unexpected response " + resp);
        }
        if (r.error().isOk()) {
            return new ProducerIds(r.producerId(), r.producerEpoch());
        }
        if (r.error().retriable()) {
            // An id that was handed out but never received is just unused.
            forgetCoordinator();
            throw Attempt.retry(r.error().toString());
        }
        throw Attempt.fail(new ClientException.Broker(r.error(), null));
    }

    // ---- transactions (ADR 0029) ----

    /**
     * Starts a transaction for the configured transactional id ({@link ClientConfig.Builder#transactionalId}),
     * getting its producer id from the coordinator first, which fences any older client with the same id. One
     * transaction at a time per client: a second call waits until the first ends. End it with {@link
     * Transaction#commit} or {@link Transaction#abort}; closed without either, the next transaction gets a fresh
     * producer id, and the coordinator aborts what was left open.
     *
     * @throws ClientException.NotTransactional no transactional id is configured
     */
    public Transaction beginTransaction() {
        checkOpen();
        String txnId = cfg.transactionalId();
        if (txnId == null) {
            throw new ClientException.NotTransactional();
        }
        try {
            txnTurn.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ClientException.GaveUp(Duration.ZERO, "interrupted while waiting for the open transaction");
        }
        try {
            ProducerIds ids = txnProducer.get();
            if (ids == null) {
                ids = retrying(() -> initProducerOnce(txnId));
                txnProducer.set(ids);
            }
            return new Transaction(this, txnId, ids.producerId(), ids.epoch());
        } catch (RuntimeException e) {
            txnTurn.release();
            throw e;
        }
    }

    /** A transaction ended: {@code next} is the producer id for the next one, {@code null} to ask anew. */
    void transactionEnded(ProducerIds next) {
        txnProducer.set(next);
        txnTurn.release();
    }

    // ---- exchanges routed by the client, in a transaction (ADR 0032) ----

    /** The exchange as metadata last said within {@code metadataMaxAge}, asking for it otherwise. */
    Exchange exchange(String name) {
        synchronized (state) {
            Long at = exchangesAt.get(name);
            if (at != null && System.nanoTime() - at < cfg.metadataMaxAge().toNanos()) {
                return exchanges.get(name);
            }
        }
        // learn() keeps the exchange and the queues bound to it.
        Response.Metadata m = metadata(name);
        if (find(m.resources(), name) == null) {
            // Declared a moment ago: not every broker knows it yet.
            metadataFromAll(name);
        }
        synchronized (state) {
            Exchange x = exchanges.get(name);
            if (x == null) {
                throw new ClientException.UnknownStream(name, 0);
            }
            return x;
        }
    }

    /** The queues {@code message} goes to, by name, sorted. With {@code mandatory}, a message no queue takes is refused. */
    List<String> routeMessage(String exchange, String routingKey, OutMessage message, boolean mandatory) {
        Exchange x = exchange(exchange);
        TreeSet<Long> ids = x.route(routingKey, message.headers());
        if (ids.isEmpty() && mandatory) {
            throw new ClientException.Unroutable(exchange, routingKey);
        }
        List<String> queues = new ArrayList<>();
        synchronized (state) {
            for (long id : ids) {
                String n = names.get(id);
                if (n == null) {
                    throw new ClientException.UnknownStream(
                            "queue " + Long.toUnsignedString(id) + " bound to " + exchange, 0);
                }
                queues.add(n);
            }
        }
        Collections.sort(queues);
        return queues;
    }

    // ---- consumer groups (ADR 0033) ----

    /**
     * Joins {@code group} to read {@code streams}; partitions arrive with the first calls to {@link
     * GroupConsumer#poll}.
     */
    public GroupConsumer joinGroup(String group, List<String> streams, GroupConfig config) {
        checkOpen();
        // Learn the streams' ids (and names) first.
        metadata(streams.toArray(new String[0]));
        return new GroupConsumer(this, group, List.copyOf(streams), null, config);
    }

    /**
     * Joins {@code group} to read every stream of the tenant whose whole name matches the regular expression {@code
     * pattern} and that this principal may read (ADR 0073). The coordinator matches it with every heartbeat, so
     * streams created later join in.
     */
    public GroupConsumer joinGroupMatching(String group, String pattern, GroupConfig config) {
        checkOpen();
        metadata();
        return new GroupConsumer(this, group, List.of(), pattern, config);
    }

    /** The id of a stream this client has looked up, or {@code null}. */
    Long streamId(String stream) {
        synchronized (state) {
            for (Map.Entry<Long, String> e : names.entrySet()) {
                if (e.getValue().equals(stream)) {
                    return e.getKey();
                }
            }
            return null;
        }
    }

    /** The name of a resource this client has looked up, or {@code null}. */
    String streamName(long id) {
        synchronized (state) {
            return names.get(id);
        }
    }

    ClientConfig config() {
        return cfg;
    }

    // ---- list offsets (ADR 0059) ----

    /**
     * Where {@code at} falls in a partition, as a read_committed reader sees it (ADR 0059): {@link OffsetAt#EARLIEST}
     * is the first offset still stored, {@link OffsetAt#LATEST} the offset after the last one it may read, and
     * {@link OffsetAt#time} the first offset of the first batch with a record at or after that time, or the latest
     * offset if there is none yet.
     */
    public long listOffset(String stream, int partition, OffsetAt at) {
        checkOpen();
        return retrying(() -> listOffsetOnce(stream, partition, at));
    }

    private long listOffsetOnce(String stream, int partition, OffsetAt at) {
        Routed routed = route(stream, partition);
        // Like a fetch, from the replica in this client's zone, which may lag the leader (ADR 0116).
        Address replica = replicaInZone(stream, partition, routed.route().leader());
        boolean allowFollower = replica != null;
        Address addr = allowFollower ? replica : routed.address();
        Request req = new Request.ListOffsets(
                Messages.Isolation.COMMITTED,
                List.of(new Messages.ListOffsetsResource(
                        routed.route().resourceId(), List.of(new Messages.ListOffsetsPartition(partition, at.timestamp())))),
                allowFollower);
        Response resp;
        try {
            resp = call(addr, req);
        } catch (CallFailure e) {
            if (allowFollower) {
                throw leaveReplica(stream, partition, addr + ": " + e.getMessage());
            }
            forget(stream, partition);
            throw e.toAttempt();
        }
        if (!(resp instanceof Response.ListOffsets r)) {
            throw Attempt.retry("unexpected response " + resp);
        }
        if (r.resources().isEmpty() || r.resources().get(0).partitions().isEmpty()) {
            throw Attempt.retry("empty list-offsets response");
        }
        Messages.ListOffsetsPartitionResponse p = r.resources().get(0).partitions().get(0);
        ErrorCode code = p.error();
        if (allowFollower && !code.isOk()) {
            throw leaveReplica(stream, partition, addr + ": " + code);
        }
        if (code.isOk()) {
            return p.offset();
        }
        if (code.retriable()) {
            redirect(stream, partition, p.leaderHint());
            throw Attempt.retry(code.toString());
        }
        if (code.equals(ErrorCode.UNKNOWN_RESOURCE)) {
            forget(stream, partition);
        }
        throw Attempt.fail(new ClientException.Broker(code, null));
    }

    // ---- fetch ----

    /**
     * Reads committed batches from {@code offset}, waiting up to {@code maxWait} at the broker for some to arrive
     * (a long poll). Keep {@code maxWait} well below {@link ClientConfig#requestTimeout()}.
     *
     * <p>This is read_committed: reading stops at the last stable offset. Use {@link Fetched#committedBatches()}
     * or {@link Fetched#committedMessages()} to leave out control batches and aborted transactions.
     *
     * @throws ClientException.OffsetOutOfRange retention deleted the offset, or it is past the end; the exception
     *     says where the partition starts
     */
    public Fetched fetch(String stream, int partition, long offset, Duration maxWait) {
        return fetch(stream, partition, offset, maxWait, Messages.Isolation.COMMITTED);
    }

    /**
     * Like {@link #fetch}, but also past the last stable offset: batches of open and aborted transactions and
     * their markers, up to the high watermark. For inspecting a log, not for processing it.
     */
    public Fetched fetchUncommitted(String stream, int partition, long offset, Duration maxWait) {
        return fetch(stream, partition, offset, maxWait, Messages.Isolation.UNCOMMITTED);
    }

    /** {@link #fetch} as a future. */
    public CompletableFuture<Fetched> fetchAsync(String stream, int partition, long offset, Duration maxWait) {
        return CompletableFuture.supplyAsync(() -> fetch(stream, partition, offset, maxWait), executor);
    }

    private Fetched fetch(String stream, int partition, long offset, Duration maxWait, int isolation) {
        checkOpen();
        return retrying(() -> fetchOnce(stream, partition, offset, maxWait, isolation));
    }

    /**
     * A replica of the partition in this client's zone other than the leader: where fetches go (ADR 0104). The one the
     * leader last named, if any, else a voter in the zone as metadata has them. {@code null} without a zone, when the
     * leader is in it, or when there is none.
     */
    private Address replicaInZone(String stream, int partition, long leader) {
        String zone = cfg.zone();
        if (zone == null) {
            return null;
        }
        PartitionKey key = new PartitionKey(stream, partition);
        synchronized (state) {
            if (notInZone.contains(key) || zone.equals(zones.get(leader))) {
                return null;
            }
            Long named = zoneReplica.get(key);
            if (named != null && named != leader && brokers.containsKey(named)) {
                return brokers.get(named);
            }
            for (long v : voters.getOrDefault(key, List.of())) {
                if (zone.equals(zones.get(v))) {
                    return brokers.get(v);
                }
            }
            return null;
        }
    }

    /**
     * Leaves a partition's replica in the zone for the leader until metadata is asked for again: an older node,
     * one that left, or one whose log starts later.
     */
    private Attempt leaveReplica(String stream, int partition, String why) {
        synchronized (state) {
            notInZone.add(new PartitionKey(stream, partition));
        }
        return Attempt.retry(why);
    }

    private Fetched fetchOnce(String stream, int partition, long offset, Duration maxWait, int isolation) {
        Routed routed = route(stream, partition);
        Address replica = replicaInZone(stream, partition, routed.route().leader());
        boolean allowFollower = replica != null;
        Address addr = allowFollower ? replica : routed.address();
        Request.Fetch req = new Request.Fetch(
                Math.min(maxWait.toMillis(), U32_MAX),
                1,
                4L << 20,
                isolation,
                List.of(new FetchResource(
                        routed.route().resourceId(),
                        List.of(new FetchPartition(partition, offset, routed.route().term(), 1L << 20)))),
                allowFollower,
                // Only the leader names a replica to come back to.
                allowFollower ? null : cfg.zone());
        Response resp;
        try {
            resp = call(addr, req);
        } catch (CallFailure e) {
            if (allowFollower) {
                throw leaveReplica(stream, partition, addr + ": " + e.getMessage());
            }
            forget(stream, partition);
            throw e.toAttempt();
        }
        if (!(resp instanceof Response.Fetch fetched)) {
            throw Attempt.retry("unexpected response " + resp);
        }
        if (fetched.resources().isEmpty() || fetched.resources().get(0).partitions().isEmpty()) {
            throw Attempt.retry("empty fetch response");
        }
        FetchPartitionResponse p = fetched.resources().get(0).partitions().get(0);
        ErrorCode code = p.error();
        if (allowFollower && !code.isOk()) {
            throw leaveReplica(stream, partition, addr + ": " + code);
        }
        // The leader names a replica in this client's zone that is current again: the next fetch goes there, without
        // asking for metadata. An answer with an error says nothing about the partition's replicas, and a node without
        // a known address cannot be asked: clearing the fallback for it would send the next fetch to the replica that
        // failed.
        Long preferred = p.preferredReplica();
        if (!allowFollower && code.isOk() && preferred != null && preferred != routed.route().leader()) {
            synchronized (state) {
                if (brokers.containsKey(preferred)) {
                    PartitionKey key = new PartitionKey(stream, partition);
                    notInZone.remove(key);
                    zoneReplica.put(key, preferred);
                }
            }
        }
        if (code.isOk()) {
            return new Fetched(p.records(), p.highWatermark(), p.lastStableOffset(), p.logStartOffset(), p.abortedTxns());
        }
        if (code.retriable()) {
            redirect(stream, partition, p.leaderHint());
            throw Attempt.retry(code.toString());
        }
        if (code.equals(ErrorCode.OFFSET_OUT_OF_RANGE)) {
            throw Attempt.fail(new ClientException.OffsetOutOfRange(stream, partition, offset, p.logStartOffset()));
        }
        if (code.equals(ErrorCode.UNKNOWN_RESOURCE)) {
            forget(stream, partition);
        }
        throw Attempt.fail(new ClientException.Broker(code, null));
    }

    // ---- publishing through exchanges (ADR 0079) ----

    /**
     * Publishes a message to an exchange, which routes it to queues by {@code routingKey} (and headers). A node
     * routes it by the bindings it knows and writes it to each queue, if this client's roles allow every one of
     * them. Returns once every routed queue holds it; a message no queue takes is dropped, or refused with
     * {@link ClientException.Unroutable} when {@code mandatory}. If some queues could not be written,
     * {@link ClientException.NotAllWritten} says which hold it. If the node's answer is lost, the result is
     * {@link ClientException.Unknown}: the publish is not sent again, since the node may have written it.
     */
    public Published publish(String exchange, String routingKey, OutMessage message, boolean mandatory) {
        checkOpen();
        ByteString batch = build(List.of(message));
        // A name deleted and made again has a new id: ask once more.
        for (int attempt = 0; attempt < 2; attempt++) {
            if (attempt > 0) {
                exchangeIds.remove(exchange);
            }
            long id = exchangeId(exchange);
            Messages.PublishResult r = publishOnce(new Messages.PublishEntry(id, routingKey, mandatory, batch));
            ErrorCode code = r.error();
            if (code.isOk()) {
                return published(r);
            }
            if (code.equals(ErrorCode.UNKNOWN_RESOURCE) && attempt == 0) {
                continue;
            }
            if (code.equals(ErrorCode.UNKNOWN_RESOURCE)) {
                throw new ClientException.UnknownStream(exchange, 0);
            }
            if (code.equals(ErrorCode.UNROUTABLE)) {
                throw new ClientException.Unroutable(exchange, routingKey);
            }
            throw new ClientException.Broker(code, null);
        }
        throw new IllegalStateException("the second attempt returns");
    }

    private final Map<String, Long> exchangeIds = new ConcurrentHashMap<>();

    /** The exchange's id, from metadata the first time. */
    private long exchangeId(String exchange) {
        Long known = exchangeIds.get(exchange);
        if (known != null) {
            return known;
        }
        ResourceMetadata r = find(metadata(exchange).resources(), exchange, false);
        if (r == null || !r.error().isOk() || r.kind() != Messages.ResourceKind.EXCHANGE) {
            throw new ClientException.UnknownStream(exchange, 0);
        }
        exchangeIds.put(exchange, r.resourceId());
        return r.resourceId();
    }

    /** One {@code Publish} to the first node that takes it. */
    private Messages.PublishResult publishOnce(Messages.PublishEntry entry) {
        long timeoutMs = Math.min(cfg.requestTimeout().toMillis(), 0xFFFF_FFFFL);
        Request req = new Request.Publish(null, timeoutMs, List.of(entry));
        long start = System.nanoTime();
        String why = "no broker known";
        while (true) {
            for (Address addr : candidates()) {
                try {
                    Response r = call(addr, req);
                    if (r instanceof Response.Publish p && p.entries().size() == 1) {
                        Messages.PublishResult entry0 = p.entries().get(0);
                        // Refused before anything was written, by a node that may not know the exchange yet:
                        // another node, or later.
                        if (!entry0.error().retriable()) {
                            return entry0;
                        }
                        why = addr + ": " + entry0.error();
                        continue;
                    }
                    why = "unexpected response " + r;
                } catch (CallFailure e) {
                    switch (e.kind) {
                        case DENIED -> throw new ClientException.Authentication(e.getMessage());
                        case REFUSED -> throw new ClientException.Broker(e.code, e.getMessage());
                        // The node may have written it: not sent again.
                        case LOST -> throw new ClientException.Unknown(addr + ": " + e.getMessage());
                        case NOT_SENT -> why = addr + ": " + e.describe();
                    }
                }
            }
            if (elapsed(start).compareTo(cfg.retryFor()) >= 0) {
                throw new ClientException.GaveUp(cfg.retryFor(), why);
            }
            if (!pause()) {
                throw new ClientException.GaveUp(elapsed(start), "interrupted");
            }
        }
    }

    /** A routed message's outcome by queue name. */
    private Published published(Messages.PublishResult result) {
        // The node names each queue: this client may never have seen one bound and deleted between two
        // of its lookups. Only a queue the node no longer knows comes without a name.
        List<Published.Routed> written = new ArrayList<>();
        List<String> writtenNames = new ArrayList<>();
        List<ClientException> failures = new ArrayList<>();
        for (Messages.RoutedMessage q : result.routed()) {
            String name = q.name();
            if (name.isEmpty()) {
                synchronized (state) {
                    name = names.getOrDefault(q.queueId(), "queue " + Long.toUnsignedString(q.queueId()));
                }
            }
            if (q.error().isOk()) {
                written.add(new Published.Routed(name, q.baseOffset()));
                writtenNames.add(name);
            } else if (q.error().equals(ErrorCode.REQUEST_TIMED_OUT)) {
                failures.add(new ClientException.Unknown(name + ": the write's outcome is unknown"));
            } else {
                failures.add(new ClientException.Broker(q.error(), name));
            }
        }
        written.sort(java.util.Comparator.comparing(Published.Routed::queue));
        if (!failures.isEmpty()) {
            throw new ClientException.NotAllWritten(writtenNames, failures);
        }
        return new Published(written);
    }

    // ---- admin (ADR 0034, 0035) ----
    //
    // Admin requests go to the metadata group's leader. Any broker answers NOT_LEADER with a hint to it, which the
    // client follows. A create whose outcome is unknown is retried; if the first attempt did create the resource, the
    // retry finds it and the call fails with ClientException.ResourceExists with its id. A delete is done once the
    // resource is gone, whoever deleted it, so a retried delete succeeds too.

    /**
     * Creates a stream in the configured tenant and returns its id.
     *
     * <p>A create whose outcome is unknown is retried. If the first attempt did create the stream, the retry finds
     * it and the call fails with {@link ClientException.ResourceExists}, which carries the id.
     *
     * @param partitions the number of partitions
     * @param replication the number of nodes holding each partition
     * @param configs settings such as {@code retention.ms} and {@code retention.bytes}; may be empty
     */
    public long createStream(String name, int partitions, int replication, Map<String, String> configs) {
        checkOpen();
        List<Config> list = new ArrayList<>();
        configs.forEach((k, v) -> list.add(new Config(k, v)));
        return create(name, new Request.CreateStream(name, cfg.tenant(), partitions, replication, list));
    }

    /** Sends a create request to the metadata leader and returns the id of what was created. */
    long create(String name, Request req) {
        return retrying(() -> {
            Response r = callAdmin(req);
            if (!(r instanceof Response.Create c)) {
                throw Attempt.retry("unexpected response " + r);
            }
            ErrorCode code = c.error();
            if (code.isOk()) {
                return c.resourceId();
            }
            if (code.equals(ErrorCode.RESOURCE_EXISTS)) {
                throw Attempt.fail(new ClientException.ResourceExists(name, c.resourceId()));
            }
            if (code.retriable()) {
                metadataLeader = c.leaderHint() == null ? null : c.leaderHint().leader();
                throw Attempt.retryMaybeDone(code.toString());
            }
            throw Attempt.fail(new ClientException.Broker(code, c.errorMsg()));
        });
    }

    /**
     * Creates a queue in the configured tenant and returns its id. A create whose outcome is unknown is retried and
     * may then fail with
     * {@link ClientException.ResourceExists}, as for {@link #createStream}.
     */
    public long createQueue(String name, QueueOptions options) {
        checkOpen();
        return create(
                name,
                new Request.CreateQueue(
                        name, cfg.tenant(), 1, false, options.deadLetterQueue(), options.replication(), options.configs()));
    }

    /** Creates a queue on 3 nodes with the default settings. */
    public long createQueue(String name) {
        return createQueue(name, QueueOptions.defaults());
    }

    // ---- queues ----

    /**
     * Subscribes to a queue. The consumer is connected to the queue's leader when this returns; messages are
     * produced to a queue with {@link #produce} to partition 0.
     *
     * @throws ClientException.UnknownStream if the queue does not exist
     * @throws ClientException.GaveUp if no leader can be reached within {@code retryFor}
     */
    public QueueConsumer subscribe(String queue, SubscribeOptions options) {
        checkOpen();
        QueueConsumer consumer = new QueueConsumer(this, queue, options.credit());
        consumer.start();
        return consumer;
    }

    /** {@link #subscribe(String, SubscribeOptions)} with the default options. */
    public QueueConsumer subscribe(String queue) {
        return subscribe(queue, SubscribeOptions.defaults());
    }

    /** Creates a stream with the node's default settings; see {@link #createStream(String, int, int, Map)}. */
    public long createStream(String name, int partitions, int replication) {
        return createStream(name, partitions, replication, Map.of());
    }

    /**
     * Declares an exchange in the configured tenant and returns its id.
     *
     * @param type one of {@link Messages.ExchangeType}: direct, topic, fanout or headers
     */
    public long declareExchange(String name, int type) {
        checkOpen();
        if (type < Messages.ExchangeType.DIRECT || type > Messages.ExchangeType.HEADERS) {
            throw new IllegalArgumentException("unknown exchange type " + type);
        }
        return create(name, new Request.DeclareExchange(name, cfg.tenant(), type, List.of()));
    }

    /** Binds {@code queue} to {@code exchange} with a binding key (direct and topic exchanges). */
    public void bind(String exchange, String queue, String key) {
        binding(exchange, queue, key, List.of(), true, false);
    }

    /**
     * Binds {@code queue} to a headers exchange: all of {@code headers} must match ({@code matchAll}), or any one. A
     * header with a {@code null} value matches only a header whose value is null.
     */
    public void bindHeaders(String exchange, String queue, List<Header> headers, boolean matchAll) {
        List<HeaderMatch> matches = new ArrayList<>();
        for (Header h : headers) {
            matches.add(new HeaderMatch(h.key(), h.value()));
        }
        binding(exchange, queue, "", matches, matchAll, false);
    }

    /** Removes a binding made by {@link #bind}. */
    public void unbind(String exchange, String queue, String key) {
        binding(exchange, queue, key, List.of(), true, true);
    }

    private void binding(
            String exchange, String queue, String key, List<HeaderMatch> matches, boolean matchAll, boolean unbind) {
        checkOpen();
        List<ResourceMetadata> m = metadataFromAll(exchange, queue);
        ResourceMetadata x = find(m, exchange);
        ResourceMetadata q = find(m, queue);
        if (x == null || q == null) {
            throw new ClientException.UnknownStream(exchange + " or " + queue, 0);
        }
        Request req = unbind
                ? new Request.Unbind(x.resourceId(), q.resourceId(), key, matches, matchAll)
                : new Request.Bind(x.resourceId(), q.resourceId(), key, matches, matchAll);
        retrying(() -> {
            Response r = callAdmin(req);
            if (!(r instanceof Response.Outcome o)) {
                throw Attempt.retry("unexpected response " + r);
            }
            adminAnswer(o.error(), o.leaderHint(), null);
            return null;
        });
        // A transaction's publish routes by the bindings this client has seen: see the new ones.
        forgetExchange(exchange);
    }

    private void forgetExchange(String name) {
        synchronized (state) {
            exchanges.remove(name);
            exchangesAt.remove(name);
        }
    }

    /**
     * Deletes a stream, queue or exchange created at runtime, with its data and bindings, and returns its id.
     * Resources from the node configuration cannot be deleted. Done once the resource is gone, even if someone else
     * deleted it first.
     */
    public long delete(String name) {
        return deleteWith(name, false);
    }

    /**
     * {@link #delete}, but only if the stream or queue holds nothing (ADR 0063): no unsettled message in a queue, no
     * record in a stream. Otherwise it fails with {@code RESOURCE_NOT_EMPTY} and nothing is deleted. An exchange
     * holds no messages and is deleted. The check guards against mistakes, not against a producer: a message produced
     * while the delete is under way goes with it.
     */
    public long deleteIfEmpty(String name) {
        return deleteWith(name, true);
    }

    private long deleteWith(String name, boolean ifEmpty) {
        checkOpen();
        long id = resourceId(name);
        Request req = new Request.DeleteResource(id, ifEmpty);
        retrying(() -> {
            Response r = callAdmin(req);
            if (!(r instanceof Response.Admin a)) {
                throw Attempt.retry("unexpected response " + r);
            }
            // Gone already, deleted by this request's earlier attempt or by someone else: done.
            if (a.error().equals(ErrorCode.UNKNOWN_RESOURCE)) {
                return null;
            }
            adminAnswer(a.error(), a.leaderHint(), a.errorMsg());
            return null;
        });
        forgetExchange(name);
        exchangeIds.remove(name);
        return id;
    }

    /**
     * Settles every message of {@code queue} that is ready now: not held by a consumer, not in an open transaction
     * (ADR 0064). Messages produced while it runs are left alone. Returns how many were purged. Needs the
     * tenant-admin role.
     */
    public long purge(String queue) {
        return queueAction(queue, false);
    }

    /**
     * Moves every ready message of the dead-letter queue {@code queue} back to the queue its dead-letter headers
     * name, once each across failovers (ADR 0064). Messages without such headers, or whose queue is gone, stay.
     * Returns how many were moved. Needs the tenant-admin role.
     */
    public long requeue(String queue) {
        return queueAction(queue, true);
    }

    private record ActionStep(long count, long upTo, boolean more) {}

    private long queueAction(String queue, boolean requeue) {
        checkOpen();
        long total = 0;
        long upTo = 0;
        while (true) {
            long bound = upTo;
            ActionStep step = retrying(() -> queueActionOnce(queue, requeue, bound));
            total += step.count();
            upTo = step.upTo();
            if (!step.more()) {
                return total;
            }
        }
    }

    private ActionStep queueActionOnce(String queue, boolean requeue, long upTo) {
        Routed routed = route(queue, 0);
        Response r;
        try {
            r = call(routed.address(), new Request.QueueAction(requeue, routed.route().resourceId(), upTo));
        } catch (CallFailure e) {
            forget(queue, 0);
            throw e.toAttempt();
        }
        if (!(r instanceof Response.QueueAction a)) {
            throw Attempt.retry("unexpected response " + r);
        }
        if (a.error().isOk()) {
            return new ActionStep(a.count(), a.upTo(), a.more());
        }
        if (a.error().retriable()) {
            redirect(queue, 0, a.leaderHint());
            // Acting again is harmless: what was done is settled.
            throw Attempt.retryMaybeDone(a.error().toString());
        }
        throw Attempt.fail(new ClientException.Broker(a.error(), a.errorMsg()));
    }

    /**
     * Changes settings of a resource created at runtime: {@code set} gives keys new values, {@code reset} returns keys
     * to their default.
     */
    public void alterConfigs(String name, Map<String, String> set, List<String> reset) {
        checkOpen();
        long id = resourceId(name);
        List<Config> configs = new ArrayList<>();
        new TreeMap<>(set).forEach((k, v) -> configs.add(new Config(k, v)));
        adminChange(new Request.AlterConfigs(id, configs, reset));
    }

    /**
     * A resource's settings, each with where its value comes from ({@link Messages.ConfigSource}). Read from the
     * metadata leader or, with a zone, from a node in it that has applied every metadata index this client saw, so
     * changes this client made are in it.
     */
    public List<Messages.ConfigEntry> describeConfigs(String name) {
        checkOpen();
        long id = resourceId(name);
        Request req = new Request.DescribeConfigs(List.of(id));
        return retrying(() -> {
            Response r = callAdminRead(req);
            if (!(r instanceof Response.DescribeConfigs d)) {
                throw Attempt.retry("unexpected response " + r);
            }
            adminAnswer(d.error(), d.leaderHint(), null);
            if (d.resources().isEmpty()) {
                throw Attempt.retry("no resource in the answer");
            }
            Messages.ResourceConfigs rc = d.resources().get(0);
            if (!rc.error().isOk()) {
                throw Attempt.fail(new ClientException.Broker(rc.error(), null));
            }
            return rc.configs();
        });
    }

    /**
     * Sets {@code user}'s SCRAM-SHA-512 password, creating the user if needed. Only a salted credential leaves the
     * client; the password does not.
     */
    public void setUserPassword(String user, String password) {
        checkOpen();
        adminChange(ScramCredential.forPassword(password, new SecureRandom()).alterUser(user));
    }

    /**
     * Removes {@code user}'s SCRAM credential. Sessions it opened end at the latest when the node's maximum session
     * time is up.
     */
    public void deleteUser(String user) {
        checkOpen();
        adminChange(ScramCredential.removal(user));
    }

    /** The users with a SCRAM credential, configured and set at runtime. */
    public List<Messages.UserEntry> describeUsers() {
        checkOpen();
        return retrying(() -> {
            Response r = callAdminRead(new Request.DescribeUsers());
            if (!(r instanceof Response.DescribeUsers d)) {
                throw Attempt.retry("unexpected response " + r);
            }
            adminAnswer(d.error(), d.leaderHint(), null);
            return d.users();
        });
    }

    /**
     * The client certificates the cluster has seen or had revoked, by expiry (cluster admin, ADR 0091).
     *
     * @param nowMs the answering node's clock in milliseconds since the Unix epoch, which the entries' states are
     *     computed against
     * @param certificates the inventory
     */
    public record CertificateInventory(long nowMs, List<Messages.CertificateEntry> certificates) {
        /** Creates the result. */
        public CertificateInventory {
            certificates = List.copyOf(certificates);
        }
    }

    /** The inventory of client certificates (cluster admin). */
    public CertificateInventory describeCertificates() {
        checkOpen();
        return retrying(() -> {
            Response r = callAdminRead(new Request.DescribeCertificates());
            if (!(r instanceof Response.DescribeCertificates d)) {
                throw Attempt.retry("unexpected response " + r);
            }
            adminAnswer(d.error(), d.leaderHint(), null);
            return new CertificateInventory(d.nowMs(), d.certificates());
        });
    }

    /**
     * The certificate requests, newest first (cluster admin, ADR 0092).
     *
     * @param nowMs the answering node's clock in milliseconds since the Unix epoch
     * @param requests the requests
     */
    public record CertificateRequests(long nowMs, List<Messages.CertificateRequestEntry> requests) {
        /** Creates the result. */
        public CertificateRequests {
            requests = List.copyOf(requests);
        }
    }

    /**
     * Asks the CA holder for a client certificate. A cluster admin asks in any name; anyone else only in their own,
     * and a new request replaces their pending one (ADR 0119). The request's id is the SHA-256 of {@code csr}, so the
     * same request sent twice is one request.
     *
     * @param principal the principal the certificate will name: its common name
     * @param csr a PKCS#10 signing request in DER form; the private key stays with the requester
     */
    public void requestCertificate(String principal, byte[] csr) {
        checkOpen();
        adminChange(new Request.RequestCertificate(principal, ByteString.copyOf(csr)));
    }

    /** The certificate requests, newest first: all of them for a cluster admin, the caller's own for anyone else. */
    public CertificateRequests describeCertificateRequests() {
        checkOpen();
        return retrying(() -> {
            Response r = callAdminRead(new Request.DescribeCertificateRequests());
            if (!(r instanceof Response.DescribeCertificateRequests d)) {
                throw Attempt.retry("unexpected response " + r);
            }
            adminAnswer(d.error(), d.leaderHint(), null);
            return new CertificateRequests(d.nowMs(), d.requests());
        });
    }

    /**
     * Records the certificate the CA holder signed for a request (cluster admin).
     *
     * @param id the request it answers: SHA-256 of its signing request
     * @param certificate the signed certificate, DER
     * @param issuer SHA-256 of the issuer's DER name and authority key identifier (ADR 0096), 32 bytes
     * @param serial the serial number, big-endian
     * @param notBeforeMs first millisecond it is valid in
     * @param notAfterMs millisecond it stops being valid in
     */
    public void recordCertificate(
            byte[] id, byte[] certificate, byte[] issuer, byte[] serial, long notBeforeMs, long notAfterMs) {
        checkOpen();
        adminChange(new Request.RecordCertificate(
                ByteString.copyOf(id),
                ByteString.copyOf(certificate),
                ByteString.copyOf(issuer),
                ByteString.copyOf(serial),
                notBeforeMs,
                notAfterMs));
    }

    /** Creates a tenant. */
    public void createTenant(String name) {
        checkOpen();
        adminChange(new Request.CreateTenant(name));
    }

    /** Deletes a tenant that holds no resources, and its role bindings. */
    public void deleteTenant(String name) {
        checkOpen();
        adminChange(new Request.DeleteTenant(name));
    }

    /** The tenants this client has a role in; all of them for a cluster admin. */
    public List<Messages.TenantEntry> describeTenants() {
        checkOpen();
        return retrying(() -> {
            Response r = callAdminRead(new Request.DescribeTenants());
            if (!(r instanceof Response.DescribeTenants d)) {
                throw Attempt.retry("unexpected response " + r);
            }
            adminAnswer(d.error(), d.leaderHint(), null);
            return d.tenants();
        });
    }

    /** Adds and removes role bindings; removals first. */
    public void alterRoleBindings(List<Messages.RoleBinding> add, List<Messages.RoleBinding> remove) {
        checkOpen();
        adminChange(new Request.AlterRoleBindings(add, remove));
    }

    /** Every role binding, each with where it comes from ({@link Messages.ConfigSource}). */
    public List<Messages.SourcedBinding> describeRoleBindings() {
        checkOpen();
        return retrying(() -> {
            Response r = callAdminRead(new Request.DescribeRoleBindings());
            if (!(r instanceof Response.DescribeRoleBindings d)) {
                throw Attempt.retry("unexpected response " + r);
            }
            adminAnswer(d.error(), d.leaderHint(), null);
            return d.bindings();
        });
    }

    /** Who this client acts as, and the role bindings that apply. */
    public Response.WhoAmI whoAmI() {
        checkOpen();
        return retrying(() -> {
            Response r = callAdmin(new Request.WhoAmI());
            if (!(r instanceof Response.WhoAmI w)) {
                throw Attempt.retry("unexpected response " + r);
            }
            return w;
        });
    }

    /**
     * Revokes the sessions of a user, or of every member of an IdP group (ADR 0070): each node ends their sessions
     * that authenticated before now, and refuses their tokens issued before now. A SCRAM user who still has the
     * password can sign in again; change or delete the password to keep them out. Needs the cluster-admin role.
     */
    public void revokeSessions(String subject, boolean group) {
        checkOpen();
        int kind = group ? Messages.SubjectKind.GROUP : Messages.SubjectKind.USER;
        adminChange(new Request.RevokeSessions(kind, subject, 0));
    }

    /**
     * Creates an access token (ADR 0133) for {@code owner}, or for the caller with {@code null}; only a cluster admin
     * names someone else. The secret is drawn here and only its SHA-256 is sent, so the returned text is the only
     * place it exists: keep it, nobody can show it again. A retry after a lost answer may have made a token the
     * caller never sees; {@link #describeTokens} lists it.
     *
     * @throws ClientException.Broker with {@code UNAUTHORIZED} on a session signed in with a token or for another
     *     owner without the cluster-admin role, {@code INVALID_REQUEST} for a bad name or a full owner or cluster,
     *     {@code UNSUPPORTED_VERSION} below cluster version 0.1; the message says which
     */
    public CreatedToken createToken(String name, String owner, TokenLifetime lifetime) {
        checkOpen();
        java.util.Objects.requireNonNull(name, "name");
        java.util.Objects.requireNonNull(lifetime, "lifetime");
        byte[] secret = new byte[32];
        new SecureRandom().nextBytes(secret);
        // The id is the cluster's, so the text is built from its answer; the hash does not depend on the id.
        byte[] hash = new AccessToken(0, secret).hash();
        Request req = new Request.CreateToken(
                name, owner == null ? "" : owner, lifetime.code(), ByteString.copyOf(hash));
        Response.CreateToken r = retrying(() -> {
            Response resp = callAdmin(req);
            if (!(resp instanceof Response.CreateToken c)) {
                throw Attempt.retry("unexpected response " + resp);
            }
            adminAnswer(c.error(), c.leaderHint(), c.errorMsg());
            return c;
        });
        String text = new AccessToken(r.id(), secret).text();
        java.util.Arrays.fill(secret, (byte) 0);
        return new CreatedToken(r.id(), text, r.expiresAtMs());
    }

    /**
     * Lists access tokens (ADR 0133): the caller's own, or with {@code owner} that user's, which only a cluster admin
     * or viewer may see; {@code null} lists all the caller may see. Only what describes them: no secret is kept
     * anywhere to list. Below cluster version 0.1 the list is empty.
     */
    public List<Messages.TokenEntry> describeTokens(String owner) {
        checkOpen();
        Request req = new Request.DescribeTokens(owner);
        return retrying(() -> {
            Response r = callAdminRead(req);
            if (!(r instanceof Response.DescribeTokens d)) {
                throw Attempt.retry("unexpected response " + r);
            }
            adminAnswer(d.error(), d.leaderHint(), null);
            return d.tokens();
        });
    }

    /**
     * Revokes an access token (ADR 0133): every node ends the sessions it opened and refuses it from then on. A user
     * revokes their own tokens, a cluster admin anyone's.
     *
     * @throws ClientException.Broker with {@code UNKNOWN_RESOURCE} for a token that is not the caller's to revoke,
     *     does not exist or has expired
     */
    public void revokeToken(long id) {
        checkOpen();
        adminChange(new Request.RevokeToken(id));
    }

    /**
     * Pauses ({@code true}) or resumes ({@code false}) the cluster's restart turns (ADR 0127): while paused, no node
     * takes the next turn, and the one that holds it keeps it. Needs the cluster-admin role.
     */
    public void holdUpgrade(boolean hold) {
        checkOpen();
        adminChange(new Request.AlterUpgrade(hold, null, null));
    }

    /**
     * Gives back the restart turn {@code node} holds, for a node that will not come back to give it back itself (ADR
     * 0127). Nothing happens if the node holds no turn. Needs the cluster-admin role.
     */
    public void releaseTurn(long node) {
        checkOpen();
        adminChange(new Request.AlterUpgrade(null, node, null));
    }

    /**
     * Raises the cluster version to {@code major.minor} (ADR 0127), once every node runs a release that supports it;
     * the metadata leader refuses it, naming the nodes that do not, otherwise. The version is never lowered. Needs
     * the cluster-admin role.
     */
    public void raiseClusterVersion(int major, int minor) {
        checkOpen();
        if (major < 0 || major > 0xFFFF || minor < 0 || minor > 0xFFFF) {
            throw new IllegalArgumentException("a cluster version's major and minor are 0 to 65535");
        }
        adminChange(new Request.AlterUpgrade(null, null, ((long) major << 16) | minor));
    }

    /**
     * Where a rolling upgrade stands (ADR 0127): the cluster version, the restart turn, the pause and each node's
     * versions. Needs the cluster-admin or cluster-viewer role.
     */
    public Response.DescribeUpgrade describeUpgrade() {
        checkOpen();
        return retrying(() -> {
            Response r = callAdminRead(new Request.DescribeUpgrade());
            if (!(r instanceof Response.DescribeUpgrade d)) {
                throw Attempt.retry("unexpected response " + r);
            }
            adminAnswer(d.error(), d.leaderHint(), null);
            return d;
        });
    }

    /**
     * Adds a certificate to the cluster's revocation list: every node refuses it from then on and ends its
     * connections. Needs the cluster-admin role.
     *
     * @param issuer SHA-256 of the issuer's DER name and authority key identifier (ADR 0096), 32 bytes
     * @param serial the serial number, big-endian
     * @param notAfterMs milliseconds since the Unix epoch at which the certificate stops being valid; the entry is
     *     dropped after that
     * @param reason why, for whoever reads the list later; may be empty
     */
    public void revokeCertificate(byte[] issuer, byte[] serial, long notAfterMs, String reason) {
        checkOpen();
        adminChange(new Request.RevokeCertificate(
                ByteString.copyOf(issuer), ByteString.copyOf(serial), notAfterMs, reason));
    }

    /**
     * Renews the client certificate this client's connections are made with (ADR 0123): makes a new P-256 key, has
     * the metadata leader sign a certificate for it naming the same principal, and returns the key and chain. The
     * certificate must be in its renewal window: its last 30 days, or past two thirds of its validity
     * ({@link CertificateRenewal#mayRenew}). A retry after a lost answer renews again; the earlier renewal then stops
     * working, so use only the one returned.
     *
     * @throws ClientException.Broker with {@code INVALID_REQUEST} outside the window, without a client certificate,
     *     or if the cluster has no renewal CA
     */
    public RenewedCertificate renewCertificate() {
        checkOpen();
        CertificateRenewal.NewKey key = CertificateRenewal.newKey();
        Request req = new Request.RenewCertificate(ByteString.copyOf(key.csr()));
        Response.RenewCertificate r = retrying(() -> {
            Response resp = callAdmin(req);
            if (!(resp instanceof Response.RenewCertificate c)) {
                throw Attempt.retry("unexpected response " + resp);
            }
            adminAnswer(c.error(), c.leaderHint(), c.errorMsg());
            return c;
        });
        List<byte[]> chain = new ArrayList<>();
        for (ByteString c : r.chain()) {
            chain.add(c.toByteArray());
        }
        return new RenewedCertificate(key.keyPem(), CertificateRenewal.chainPem(chain), r.notAfterMs());
    }

    /**
     * Renews as {@link #renewCertificate()} does and writes the new key, then the new chain, over the files (each
     * moved into place whole). A {@link dev.lotse.net.TlsConfig} built from these files uses them from the next
     * connection on.
     *
     * @throws java.io.UncheckedIOException if a file cannot be written; the renewed certificate is then lost and the
     *     old one still works, so renew again
     */
    public RenewedCertificate renewCertificate(Path certPem, Path keyPem) {
        RenewedCertificate renewed = renewCertificate();
        try {
            CertificateRenewal.write(renewed, certPem, keyPem);
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException("cannot write the renewed certificate: " + e.getMessage(), e);
        }
        return renewed;
    }

    /** Why the last automatic renewal failed, or {@code null} if none has since one succeeded. */
    public Exception renewalFailure() {
        return renewalFailure;
    }

    private volatile Exception renewalFailure;

    /**
     * With {@link ClientConfig.Builder#renewCertificate}: looks at the certificate file now and every
     * {@link ClientConfig#renewCheck()}, and renews it once its window is open. A failure is tried again a minute
     * later.
     */
    private void keepRenewed() {
        Path cert = cfg.renewCertificateFiles().get(0);
        Path key = cfg.renewCertificateFiles().get(1);
        while (!closed) {
            long wait = cfg.renewCheck().toMillis();
            try {
                if (CertificateRenewal.due(cert, System.currentTimeMillis())) {
                    renewCertificate(cert, key);
                }
                renewalFailure = null;
            } catch (RuntimeException | java.io.IOException e) {
                renewalFailure = e;
                wait = Math.min(wait, 60_000);
            }
            try {
                Thread.sleep(wait);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /**
     * Adds node {@code nodeId} to the cluster, reached by nodes at {@code peer} and by clients at {@code client};
     * start it with {@code join} afterwards. Ids are never used twice.
     */
    public void addNode(long nodeId, String peer, String client) {
        checkOpen();
        adminChange(new Request.AddNode(nodeId, peer, client));
    }

    /**
     * Starts draining a node: its replicas move to other nodes, then it leaves. {@link #describeNodes} shows how
     * far.
     */
    public void removeNode(long nodeId) {
        checkOpen();
        adminChange(new Request.RemoveNode(nodeId));
    }

    /** The cluster's nodes, as the metadata group knows them. */
    public List<Messages.NodeEntry> describeNodes() {
        checkOpen();
        return retrying(() -> {
            Response r = callAdminRead(new Request.DescribeNodes());
            if (!(r instanceof Response.DescribeNodes d)) {
                throw Attempt.retry("unexpected response " + r);
            }
            adminAnswer(d.error(), d.leaderHint(), null);
            return d.nodes();
        });
    }

    /**
     * Pins the leader of {@code partition} of the stream or queue {@code name} to {@code node}, one of its voters, or
     * unpins it with {@code null} so that leaders spread by themselves again (ADR 0065). The group's leader hands over
     * shortly after; the pin stays until it is removed or the node leaves. Needs the cluster-admin role.
     */
    public void moveLeader(String name, int partition, Long node) {
        checkOpen();
        long id = resourceId(name);
        adminChange(new Request.MoveLeader(id, partition, node == null ? 0 : node));
    }

    /**
     * Places {@code partition} of the stream or queue {@code name} on the nodes {@code voters}, as Kafka's partition
     * reassignment does, or releases it with no nodes so the cluster manages it again (ADR 0115). The partition's
     * leader moves its replicas shortly after; rebalancing then leaves it where it was placed, and only a drain moves
     * it. Refused while the partition is moving. Needs the cluster-admin role. To change how many replicas every
     * partition has, set {@code replication.factor} with {@link #alterConfigs}.
     */
    public void reassignPartition(String name, int partition, List<Long> voters) {
        checkOpen();
        long id = resourceId(name);
        adminChange(new Request.ReassignPartition(id, partition, voters));
    }

    /**
     * Raises the partition count of the stream {@code name}, created at runtime, to {@code count} (ADR 0074).
     * Existing partitions stay as they are, so a key may map to another partition afterwards; consumer groups spread
     * over the new ones on their next heartbeat. Needs the tenant-admin role.
     */
    public void createPartitions(String name, int count) {
        checkOpen();
        long id = resourceId(name);
        adminChange(new Request.CreatePartitions(id, count));
    }

    /**
     * Every node's metrics, asked of each node directly, for every node the cluster's metadata names: each with its
     * answer, or why there is none. Counters grow from each node's start; see {@code DescribeMetrics}.
     */
    public List<NodeMetrics> nodeMetrics() {
        checkOpen();
        Response.Metadata m = metadata();
        List<NodeMetrics> out = new ArrayList<>();
        for (Messages.Broker b : m.brokers()) {
            Address addr = new Address(b.host(), b.port());
            try {
                Response r = call(addr, new Request.DescribeMetrics());
                if (r instanceof Response.DescribeMetrics d && d.error().isOk()) {
                    out.add(new NodeMetrics(b.nodeId(), d, null));
                } else if (r instanceof Response.DescribeMetrics d) {
                    out.add(new NodeMetrics(b.nodeId(), null, new ClientException.Broker(d.error(), null)));
                } else {
                    out.add(new NodeMetrics(
                            b.nodeId(), null, new ClientException.NoAnswer(b.nodeId(), "unexpected response " + r)));
                }
            } catch (CallFailure e) {
                out.add(new NodeMetrics(
                        b.nodeId(), null, new ClientException.NoAnswer(b.nodeId(), addr + ": " + e.describe())));
            }
        }
        return out;
    }

    /**
     * Every node's open client sessions, asked of each node directly, for every node the cluster's metadata names:
     * each with its answer, or why there is none (ADR 0121). A cluster admin or viewer gets every session, anyone
     * else those made with their own name.
     */
    public List<NodeSessions> nodeSessions() {
        checkOpen();
        Response.Metadata m = metadata();
        List<NodeSessions> out = new ArrayList<>();
        for (Messages.Broker b : m.brokers()) {
            Address addr = new Address(b.host(), b.port());
            try {
                Response r = call(addr, new Request.DescribeSessions());
                if (r instanceof Response.DescribeSessions d && d.error().isOk()) {
                    out.add(new NodeSessions(b.nodeId(), d, null));
                } else if (r instanceof Response.DescribeSessions d) {
                    out.add(new NodeSessions(b.nodeId(), null, new ClientException.Broker(d.error(), null)));
                } else {
                    out.add(new NodeSessions(
                            b.nodeId(), null, new ClientException.NoAnswer(b.nodeId(), "unexpected response " + r)));
                }
            } catch (CallFailure e) {
                out.add(new NodeSessions(
                        b.nodeId(), null, new ClientException.NoAnswer(b.nodeId(), addr + ": " + e.describe())));
            }
        }
        return out;
    }

    /** The id of the resource called {@code name}, asking every broker before calling it unknown. */
    private long resourceId(String name) {
        ResourceMetadata r = find(metadataFromAll(name), name);
        if (r == null) {
            throw new ClientException.UnknownStream(name, 0);
        }
        return r.resourceId();
    }

    /** An admin change answered with {@code AdminResponse}. */
    private void adminChange(Request req) {
        retrying(() -> {
            Response r = callAdmin(req);
            if (!(r instanceof Response.Admin a)) {
                throw Attempt.retry("unexpected response " + r);
            }
            adminAnswer(a.error(), a.leaderHint(), a.errorMsg());
            return null;
        });
    }

    /** An admin answer: retriable errors follow the leader hint and may have been carried out; others fail. */
    private void adminAnswer(ErrorCode error, LeaderHint hint, String message) {
        if (error.isOk()) {
            return;
        }
        if (error.retriable()) {
            metadataLeader = hint == null ? null : hint.leader();
            throw Attempt.retryMaybeDone(error.toString());
        }
        throw Attempt.fail(new ClientException.Broker(error, message));
    }

    /**
     * Learns the address of node {@code node} from the first broker whose metadata lists it. Not from the first broker
     * that answers: one that lags (cut off from the metadata group) may not know the leader the others name, and would
     * answer first every time. Only that address is taken; what else the answer says is left alone.
     */
    private void findBroker(long node) {
        Set<Address> sorted = new TreeSet<>(java.util.Comparator.comparing(Address::toString));
        sorted.addAll(candidates());
        for (Address addr : sorted) {
            try {
                if (call(addr, new Request.Metadata(List.of())) instanceof Response.Metadata m) {
                    for (Messages.Broker b : m.brokers()) {
                        if (b.nodeId() == node) {
                            synchronized (state) {
                                brokers.put(node, new Address(b.host(), b.port()));
                            }
                            return;
                        }
                    }
                }
            } catch (CallFailure e) {
                // a broker that does not answer knows nothing
            }
        }
    }

    /**
     * An admin read (one of the zone reads): to the lowest-id node in this client's zone that the latest metadata
     * listed, which answers once it has applied what this client saw (the zone reads design). A refusal, an error or a
     * failed call sends that one read to the metadata leader, whom a refusal may name, with the same indexes: a leader
     * deposed without knowing it refuses a read it has not applied (ADR 0141), so the retry finds the new one. The
     * next read tries the zone again.
     */
    private Response callAdminRead(Request req) {
        String zone = cfg.zone();
        List<GroupIndex> atLeast = List.of();
        if (zone != null) {
            atLeast = indexes.snapshot();
            Address addr;
            synchronized (state) {
                Long node = Zone.zoneNode(zone, listed, zones);
                addr = node == null ? null : brokers.get(node);
            }
            if (addr != null) {
                try {
                    Response resp = call(addr, req, atLeast);
                    if (!Zone.refused(resp)) {
                        return resp;
                    }
                    LeaderHint hint = Zone.leaderHint(resp);
                    if (hint != null) {
                        metadataLeader = hint.leader();
                    }
                } catch (CallFailure e) {
                    // the leader answers instead
                }
            }
        }
        return callAdmin(req, atLeast);
    }

    /** Sends an admin request to the metadata leader, or any broker if it is not known. */
    private Response callAdmin(Request req) {
        return callAdmin(req, List.of());
    }

    /** {@link #callAdmin(Request)} with the request header's {@code read_at_least}; empty, no tag. */
    private Response callAdmin(Request req, List<GroupIndex> readAtLeast) {
        Long leader = metadataLeader;
        // A leader whose address this client has not seen (a node added since it last asked): learn the cluster's
        // nodes first, or it keeps asking the others. If the broker that answers does not know it either (one that
        // lags), ask the others for its address.
        if (leader != null && addressOf(leader) == null) {
            try {
                metadata();
            } catch (ClientException e) {
                // the retry loop reports what is still wrong
            }
            if (addressOf(leader) == null) {
                findBroker(leader);
            }
        }
        Address addr = leader == null ? null : addressOf(leader);
        if (addr == null) {
            List<Address> targets = knownBrokers();
            if (targets.isEmpty()) {
                targets = cfg.bootstrap();
            }
            addr = targets.get(Math.floorMod(adminTurn.getAndIncrement(), targets.size()));
        }
        try {
            return call(addr, req, readAtLeast);
        } catch (CallFailure e) {
            if (e.kind == CallFailure.Kind.DENIED || e.kind == CallFailure.Kind.REFUSED) {
                throw e.toAttempt();
            }
            metadataLeader = null;
            // The request may have arrived: a retry finds what it did.
            throw Attempt.retryMaybeDone(e.describe());
        }
    }

    private Address addressOf(long node) {
        synchronized (state) {
            return brokers.get(node);
        }
    }

    private List<Address> knownBrokers() {
        synchronized (state) {
            return new ArrayList<>(brokers.values());
        }
    }

    // ---- one request to one broker ----

    /**
     * Sends one request to {@code addr} and waits for its response. A connection that failed once is not trusted
     * again: it is closed and the next call opens a new one.
     *
     * @throws CallFailure when there is no response
     */
    Response call(Address addr, Request req) {
        return callOn(connect(addr), req);
    }

    /**
     * {@link #call} with the request header's {@code read_at_least}: a zone read the node may answer only once it has
     * applied these indexes.
     */
    Response call(Address addr, Request req, List<GroupIndex> readAtLeast) {
        return callOn(connect(addr), req, readAtLeast);
    }

    /**
     * The pooled connection to {@code addr}, logged in.
     *
     * @throws CallFailure when there is none
     */
    Connection connect(Address addr) {
        checkOpen();
        Connection c;
        try {
            c = pool.get(addr);
        } catch (InsecureTransportException e) {
            throw e;
        } catch (AuthenticationException e) {
            CallFailure f = CallFailure.denied(e.getMessage());
            noteFatal(f);
            throw f;
        } catch (ConnectionException e) {
            throw CallFailure.notSent(e.getMessage());
        }
        if (seen.add(c)) {
            // A new connection logged in (if it has credentials): an earlier failed login no longer holds.
            fatal.set(null);
        }
        return c;
    }

    /**
     * Sends one request on a given connection and waits for its response; failures are handled as in
     * {@link #call}.
     *
     * @throws CallFailure when there is no response
     */
    Response callOn(Connection c, Request req) {
        return callOn(c, req, List.of());
    }

    private Response callOn(Connection c, Request req, List<GroupIndex> readAtLeast) {
        try {
            return c.call(req, readAtLeast);
        } catch (BrokerException e) {
            pool.discard(c);
            // Not authenticated, or the session ended: sending again with the same credentials gets the same
            // answer. Refused as a whole with a reason: not carried out, and the same request would be refused again.
            CallFailure f;
            if (e.code().equals(ErrorCode.UNAUTHORIZED) && e.reason() == null) {
                f = CallFailure.denied("the node requires authentication");
            } else if (e.reason() == null) {
                f = CallFailure.notSent("refused: " + e.code());
            } else {
                f = CallFailure.refused(e.code(), e.reason());
            }
            noteFatal(f);
            throw f;
        } catch (ConnectionException e) {
            pool.discard(c);
            throw e.requestSent() ? CallFailure.lost(e.getMessage()) : CallFailure.notSent(e.getMessage());
        }
    }

    /**
     * Keeps a failed login for the retry loop to report: it is the client's credentials that fail, so it holds for
     * every request, also one whose attempt only saw a metadata lookup fail. A refused request is not kept: its own
     * caller gets the refusal, and keeping it would report it for another request.
     */
    private void noteFatal(CallFailure f) {
        if (f.kind == CallFailure.Kind.DENIED) {
            fatal.set(new ClientException.Authentication(f.getMessage()));
        }
    }

    private void checkOpen() {
        if (closed) {
            throw new IllegalStateException("the client is closed");
        }
    }

    // ---- for tests in this package ----

    /** The route this client holds for a partition, or {@code null}. */
    Route routeOf(String stream, int partition) {
        synchronized (state) {
            return routes.get(new PartitionKey(stream, partition));
        }
    }

    /** The producer id of the current session, or {@code 0} if there is none. */
    long producerId() {
        synchronized (sessionLock) {
            return session == null ? 0 : session.producerId;
        }
    }
}
