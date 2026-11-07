package dev.lotse.client;

import dev.lotse.proto.ErrorCode;
import dev.lotse.proto.Messages;
import dev.lotse.proto.Messages.OffsetCommitPartition;
import dev.lotse.proto.Messages.OffsetCommitResource;
import dev.lotse.proto.Messages.TxnResource;
import dev.lotse.proto.Request;
import dev.lotse.proto.Response;
import dev.lotse.record.Batch;
import dev.lotse.record.BatchRecord;
import dev.lotse.record.RecordFormatException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * A member of a consumer group, from {@link LotseClient#joinGroup} (ADR 0033). The coordinator assigns partitions;
 * the member reads them with read_committed from the group's committed offsets. It follows the Rust client's {@code
 * GroupConsumer} rule for rule:
 *
 * <ul>
 *   <li>it heartbeats only from {@link #poll}, when one is due: call {@code poll} more often than the session
 *       timeout, or the member loses its partitions;
 *   <li>a revoked partition is no longer read from the moment the answer that revokes it arrives; with auto-commit its
 *       position is committed, then the next heartbeat, at once, reports it released, and only then may another
 *       member get it;
 *   <li>when no heartbeat has succeeded for the session timeout, counted from when the last successful one was sent,
 *       every partition is dropped at once, since the coordinator may give them to others by then, and the member
 *       joins again as a new member;
 *   <li>a partition whose committed offset retention has deleted goes on from the oldest record left.
 * </ul>
 *
 * <p>Not for use by several threads at once.
 */
public final class GroupConsumer implements AutoCloseable {
    record Part(String stream, int partition) {}

    private static final Comparator<Part> ORDER =
            Comparator.comparing(Part::stream).thenComparingInt(Part::partition);

    private final LotseClient client;
    private final String group;
    private final List<String> streams;
    /** Subscribed by pattern instead (ADR 0073), or {@code null}. */
    private final String pattern;
    private final GroupConfig cfg;
    private String memberId = "";
    private int epoch;
    /** Owned partitions and the next offset to read; {@code null} until the committed offset is known. */
    private final TreeMap<Part, Long> owned = new TreeMap<>(ORDER);
    /** {@link System#nanoTime} when the last heartbeat that succeeded was sent, or {@code null}. */
    private Long lastOk;
    private long nextHeartbeat = System.nanoTime();
    /** Read the partitions round robin. */
    private int turn;
    /** A newer instance with the same instance id took this member's place (ADR 0072): it stays out. */
    private boolean fenced;
    private boolean closed;

    GroupConsumer(LotseClient client, String group, List<String> streams, String pattern, GroupConfig cfg) {
        this.client = client;
        this.group = Objects.requireNonNull(group);
        this.streams = streams;
        this.pattern = pattern;
        this.cfg = Objects.requireNonNull(cfg);
    }

    private static ClientException fencedInstance() {
        return new ClientException.Broker(
                ErrorCode.FENCED_INSTANCE_ID, "another member joined with this instance id");
    }

    String group() {
        return group;
    }

    /** The member id, empty until joined. */
    public String memberId() {
        return memberId;
    }

    /** The member epoch. */
    public int memberEpoch() {
        return epoch;
    }

    /** The partitions this member owns now, as {@code stream -> partitions}. */
    public Map<String, TreeSet<Integer>> assignment() {
        Map<String, TreeSet<Integer>> out = new TreeMap<>();
        for (Part p : owned.keySet()) {
            out.computeIfAbsent(p.stream(), s -> new TreeSet<>()).add(p.partition());
        }
        return out;
    }

    /** The next offset to read per owned partition whose position is known, as {@code stream -> partition -> offset}. */
    public Map<String, Map<Integer, Long>> positions() {
        Map<String, Map<Integer, Long>> out = new TreeMap<>();
        for (Map.Entry<Part, Long> e : positionMap().entrySet()) {
            out.computeIfAbsent(e.getKey().stream(), s -> new TreeMap<>()).put(e.getKey().partition(), e.getValue());
        }
        return out;
    }

    TreeMap<Part, Long> positionMap() {
        TreeMap<Part, Long> out = new TreeMap<>(ORDER);
        for (Map.Entry<Part, Long> e : owned.entrySet()) {
            if (e.getValue() != null) {
                out.put(e.getKey(), e.getValue());
            }
        }
        return out;
    }

    /**
     * Heartbeats when one is due, then reads what one owned partition has, waiting up to {@code wait}. Returns the
     * records in offset order; empty if there is nothing yet.
     *
     * @throws ClientException.Broker with {@code FENCED_INSTANCE_ID} once a newer instance took this member's place
     */
    public List<GroupRecord> poll(Duration wait) {
        if (closed) {
            throw new IllegalStateException("the consumer is closed");
        }
        if (fenced) {
            throw fencedInstance();
        }
        long now = System.nanoTime();
        if (lastOk != null && now - lastOk >= cfg.sessionTimeout().toNanos()) {
            // The coordinator may have given everything away by now.
            lost();
        }
        if (now - nextHeartbeat >= 0 || memberId.isEmpty()) {
            heartbeat();
        }
        loadPositions();
        List<Map.Entry<Part, Long>> ready = new ArrayList<>(positionMap().entrySet());
        long untilHeartbeat = Math.max(0, nextHeartbeat - now);
        if (ready.isEmpty()) {
            long pause = Math.min(Math.max(0, nextHeartbeat - System.nanoTime()), wait.toNanos());
            sleep(Math.max(pause, 1_000_000));
            return List.of();
        }
        turn++;
        Map.Entry<Part, Long> pick = ready.get(Math.floorMod(turn, ready.size()));
        Part part = pick.getKey();
        long offset = pick.getValue();
        Duration fetchWait = Duration.ofNanos(Math.max(Math.min(wait.toNanos(), untilHeartbeat), 1_000_000));
        Fetched f;
        try {
            f = client.fetch(part.stream(), part.partition(), offset, fetchWait);
        } catch (ClientException.GaveUp e) {
            return List.of();
        } catch (ClientException.OffsetOutOfRange e) {
            // Retention deleted what the group had not read: go on from the oldest record left, as Kafka's
            // earliest reset does.
            if (Long.compareUnsigned(e.logStart(), offset) <= 0) {
                throw e;
            }
            if (Objects.equals(owned.get(part), offset)) {
                owned.put(part, e.logStart());
            }
            return List.of();
        }
        if (lastOk == null || System.nanoTime() - lastOk >= cfg.sessionTimeout().toNanos()) {
            lost();
            return List.of();
        }
        // Revoked or lost meanwhile: nothing of it is handed out.
        if (!Objects.equals(owned.get(part), offset)) {
            return List.of();
        }
        List<GroupRecord> out = new ArrayList<>();
        for (Batch b : f.committedBatches()) {
            List<BatchRecord> records;
            try {
                records = b.records();
            } catch (RecordFormatException e) {
                throw new ClientException.Broker(ErrorCode.CORRUPT_BATCH, e.getMessage());
            }
            for (BatchRecord r : records) {
                out.add(new GroupRecord(part.stream(), part.partition(), r));
            }
        }
        owned.put(part, f.nextOffset(offset));
        return out;
    }

    private static void sleep(long nanos) {
        try {
            Thread.sleep(nanos / 1_000_000, (int) (nanos % 1_000_000));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Everything is gone: no commits, join again as a new member. */
    private void lost() {
        owned.clear();
        memberId = "";
        epoch = 0;
        lastOk = null;
    }

    /** One heartbeat. Only what retrying cannot fix (a refusal, a login that fails) is an error. */
    private void heartbeat() {
        long sent = System.nanoTime();
        boolean joining = memberId.isEmpty();
        List<Long> ids = new ArrayList<>();
        for (String s : streams) {
            Long id = client.streamId(s);
            if (id != null) {
                ids.add(id);
            }
        }
        TreeMap<Long, List<Long>> byStream = new TreeMap<>(Long::compareUnsigned);
        for (Part p : owned.keySet()) {
            Long id = client.streamId(p.stream());
            if (id != null) {
                byStream.computeIfAbsent(id, k -> new ArrayList<>()).add((long) p.partition());
            }
        }
        List<TxnResource> ownedNow = new ArrayList<>();
        byStream.forEach((id, parts) -> ownedNow.add(new TxnResource(id, parts)));
        Request req = new Request.GroupHeartbeat(
                group,
                memberId,
                joining ? 0 : epoch,
                Math.min(cfg.rebalanceTimeout().toMillis(), 0xFFFF_FFFFL),
                joining && pattern == null ? ids : null,
                ownedNow,
                client.config().tenant(),
                cfg.instanceId(),
                joining ? pattern : null);
        long retrySoon = sent + client.config().backoff().toNanos();
        Response.GroupHeartbeat resp;
        try {
            if (!(client.callCoordinator(group, req) instanceof Response.GroupHeartbeat r)) {
                client.forgetCoordinator();
                nextHeartbeat = retrySoon;
                return;
            }
            resp = r;
        } catch (Attempt a) {
            if (a.kind == Attempt.Kind.FAIL) {
                throw a.error;
            }
            client.forgetCoordinator();
            nextHeartbeat = retrySoon;
            return;
        }
        ErrorCode code = resp.error();
        if (code.equals(ErrorCode.UNKNOWN_MEMBER_ID) || code.equals(ErrorCode.FENCED_MEMBER_EPOCH)) {
            lost();
            nextHeartbeat = retrySoon;
            return;
        }
        if (code.equals(ErrorCode.FENCED_INSTANCE_ID)) {
            // A newer instance with this instance id took the place: stop, rather than join again and take it back.
            lost();
            fenced = true;
            throw fencedInstance();
        }
        if (!code.isOk()) {
            if (code.retriable()) {
                client.forgetCoordinator();
            }
            nextHeartbeat = retrySoon;
            return;
        }
        lastOk = sent;
        memberId = resp.memberId();
        epoch = resp.memberEpoch();
        long interval = Math.min(
                Duration.ofMillis(resp.heartbeatIntervalMs()).toNanos(), cfg.sessionTimeout().toNanos() / 3);
        nextHeartbeat = sent + interval;
        List<TxnResource> assignment = resp.assignment();
        if (assignment == null) {
            return;
        }
        // A pattern may bring streams this client has not looked up yet.
        for (TxnResource r : assignment) {
            if (client.streamName(r.resourceId()) == null) {
                try {
                    client.metadata();
                } catch (ClientException e) {
                    // the names come with a later heartbeat
                }
                break;
            }
        }
        TreeSet<Part> target = new TreeSet<>(ORDER);
        for (TxnResource r : assignment) {
            String name = client.streamName(r.resourceId());
            if (name == null) {
                continue;
            }
            for (long p : r.partitions()) {
                target.add(new Part(name, (int) p));
            }
        }
        List<Part> revoked = new ArrayList<>();
        for (Part p : owned.keySet()) {
            if (!target.contains(p)) {
                revoked.add(p);
            }
        }
        if (!revoked.isEmpty()) {
            // Stop reading them now; commit where they got to, then report them released with the next heartbeat,
            // which comes at once.
            TreeMap<Part, Long> positions = new TreeMap<>(ORDER);
            for (Part p : revoked) {
                Long at = owned.remove(p);
                if (at != null) {
                    positions.put(p, at);
                }
            }
            if (cfg.autoCommit() && !positions.isEmpty()) {
                try {
                    commitPositions(positions);
                } catch (ClientException e) {
                    // the next owner starts from the last commit
                }
            }
            nextHeartbeat = System.nanoTime();
        }
        for (Part p : target) {
            owned.putIfAbsent(p, null);
        }
    }

    /**
     * Learns the committed offsets of partitions whose position is not known yet; a partition whose offset an open
     * transaction holds waits for it.
     */
    private void loadPositions() {
        TreeMap<Long, List<Long>> byStream = new TreeMap<>(Long::compareUnsigned);
        for (Map.Entry<Part, Long> e : owned.entrySet()) {
            if (e.getValue() != null) {
                continue;
            }
            Long id = client.streamId(e.getKey().stream());
            if (id != null) {
                byStream.computeIfAbsent(id, k -> new ArrayList<>()).add((long) e.getKey().partition());
            }
        }
        if (byStream.isEmpty()) {
            return;
        }
        List<TxnResource> resources = new ArrayList<>();
        byStream.forEach((id, parts) -> resources.add(new TxnResource(id, parts)));
        Request req = new Request.OffsetFetch(group, true, resources, client.config().tenant());
        Response.OffsetFetch resp;
        try {
            if (!(client.callCoordinator(group, req) instanceof Response.OffsetFetch r)) {
                client.forgetCoordinator();
                return;
            }
            resp = r;
        } catch (Attempt a) {
            client.forgetCoordinator();
            return;
        }
        for (Messages.OffsetFetchResource r : resp.resources()) {
            String name = client.streamName(r.resourceId());
            if (name == null) {
                continue;
            }
            for (Messages.OffsetFetchPartition p : r.partitions()) {
                if (!p.error().isOk()) {
                    continue;
                }
                long start;
                if (p.offset() != Messages.NO_OFFSET) {
                    start = p.offset();
                } else if (cfg.startFrom().equals(OffsetAt.EARLIEST)) {
                    // From the start: a position below the log start goes on from there anyway.
                    start = 0;
                } else {
                    try {
                        start = client.listOffset(name, (int) p.partition(), cfg.startFrom());
                    } catch (ClientException e) {
                        // asked again with the next fetch of positions
                        continue;
                    }
                }
                Part key = new Part(name, (int) p.partition());
                if (owned.containsKey(key)) {
                    owned.put(key, start);
                }
            }
        }
    }

    /** Commits the position of every owned partition whose position is known. */
    public void commit() {
        TreeMap<Part, Long> positions = positionMap();
        if (!positions.isEmpty()) {
            commitPositions(positions);
        }
    }

    private void commitPositions(TreeMap<Part, Long> positions) {
        Request req = new Request.CommitOffsets(
                group, memberId, epoch, resources(client, positions), client.config().tenant());
        client.retrying(() -> commitOnce(client, group, req));
    }

    /** The positions as commit resources, by stream id. */
    static List<OffsetCommitResource> resources(LotseClient client, TreeMap<Part, Long> positions) {
        TreeMap<Long, List<OffsetCommitPartition>> by = new TreeMap<>(Long::compareUnsigned);
        for (Map.Entry<Part, Long> e : positions.entrySet()) {
            Long id = client.streamId(e.getKey().stream());
            if (id != null) {
                by.computeIfAbsent(id, k -> new ArrayList<>())
                        .add(new OffsetCommitPartition(e.getKey().partition(), e.getValue(), 0, null));
            }
        }
        List<OffsetCommitResource> out = new ArrayList<>();
        by.forEach((id, parts) -> out.add(new OffsetCommitResource(id, parts)));
        return out;
    }

    /** One {@code CommitOffsets} or {@code AddOffsetsToTxn} to the coordinator, keyed by {@code key}. */
    static Void commitOnce(LotseClient client, String key, Request req) {
        if (!(client.callCoordinator(key, req) instanceof Response.CommitOffsets r)) {
            throw Attempt.retry("unexpected response");
        }
        for (Messages.TxnResourceResult res : r.resources()) {
            for (Messages.TxnPartitionResult p : res.partitions()) {
                ErrorCode code = p.error();
                if (code.isOk()) {
                    continue;
                }
                if (code.retriable()) {
                    client.forgetCoordinator();
                    throw Attempt.retryMaybeDone(code.toString());
                }
                throw Attempt.fail(new ClientException.Broker(code, null));
            }
        }
        return null;
    }

    /**
     * Forgets the read positions: every owned partition starts again from the group's committed offset, as after an
     * aborted transaction.
     */
    public void rewind() {
        owned.replaceAll((k, v) -> null);
    }

    /** Leaves the group, committing positions first when auto-commit is on. */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (cfg.autoCommit()) {
            try {
                commit();
            } catch (ClientException e) {
                // leaving matters more
            }
        }
        if (memberId.isEmpty()) {
            return;
        }
        Request req = new Request.GroupHeartbeat(
                group, memberId, -1, 0, null, null, client.config().tenant(), cfg.instanceId(), null);
        try {
            client.callCoordinator(group, req);
        } catch (Attempt | ClientException e) {
            // the coordinator drops the member after the session timeout
        }
        owned.clear();
    }
}
