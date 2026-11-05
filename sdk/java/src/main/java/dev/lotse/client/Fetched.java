package dev.lotse.client;

import dev.lotse.proto.ErrorCode;
import dev.lotse.proto.Messages.AbortedTxn;
import dev.lotse.record.Batch;
import dev.lotse.record.BatchRecord;
import dev.lotse.record.ByteString;
import dev.lotse.record.ControlBatches;
import dev.lotse.record.ControlMarker;
import dev.lotse.record.ControlType;
import dev.lotse.record.RecordFormatException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Records read from one partition.
 *
 * <p>{@link #records()} holds whole batches exactly as the broker sent them, control batches and aborted ones
 * included. A reader that wants read_committed semantics uses {@link #committedBatches()} or
 * {@link #committedMessages()}, which leave out what it must not see.
 */
public final class Fetched {
    private final ByteString records;
    private final long highWatermark;
    private final long lastStableOffset;
    private final long logStartOffset;
    private final List<AbortedTxn> aborted;

    /**
     * Creates a result.
     *
     * @param records whole record batches
     * @param highWatermark offsets below this are committed, a {@code u64}
     * @param lastStableOffset offsets below this are committed and in no open transaction, a {@code u64}
     * @param logStartOffset the first offset still in the partition, a {@code u64}
     * @param aborted aborted transactions with data in {@code records}
     */
    public Fetched(
            ByteString records, long highWatermark, long lastStableOffset, long logStartOffset, List<AbortedTxn> aborted) {
        this.records = records;
        this.highWatermark = highWatermark;
        this.lastStableOffset = lastStableOffset;
        this.logStartOffset = logStartOffset;
        this.aborted = List.copyOf(aborted);
    }

    /** Whole record batches, control batches and aborted ones included; may be empty. */
    public ByteString records() {
        return records;
    }

    /** Offsets below this are committed, a {@code u64}. */
    public long highWatermark() {
        return highWatermark;
    }

    /** Offsets below this are committed and in no open transaction; reads stop here. A {@code u64}. */
    public long lastStableOffset() {
        return lastStableOffset;
    }

    /** The first offset still in the partition, a {@code u64}. */
    public long logStartOffset() {
        return logStartOffset;
    }

    /** Aborted transactions with data in {@link #records()}: producer id and first offset. */
    public List<AbortedTxn> aborted() {
        return aborted;
    }

    /**
     * The offset to fetch next: after the last batch, or {@code from} if there is none. Reading stops at the first
     * batch that does not parse.
     */
    public long nextOffset(long from) {
        long next = from;
        ByteString rest = records;
        while (!rest.isEmpty()) {
            Batch.Split split;
            try {
                split = Batch.parse(rest);
            } catch (RecordFormatException e) {
                break;
            }
            next = split.batch().nextOffset();
            rest = split.rest();
        }
        return next;
    }

    /**
     * The data batches a read_committed consumer sees: control batches and batches of aborted transactions left
     * out. An aborted transaction's batches are those of its producer from its first offset up to the producer's
     * ABORT marker.
     *
     * @throws ClientException.Broker with {@code CORRUPT_BATCH} if the records do not parse
     */
    public List<Batch> committedBatches() {
        List<AbortedTxn> pending = new ArrayList<>(aborted);
        pending.sort(Comparator.comparing(AbortedTxn::firstOffset, Long::compareUnsigned));
        int next = 0;
        Set<Long> skipping = new HashSet<>();
        List<Batch> out = new ArrayList<>();
        ByteString rest = records;
        while (!rest.isEmpty()) {
            Batch.Split split;
            try {
                split = Batch.parse(rest);
            } catch (RecordFormatException e) {
                throw new ClientException.Broker(ErrorCode.CORRUPT_BATCH, e.getMessage());
            }
            Batch b = split.batch();
            rest = split.rest();
            while (next < pending.size() && Long.compareUnsigned(pending.get(next).firstOffset(), b.baseOffset()) <= 0) {
                skipping.add(pending.get(next).producerId());
                next++;
            }
            if (b.attributes().control()) {
                if (isAbortMarker(b)) {
                    skipping.remove(b.producerId());
                }
                continue;
            }
            if (b.attributes().transactional() && skipping.contains(b.producerId())) {
                continue;
            }
            out.add(b);
        }
        return out;
    }

    /**
     * The records of {@link #committedBatches()}, in offset order.
     *
     * @throws RecordFormatException of kind {@code UNSUPPORTED_COMPRESSION} if a batch is compressed with a codec
     *     whose library is not on the class path
     */
    public List<BatchRecord> committedMessages() {
        List<BatchRecord> out = new ArrayList<>();
        for (Batch b : committedBatches()) {
            try {
                out.addAll(b.records());
            } catch (RecordFormatException e) {
                // A codec library missing here is no damage: say which one to add (ADR 0061).
                if (e.kind() == RecordFormatException.Kind.UNSUPPORTED_COMPRESSION) {
                    throw e;
                }
                throw new ClientException.Broker(ErrorCode.CORRUPT_BATCH, e.getMessage());
            }
        }
        return out;
    }

    private static boolean isAbortMarker(Batch b) {
        try {
            ControlMarker marker = ControlBatches.readMarker(b);
            return marker.type() == ControlType.ABORT;
        } catch (RecordFormatException e) {
            return false; // not a transaction marker: Raft's or a group's own entry
        }
    }
}
