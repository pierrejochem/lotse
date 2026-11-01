package dev.lotse.record;

import java.util.List;

/** Building and reading control batches: transaction markers and the broker's own entries. */
public final class ControlBatches {
    private ControlBatches() {}

    /**
     * Builds a transaction marker for {@code producerId}, as the broker writes it: transactional and control,
     * key {@code 0 0 type_lo type_hi}, value the coordinator epoch as a little-endian {@code u32}.
     *
     * @param producerId the producer, a {@code u64}
     * @param producerEpoch its epoch, a {@code u16}
     * @param type commit or abort
     * @param coordinatorEpoch the coordinator's epoch, a {@code u32}
     * @param timestamp milliseconds since the epoch
     */
    public static ByteString marker(
            long producerId, int producerEpoch, ControlType type, long coordinatorEpoch, long timestamp) {
        if (!Unsigned.fitsU32(coordinatorEpoch)) {
            throw new IllegalArgumentException("coordinator epoch is not a u32: " + coordinatorEpoch);
        }
        BatchBuilder b = new BatchBuilder(new BatchOptions(
                new Attributes(Compression.NONE, true, true, BatchKind.DATA), producerId, producerEpoch, -1));
        byte[] key = new byte[4];
        Batch.putU16(key, 2, type.code());
        byte[] value = new byte[4];
        Batch.putU32(value, 0, coordinatorEpoch);
        b.push(timestamp, key, value, List.of());
        return b.finish();
    }

    /**
     * Classifies a control batch.
     *
     * @return the kind, or {@code null} for a data batch
     * @throws RecordFormatException if the control record is damaged or of an unknown type
     */
    public static ControlKind kind(Batch batch) {
        if (!batch.attributes().control()) {
            return null;
        }
        BatchRecord rec = firstRecord(batch);
        ByteString key = rec.key();
        if (key == null) {
            throw RecordFormatException.malformed("control record without key");
        }
        if (key.size() != 4) {
            throw RecordFormatException.malformed("control record has the wrong size");
        }
        ControlKind kind = ControlKind.fromCode(key.at(2) | key.at(3) << 8);
        if (kind == ControlKind.ABORT_MARKER || kind == ControlKind.COMMIT_MARKER) {
            ByteString value = rec.value() == null ? ByteString.EMPTY : rec.value();
            if (value.size() != 4) {
                throw RecordFormatException.malformed("marker value has the wrong size");
            }
        }
        return kind;
    }

    /**
     * Reads the marker of a transaction control batch.
     *
     * @throws RecordFormatException if the batch is not a control batch, or its record is not a marker
     */
    public static ControlMarker readMarker(Batch batch) {
        if (!batch.attributes().control()) {
            throw RecordFormatException.malformed("not a control batch");
        }
        BatchRecord rec = firstRecord(batch);
        ByteString key = rec.key();
        ByteString value = rec.value();
        if (key == null) {
            throw RecordFormatException.malformed("control record without key");
        }
        if (value == null) {
            throw RecordFormatException.malformed("control record without value");
        }
        if (key.size() != 4 || value.size() != 4) {
            throw RecordFormatException.malformed("control record has the wrong size");
        }
        ControlType type;
        switch (key.at(2) | key.at(3) << 8) {
            case 0 -> type = ControlType.ABORT;
            case 1 -> type = ControlType.COMMIT;
            default -> throw RecordFormatException.malformed("unknown control type");
        }
        byte[] v = value.toByteArray();
        return new ControlMarker(type, Batch.getU32(v, 0));
    }

    private static BatchRecord firstRecord(Batch batch) {
        List<BatchRecord> records = batch.records();
        if (records.isEmpty()) {
            throw RecordFormatException.malformed("empty control batch");
        }
        return records.get(0);
    }
}
