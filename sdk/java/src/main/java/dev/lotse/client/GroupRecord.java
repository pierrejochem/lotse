package dev.lotse.client;

import dev.lotse.record.BatchRecord;
import java.util.Objects;

/**
 * One record read by a group member.
 *
 * @param stream the stream
 * @param partition the partition
 * @param record the record, its offset included
 */
public record GroupRecord(String stream, int partition, BatchRecord record) {
    /** Creates the record. */
    public GroupRecord {
        Objects.requireNonNull(stream);
        Objects.requireNonNull(record);
    }
}
