/**
 * The Lotse record batch: one format on disk, on the wire and in Raft.
 *
 * <p>A batch is a fixed 65-byte header followed by a block of records. All fixed-width fields are little-endian;
 * record fields are ZigZag varints. {@link dev.lotse.record.Batch#parse} checks everything the broker checks,
 * {@link dev.lotse.record.BatchBuilder} builds batches, and {@link dev.lotse.record.Batch} edits them in place.
 *
 * <p>Unsigned integers: {@code u64} fields are {@code long}s holding the unsigned bit pattern, {@code u32} fields
 * are {@code long}s in {@code [0, 2^32)}, {@code u16} fields are {@code int}s in {@code [0, 65536)}. See
 * {@link dev.lotse.record.Unsigned}.
 */
package dev.lotse.record;
