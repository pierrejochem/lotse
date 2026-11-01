package dev.lotse.client;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.lotse.proto.Messages.Range;
import java.util.List;
import org.junit.jupiter.api.Test;

class OffsetRangesTest {
    @Test
    void oneOffsetIsAHalfOpenRangeOfOne() {
        assertEquals(List.of(new Range(7, 8)), OffsetRanges.of(new long[] {7}));
    }

    @Test
    void neighboursJoinAndGapsStay() {
        assertEquals(List.of(new Range(5, 8), new Range(9, 10)), OffsetRanges.of(new long[] {5, 6, 7, 9}));
    }

    @Test
    void theOrderAndDuplicatesDoNotMatter() {
        assertEquals(List.of(new Range(1, 4), new Range(10, 11)), OffsetRanges.of(new long[] {10, 3, 1, 2, 2, 3}));
    }

    @Test
    void offsetsAreOrderedAsUnsigned() {
        // 0xFFFF_FFFF_FFFF_FFFF is the largest u64: it sorts after 1, and its range ends at 0 by wrapping.
        assertEquals(List.of(new Range(1, 2), new Range(-1L, 0)), OffsetRanges.of(new long[] {-1L, 1}));
    }

    @Test
    void nothingGivesNoRanges() {
        assertEquals(List.of(), OffsetRanges.of(new long[0]));
    }
}
