package dev.lotse.client;

import dev.lotse.proto.Messages.Range;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Offsets as the half-open ranges {@code [from, to)} that {@code Ack} and {@code Nack} carry. */
final class OffsetRanges {
    private OffsetRanges() {}

    /**
     * Sorts the offsets (as {@code u64}), drops duplicates and joins neighbours: {@code 5, 6, 7, 9} is
     * {@code [5, 8), [9, 10)}.
     */
    static List<Range> of(long[] offsets) {
        long[] sorted = offsets.clone();
        // Unsigned order: flip the sign bit, sort, flip back.
        for (int i = 0; i < sorted.length; i++) {
            sorted[i] ^= Long.MIN_VALUE;
        }
        Arrays.sort(sorted);
        List<Range> out = new ArrayList<>();
        long from = 0;
        long to = 0;
        boolean open = false;
        for (long flipped : sorted) {
            long o = flipped ^ Long.MIN_VALUE;
            if (open && o == to - 1) {
                continue; // a duplicate
            }
            if (open && o == to) {
                to = o + 1;
                continue;
            }
            if (open) {
                out.add(new Range(from, to));
            }
            from = o;
            to = o + 1;
            open = true;
        }
        if (open) {
            out.add(new Range(from, to));
        }
        return out;
    }
}
