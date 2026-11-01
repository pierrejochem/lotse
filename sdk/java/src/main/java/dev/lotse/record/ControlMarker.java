package dev.lotse.record;

import java.util.Objects;

/**
 * A transaction marker read from a control batch.
 *
 * @param type commit or abort
 * @param coordinatorEpoch the coordinator's epoch, a {@code u32}
 */
public record ControlMarker(ControlType type, long coordinatorEpoch) {
    /** Creates a marker. */
    public ControlMarker {
        Objects.requireNonNull(type);
    }
}
