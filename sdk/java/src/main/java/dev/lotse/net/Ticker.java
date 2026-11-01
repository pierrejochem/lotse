package dev.lotse.net;

/** A monotonic clock in nanoseconds, so tests can move session times without waiting. */
@FunctionalInterface
public interface Ticker {
    /** The system's monotonic clock, {@link System#nanoTime()}. */
    Ticker SYSTEM = System::nanoTime;

    /** Nanoseconds from an arbitrary origin; only differences mean anything, and they may overflow (compare with subtraction). */
    long nanos();
}
