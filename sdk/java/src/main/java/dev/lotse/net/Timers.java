package dev.lotse.net;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** One daemon thread that fires request timeouts for every connection. */
final class Timers {
    private static final ScheduledThreadPoolExecutor EXECUTOR = new ScheduledThreadPoolExecutor(1, r -> {
        Thread t = new Thread(r, "lotse-timeouts");
        t.setDaemon(true);
        return t;
    });

    static {
        EXECUTOR.setRemoveOnCancelPolicy(true);
    }

    private Timers() {}

    static ScheduledFuture<?> after(long nanos, Runnable task) {
        return EXECUTOR.schedule(task, nanos, TimeUnit.NANOSECONDS);
    }
}
