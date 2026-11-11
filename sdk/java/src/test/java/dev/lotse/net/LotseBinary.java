package dev.lotse.net;

import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assumptions;

/** Finds the {@code lotse} executable the tests that need a real node or real certificates use. */
public final class LotseBinary {
    private LotseBinary() {}

    /** {@code LOTSE_BIN}, else the cargo target directory's debug build. {@code null} if there is none. */
    public static Path find() {
        String env = System.getenv("LOTSE_BIN");
        if (env != null && !env.isBlank()) {
            Path p = Path.of(env);
            return Files.isExecutable(p) ? p : null;
        }
        String target = System.getenv("CARGO_TARGET_DIR");
        for (Path p : new Path[] {
            Path.of(target == null ? "/tmp/cargo-target" : target, "debug", "lotse"),
            Path.of("../../target/debug/lotse").toAbsolutePath().normalize()
        }) {
            if (Files.isExecutable(p)) {
                return p;
            }
        }
        return null;
    }

    /** The executable, or the test is skipped with a message that says how to get one. */
    public static Path require() {
        Path bin = find();
        if (bin == null) {
            String why = "SKIPPED: no lotse executable. Build one with `cargo build -p lotse-node` and set LOTSE_BIN "
                    + "(default: /tmp/cargo-target/debug/lotse).";
            System.err.println(why);
            Assumptions.assumeTrue(false, why);
        }
        return bin;
    }

    /** Runs the executable and returns its output; fails on a non-zero exit. */
    public static String run(Path bin, String... args) throws IOException, InterruptedException {
        List<String> command = new ArrayList<>();
        command.add(bin.toString());
        command.addAll(List.of(args));
        Process p = new ProcessBuilder(command).redirectErrorStream(true).start();
        String out = new String(p.getInputStream().readAllBytes());
        if (!p.waitFor(60, TimeUnit.SECONDS) || p.exitValue() != 0) {
            throw new AssertionError(String.join(" ", command) + " failed:\n" + out);
        }
        return out;
    }

    private static final java.util.Set<Integer> HANDED_OUT = java.util.concurrent.ConcurrentHashMap.newKeySet();

    /**
     * A port nothing listens on, chosen below the ephemeral range (32768 and up on Linux): a port the kernel handed out
     * for listening is handed out again to the next outgoing connection, which could take it before the node starts.
     */
    public static int freePort() throws IOException {
        for (;;) {
            int port = 20_000 + ThreadLocalRandom.current().nextInt(12_000);
            // Not one this JVM handed out already: those are free only until their node binds them.
            if (!HANDED_OUT.add(port)) {
                continue;
            }
            try (ServerSocket s = new ServerSocket(port, 1, InetAddress.getLoopbackAddress())) {
                return s.getLocalPort();
            } catch (BindException e) {
                // taken: try another
            }
        }
    }
}
