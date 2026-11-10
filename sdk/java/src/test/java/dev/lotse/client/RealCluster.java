package dev.lotse.client;

import dev.lotse.net.Address;
import dev.lotse.net.LotseBinary;
import dev.lotse.net.TlsConfig;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Real {@code lotse} processes started from a generated cluster file: plain TCP on loopback with no authentication,
 * or TLS with {@code alice} as the cluster admin (certificate login).
 */
final class RealCluster implements AutoCloseable {
    final Path dir;
    final Path config;
    final Path bin;
    final boolean tls;
    final List<Address> clientAddresses = new ArrayList<>();
    private final Process[] processes;

    RealCluster(Path bin, int nodes, boolean tls) throws IOException, InterruptedException {
        this.bin = bin;
        this.tls = tls;
        this.dir = Files.createTempDirectory("lotse-sdk-cluster");
        this.config = dir.resolve("cluster.toml");
        this.processes = new Process[nodes + 1];
        StringBuilder text = new StringBuilder("[cluster]\ndata_dir = \"" + dir + "/data/node-{id}\"\n\n");
        if (tls) {
            text.append("[tls]\nca = \"").append(dir).append("/tls/ca.pem\"\ncert = \"").append(dir)
                    .append("/tls/node-{id}.pem\"\nkey = \"").append(dir).append("/tls/node-{id}.key\"\n\n");
        } else {
            text.append("[tls]\nplaintext_on_loopback = true\n\n");
        }
        text.append("[retention]\nms = -1\n\n[auth]\n");
        text.append(tls ? "admins = [\"alice\"]\n" : "required = false\n");
        for (int id = 1; id <= nodes; id++) {
            int peer = LotseBinary.freePort();
            int client = LotseBinary.freePort();
            clientAddresses.add(new Address("127.0.0.1", client));
            text.append("\n[[node]]\nid = ").append(id).append("\npeer = \"127.0.0.1:").append(peer)
                    .append("\"\nclient = \"127.0.0.1:").append(client).append("\"\n");
        }
        Files.writeString(config, text.toString());
        if (tls) {
            LotseBinary.run(bin, "certs", "--config", config.toString());
            LotseBinary.run(bin, "certs", "--config", config.toString(), "--client", "alice", "--out",
                    dir.resolve("clients").toString());
        }
    }

    TlsConfig adminTls() throws IOException {
        return TlsConfig.fromFiles(
                dir.resolve("tls/ca.pem"), dir.resolve("clients/alice.pem"), dir.resolve("clients/alice.key"));
    }

    TlsConfig anonymousTls() throws IOException {
        return TlsConfig.fromFiles(dir.resolve("tls/ca.pem"));
    }

    List<String> bootstrap() {
        return clientAddresses.stream().map(Address::toString).toList();
    }

    Address address(int node) {
        return clientAddresses.get(node - 1);
    }

    void startAll() throws IOException {
        for (int id = 1; id < processes.length; id++) {
            start(id);
        }
    }

    void start(int id) throws IOException {
        processes[id] = new ProcessBuilder(bin.toString(), "run", "--config", config.toString(), "--node", String.valueOf(id))
                .redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.appendTo(dir.resolve("node-" + id + ".log").toFile()))
                .start();
    }

    /** Kills the node without warning (SIGKILL), as a crash. */
    void kill(int id) throws InterruptedException {
        Process p = processes[id];
        if (p != null) {
            p.destroyForcibly();
            p.waitFor(10, TimeUnit.SECONDS);
            processes[id] = null;
        }
    }

    String log(int id) {
        try {
            return Files.readString(dir.resolve("node-" + id + ".log"));
        } catch (IOException e) {
            return "(no log)";
        }
    }

    @Override
    public void close() {
        for (Process p : processes) {
            if (p != null) {
                p.destroyForcibly();
            }
        }
    }

    /**
     * Pins the leader of {@code name}'s first partition to the node that leads it now ({@code moveLeader}, ADR 0065)
     * and waits until that node has led it in one term for five reads in a row. A new group's leader moves to the
     * group's preferred node in its first seconds (ADR 0050), and a test that relies on what its leader keeps in memory
     * (what is in flight, delivery counts, a single active consumer, produce counters) breaks if that move falls inside
     * it; waiting for "the same leader for a while" only guesses when the move comes. A pinned leader is left where it
     * is.
     */
    static void pinLeader(LotseClient client, String name) throws InterruptedException {
        long deadline = System.nanoTime() + java.time.Duration.ofSeconds(60).toNanos();
        dev.lotse.proto.Messages.PartitionMetadata p = partition0(client, name);
        while (p == null || p.leader() == 0) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("no leader for " + name);
            }
            Thread.sleep(100);
            p = partition0(client, name);
        }
        long leader = p.leader();
        client.moveLeader(name, 0, leader);
        long lastLeader = 0;
        long lastTerm = -1;
        int same = 0;
        while (same < 5) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("node " + leader + " does not settle as the leader of " + name);
            }
            Thread.sleep(100);
            p = partition0(client, name);
            long nowLeader = p == null ? 0 : p.leader();
            long nowTerm = p == null ? -1 : p.leaderTerm();
            same = nowLeader == leader && nowLeader == lastLeader && nowTerm == lastTerm ? same + 1 : 0;
            lastLeader = nowLeader;
            lastTerm = nowTerm;
        }
    }

    private static dev.lotse.proto.Messages.PartitionMetadata partition0(LotseClient client, String name) {
        for (var r : client.metadata(name).resources()) {
            if (r.name().equals(name)) {
                for (var p : r.partitions()) {
                    if (p.partition() == 0) {
                        return p;
                    }
                }
            }
        }
        return null;
    }
}
