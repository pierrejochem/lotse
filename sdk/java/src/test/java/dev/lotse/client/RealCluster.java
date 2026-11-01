package dev.lotse.client;

import dev.lotse.net.Address;
import dev.lotse.net.LotseBinary;
import dev.lotse.net.TlsConfig;
import java.io.IOException;
import java.net.ServerSocket;
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
            int peer = freePort();
            int client = freePort();
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

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
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
}
