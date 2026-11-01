package dev.lotse.net;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;

/** A throwaway CA and certificates made by the real {@code lotse certs}, for a one-node cluster file. */
final class Certs {
    final Path dir;
    final Path config;
    final Path ca;
    final Path nodeCert;
    final Path nodeKey;
    final Path clientCert;
    final Path clientKey;
    final int peerPort;
    final int clientPort;

    /**
     * @param extra more lines for the {@code [auth]} section, such as {@code max_session_s = 3}
     */
    Certs(Path bin, String extra) throws IOException, InterruptedException {
        dir = Files.createTempDirectory("lotse-sdk-certs");
        peerPort = freePort();
        clientPort = freePort();
        config = dir.resolve("cluster.toml");
        Files.writeString(config, "[cluster]\ndata_dir = \"" + dir + "/data/node-{id}\"\n\n"
                + "[tls]\nca = \"" + dir + "/tls/ca.pem\"\ncert = \"" + dir + "/tls/node-{id}.pem\"\n"
                + "key = \"" + dir + "/tls/node-{id}.key\"\n\n[retention]\nms = -1\n\n"
                + "[auth]\nadmins = [\"alice\"]\n" + extra
                + "\n[[node]]\nid = 1\npeer = \"127.0.0.1:" + peerPort + "\"\nclient = \"127.0.0.1:" + clientPort + "\"\n");
        LotseBinary.run(bin, "certs", "--config", config.toString());
        LotseBinary.run(bin, "certs", "--config", config.toString(), "--client", "alice", "--out", dir.resolve("clients").toString());
        ca = dir.resolve("tls/ca.pem");
        nodeCert = dir.resolve("tls/node-1.pem");
        nodeKey = dir.resolve("tls/node-1.key");
        clientCert = dir.resolve("clients/alice.pem");
        clientKey = dir.resolve("clients/alice.key");
    }

    static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
