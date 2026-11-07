package dev.lotse.examples;

import dev.lotse.auth.Credentials;
import dev.lotse.client.ClientConfig;
import dev.lotse.net.TlsConfig;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;

/**
 * The examples' connection settings, from the environment:
 *
 * <ul>
 *   <li>{@code LOTSE_BOOTSTRAP}: brokers, {@code host:port} separated by commas (default {@code 127.0.0.1:9092});
 *   <li>{@code LOTSE_CA}, and optionally {@code LOTSE_CERT} and {@code LOTSE_KEY}: PEM files for TLS. Without
 *       {@code LOTSE_CA} the examples use plain TCP, which the client allows on loopback only;
 *   <li>{@code LOTSE_USER} and {@code LOTSE_PASSWORD}: a SCRAM-SHA-512 login, if the cluster asks for one.
 * </ul>
 */
final class Env {
    private Env() {}

    static ClientConfig.Builder config() {
        String bootstrap = System.getenv().getOrDefault("LOTSE_BOOTSTRAP", "127.0.0.1:9092");
        ClientConfig.Builder b = ClientConfig.builder(List.of(bootstrap.split(",")));
        String ca = System.getenv("LOTSE_CA");
        try {
            if (ca == null) {
                b.plaintextOnLoopback();
            } else if (System.getenv("LOTSE_CERT") != null) {
                b.tls(TlsConfig.fromFiles(
                        Path.of(ca), Path.of(System.getenv("LOTSE_CERT")), Path.of(System.getenv("LOTSE_KEY"))));
            } else {
                b.tls(TlsConfig.fromFiles(Path.of(ca)));
            }
        } catch (IOException e) {
            throw new UncheckedIOException("reading the TLS files", e);
        }
        String user = System.getenv("LOTSE_USER");
        if (user != null) {
            b.credentials(Credentials.scram(user, System.getenv("LOTSE_PASSWORD")));
        }
        return b;
    }
}
