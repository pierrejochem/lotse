package dev.lotse.net;

import dev.lotse.proto.Frames;
import dev.lotse.proto.Request;
import dev.lotse.proto.Response;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;

/** A TLS server in this process that answers {@code ApiVersions}, with a certificate from {@code lotse certs}. */
final class TlsTestServer implements AutoCloseable {
    private final SSLServerSocket server;
    /** Subjects of the client certificates the server saw. */
    final List<String> clientSubjects = new CopyOnWriteArrayList<>();
    final List<String> handshakeFailures = new CopyOnWriteArrayList<>();

    /**
     * @param protocols the TLS versions the server offers
     * @param clientAuth whether a client certificate is required, checked against the same CA
     */
    TlsTestServer(Certs certs, String[] protocols, boolean clientAuth) throws Exception {
        List<X509Certificate> chain = Pem.certificatesFile(certs.nodeCert);
        PrivateKey key = Pem.privateKeyFile(certs.nodeKey);
        KeyStore identity = KeyStore.getInstance("PKCS12");
        identity.load(null, null);
        identity.setKeyEntry("server", key, new char[0], chain.toArray(new X509Certificate[0]));
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(identity, new char[0]);
        KeyStore trust = KeyStore.getInstance("PKCS12");
        trust.load(null, null);
        int i = 0;
        for (X509Certificate ca : Pem.certificatesFile(certs.ca)) {
            trust.setCertificateEntry("ca" + i++, ca);
        }
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trust);
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        server = (SSLServerSocket) context.getServerSocketFactory().createServerSocket(0, 50, InetAddress.getLoopbackAddress());
        server.setEnabledProtocols(protocols);
        server.setNeedClientAuth(clientAuth);
        Thread t = new Thread(this::acceptLoop, "tls-test-server");
        t.setDaemon(true);
        t.start();
    }

    int port() {
        return server.getLocalPort();
    }

    private void acceptLoop() {
        while (!server.isClosed()) {
            try {
                SSLSocket s = (SSLSocket) server.accept();
                Thread t = new Thread(() -> serve(s), "tls-test-peer");
                t.setDaemon(true);
                t.start();
            } catch (IOException e) {
                return;
            }
        }
    }

    private void serve(SSLSocket socket) {
        try (socket) {
            socket.startHandshake();
            try {
                clientSubjects.add(((X509Certificate) socket.getSession().getPeerCertificates()[0])
                        .getSubjectX500Principal()
                        .getName());
            } catch (javax.net.ssl.SSLPeerUnverifiedException e) {
                clientSubjects.add("none");
            }
            DataInputStream in = new DataInputStream(socket.getInputStream());
            OutputStream out = socket.getOutputStream();
            while (true) {
                byte[] frame = new byte[in.readInt()];
                in.readFully(frame);
                Frames.DecodedRequest r = Frames.decodeRequest(frame);
                if (r.request() instanceof Request.ApiVersions) {
                    out.write(Frames.withLengthPrefix(Frames.encodeResponse(
                            r.header().correlationId(), new Response.ApiVersions(List.of(), List.of("tls-test")))));
                    out.flush();
                }
            }
        } catch (IOException e) {
            handshakeFailures.add(String.valueOf(e.getMessage()));
        }
    }

    @Override
    public void close() throws IOException {
        server.close();
    }
}
