package dev.lotse.net;

import java.io.IOException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/**
 * TLS for client connections: the cluster's CA, and optionally a client certificate.
 *
 * <p>Only TLS 1.3 is spoken, and the node's certificate must be valid for the host the client dialled (a name in
 * its DNS entries, or an IP address in its IP entries). Only the CA given here is trusted, not the JDK's roots.
 * Nothing is read from the system's trust store or from key stores.
 */
public final class TlsConfig {
    /** The only protocol version. */
    public static final String PROTOCOL = "TLSv1.3";

    private final SSLContext context;
    private final boolean clientCertificate;

    private TlsConfig(SSLContext context, boolean clientCertificate) {
        this.context = context;
        this.clientCertificate = clientCertificate;
    }

    /** The context connections are made with. */
    SSLContext context() {
        return context;
    }

    /** Whether a client certificate is presented when a node asks for one. */
    public boolean hasClientCertificate() {
        return clientCertificate;
    }

    /** A builder. */
    public static Builder builder() {
        return new Builder();
    }

    /** Trusts the CA in {@code caPem}, no client certificate. */
    public static TlsConfig fromFiles(Path caPem) throws IOException {
        return builder().ca(caPem).build();
    }

    /** Trusts the CA in {@code caPem} and presents the certificate and PKCS#8 key as the client's identity. */
    public static TlsConfig fromFiles(Path caPem, Path certPem, Path keyPem) throws IOException {
        return builder().ca(caPem).clientIdentity(certPem, keyPem).build();
    }

    /** Builds a {@link TlsConfig}. */
    public static final class Builder {
        private final List<X509Certificate> authorities = new ArrayList<>();
        private List<X509Certificate> chain;
        private PrivateKey key;

        private Builder() {}

        /** Trusts the CA certificates in a PEM file. */
        public Builder ca(Path pemFile) throws IOException {
            authorities.addAll(Pem.certificatesFile(pemFile));
            return this;
        }

        /** Trusts the CA certificates in PEM text. */
        public Builder ca(String pem) {
            authorities.addAll(Pem.certificates(pem));
            return this;
        }

        /** Presents a client certificate (the chain, leaf first) with its PKCS#8 private key, from files. */
        public Builder clientIdentity(Path certPem, Path keyPem) throws IOException {
            chain = Pem.certificatesFile(certPem);
            key = Pem.privateKeyFile(keyPem);
            return this;
        }

        /** Presents a client certificate (the chain, leaf first) with its PKCS#8 private key, from PEM text. */
        public Builder clientIdentity(String certPem, String keyPem) {
            chain = Pem.certificates(certPem);
            key = Pem.privateKey(keyPem);
            return this;
        }

        /**
         * Builds the configuration.
         *
         * @throws IllegalStateException if no CA was given: a node's certificate cannot be checked against nothing
         */
        public TlsConfig build() {
            if (authorities.isEmpty()) {
                throw new IllegalStateException("no CA certificate: give the cluster's ca.pem");
            }
            try {
                KeyStore trust = KeyStore.getInstance("PKCS12");
                trust.load(null, null);
                for (int i = 0; i < authorities.size(); i++) {
                    trust.setCertificateEntry("ca-" + i, authorities.get(i));
                }
                TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(trust);
                KeyManager[] keys = null;
                if (key != null) {
                    KeyStore identity = KeyStore.getInstance("PKCS12");
                    identity.load(null, null);
                    identity.setKeyEntry("client", key, new char[0], chain.toArray(new X509Certificate[0]));
                    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                    kmf.init(identity, new char[0]);
                    keys = kmf.getKeyManagers();
                }
                SSLContext context = SSLContext.getInstance(PROTOCOL);
                context.init(keys, tmf.getTrustManagers(), null);
                return new TlsConfig(context, key != null);
            } catch (GeneralSecurityException | IOException e) {
                throw new IllegalArgumentException("cannot build the TLS configuration: " + e.getMessage(), e);
            }
        }
    }
}
