package dev.lotse.net;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
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
 *
 * <p>A client identity given as files is read again for the next connection once either file changed, so a renewed
 * certificate (ADR 0123) is used without a restart. If the files cannot be read then, half written for instance, the
 * identity read before is kept.
 */
public final class TlsConfig {
    /** The only protocol version. */
    public static final String PROTOCOL = "TLSv1.3";

    private final List<X509Certificate> authorities;
    private final boolean clientCertificate;
    /** The identity's files, or {@code null} when it was given as text or not at all. */
    private final Path certFile;
    private final Path keyFile;

    private SSLContext context;
    private List<FileTime> stamps;

    private TlsConfig(
            List<X509Certificate> authorities,
            SSLContext context,
            boolean clientCertificate,
            Path certFile,
            Path keyFile,
            List<FileTime> stamps) {
        this.authorities = authorities;
        this.context = context;
        this.clientCertificate = clientCertificate;
        this.certFile = certFile;
        this.keyFile = keyFile;
        this.stamps = stamps;
    }

    /** The context connections are made with: built again first if the identity's files changed. */
    synchronized SSLContext context() {
        if (certFile == null) {
            return context;
        }
        List<FileTime> now = stamps(certFile, keyFile);
        if (now != null && !now.equals(stamps)) {
            try {
                List<X509Certificate> chain = Pem.certificatesFile(certFile);
                PrivateKey key = Pem.privateKeyFile(keyFile);
                // Between the writes of a renewal the key may be new and the certificate not yet.
                if (!chain.isEmpty() && matches(chain.get(0), key)) {
                    context = Builder.contextOf(authorities, chain, key);
                    stamps = now;
                }
            } catch (IOException | IllegalArgumentException e) {
                // Half written, or a key that does not match yet: keep what worked, look again next time.
            }
        }
        return context;
    }

    /** Whether {@code key} belongs to the certificate: it signs what the certificate's key verifies. */
    static boolean matches(X509Certificate cert, PrivateKey key) {
        String algorithm =
                switch (key.getAlgorithm()) {
                    case "EC" -> "SHA256withECDSA";
                    case "RSA" -> "SHA256withRSA";
                    default -> key.getAlgorithm();
                };
        try {
            byte[] probe = "lotse".getBytes(java.nio.charset.StandardCharsets.US_ASCII);
            java.security.Signature sign = java.security.Signature.getInstance(algorithm);
            sign.initSign(key);
            sign.update(probe);
            byte[] sig = sign.sign();
            java.security.Signature verify = java.security.Signature.getInstance(algorithm);
            verify.initVerify(cert.getPublicKey());
            verify.update(probe);
            return verify.verify(sig);
        } catch (GeneralSecurityException e) {
            return false;
        }
    }

    /** The modification times of both files, or {@code null} if either cannot be read. */
    private static List<FileTime> stamps(Path cert, Path key) {
        try {
            return List.of(Files.getLastModifiedTime(cert), Files.getLastModifiedTime(key));
        } catch (IOException e) {
            return null;
        }
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
        private Path certFile;
        private Path keyFile;
        private List<FileTime> stamps;

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
            stamps = stamps(certPem, keyPem);
            chain = Pem.certificatesFile(certPem);
            key = Pem.privateKeyFile(keyPem);
            certFile = certPem;
            keyFile = keyPem;
            return this;
        }

        /** Presents a client certificate (the chain, leaf first) with its PKCS#8 private key, from PEM text. */
        public Builder clientIdentity(String certPem, String keyPem) {
            chain = Pem.certificates(certPem);
            key = Pem.privateKey(keyPem);
            certFile = null;
            keyFile = null;
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
            List<X509Certificate> cas = List.copyOf(authorities);
            return new TlsConfig(cas, contextOf(cas, chain, key), key != null, certFile, keyFile, stamps);
        }

        static SSLContext contextOf(List<X509Certificate> authorities, List<X509Certificate> chain, PrivateKey key) {
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
                return context;
            } catch (GeneralSecurityException | IOException e) {
                throw new IllegalArgumentException("cannot build the TLS configuration: " + e.getMessage(), e);
            }
        }
    }
}
