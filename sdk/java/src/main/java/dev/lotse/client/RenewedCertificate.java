package dev.lotse.client;

/**
 * A renewed client certificate (ADR 0123).
 *
 * @param keyPem the new private key, PKCS#8 PEM
 * @param chainPem the new certificate, then the renewal CA's, PEM
 * @param notAfterMs milliseconds since the Unix epoch at which the new certificate stops being valid
 */
public record RenewedCertificate(String keyPem, String chainPem, long notAfterMs) {}
