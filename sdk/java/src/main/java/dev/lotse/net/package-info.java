/**
 * Connections to a broker: framing, pipelined requests, pushes, TLS 1.3 and sessions.
 *
 * <p>A {@link dev.lotse.net.Connection} is one persistent TCP connection to one broker address, with a reader
 * thread. Requests are pipelined and answered in any order; each returns a {@code CompletableFuture}.
 * {@link dev.lotse.net.ConnectionPool} keeps one authenticated connection per address and renews or replaces it
 * before its session ends. TLS 1.3 is the default and the only version; plain TCP exists for loopback only and must
 * be asked for ({@link dev.lotse.net.ConnectionOptions.Builder#plaintextOnLoopback}).
 */
package dev.lotse.net;
