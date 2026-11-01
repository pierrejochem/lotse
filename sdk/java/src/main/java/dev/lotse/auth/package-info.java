/**
 * Authenticating a connection: SASL SCRAM-SHA-512 and OAUTHBEARER, and the credential a user's password becomes.
 *
 * <p>{@link dev.lotse.auth.Authenticator} runs the exchange over any connection that can send a request and wait
 * for its answer. {@link dev.lotse.auth.ScramClient} is the SCRAM state machine without I/O, so a test can drive it
 * with fixed nonces. Nothing here reads the clock or opens a socket.
 */
package dev.lotse.auth;
