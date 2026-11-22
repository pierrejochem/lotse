/**
 * Authenticating a connection: SASL SCRAM-SHA-512, OAUTHBEARER and access tokens ({@code LOTSE-TOKEN}), the
 * credential a user's password becomes, and an access token's text and hash.
 *
 * <p>{@link dev.lotse.auth.Authenticator} runs the exchange over any connection that can send a request and wait
 * for its answer. {@link dev.lotse.auth.ScramClient} is the SCRAM state machine without I/O, so a test can drive it
 * with fixed nonces. Nothing here reads the clock or opens a socket.
 */
package dev.lotse.auth;
