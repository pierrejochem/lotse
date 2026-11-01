package dev.lotse.auth;

import dev.lotse.proto.Messages;
import dev.lotse.proto.Request;
import dev.lotse.record.ByteString;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Objects;

/**
 * What a node keeps for one SCRAM user: salt, iteration count, {@code StoredKey} and {@code ServerKey}. Neither
 * the password nor anything that lets one log in without it is stored or sent: a stolen credential lets an
 * attacker impersonate the server, not the user.
 *
 * @param salt the salt
 * @param iterations PBKDF2 iterations, from {@link ScramClient#MIN_ITERATIONS} to {@link ScramClient#MAX_ITERATIONS}
 * @param storedKey {@code H(ClientKey)}, 64 bytes
 * @param serverKey {@code HMAC(SaltedPassword, "Server Key")}, 64 bytes
 */
public record ScramCredential(byte[] salt, int iterations, byte[] storedKey, byte[] serverKey) {
    /** The salt length {@link #forPassword} uses. */
    public static final int SALT_BYTES = 16;

    /** Checks the parts. */
    public ScramCredential {
        Objects.requireNonNull(salt);
        Objects.requireNonNull(storedKey);
        Objects.requireNonNull(serverKey);
        salt = salt.clone();
        storedKey = storedKey.clone();
        serverKey = serverKey.clone();
    }

    /**
     * The credential for {@code password} with {@code salt} and {@code iterations}. The password is used as its
     * UTF-8 bytes; there is no SASLprep.
     *
     * @throws IllegalArgumentException if {@code iterations} is outside the bounds a node accepts
     */
    public static ScramCredential of(String password, byte[] salt, int iterations) {
        if (iterations < ScramClient.MIN_ITERATIONS || iterations > ScramClient.MAX_ITERATIONS) {
            throw new IllegalArgumentException("iterations must be from " + ScramClient.MIN_ITERATIONS + " to "
                    + ScramClient.MAX_ITERATIONS + ", not " + iterations);
        }
        byte[] salted = Scram.saltedPassword(password.getBytes(java.nio.charset.StandardCharsets.UTF_8), salt, iterations);
        byte[] clientKey = Scram.hmac(salted, Scram.CLIENT_KEY);
        return new ScramCredential(salt, iterations, Scram.sha512(clientKey), Scram.hmac(salted, Scram.SERVER_KEY));
    }

    /**
     * A credential for {@code password} with a fresh 16-byte salt and {@link ScramClient#MIN_ITERATIONS}
     * iterations, as {@code lotse-client}'s {@code set_user_password} makes it.
     */
    public static ScramCredential forPassword(String password, SecureRandom random) {
        byte[] salt = new byte[SALT_BYTES];
        random.nextBytes(salt);
        return of(password, salt, ScramClient.MIN_ITERATIONS);
    }

    /** The {@code AlterUser} request that sets this credential for {@code user} (admin). */
    public Request.AlterUser alterUser(String user) {
        return new Request.AlterUser(
                user,
                new Messages.ScramCredentialData(
                        iterations, ByteString.copyOf(salt), ByteString.copyOf(storedKey), ByteString.copyOf(serverKey)));
    }

    /** The {@code AlterUser} request that removes {@code user}'s credential (admin). */
    public static Request.AlterUser removal(String user) {
        return new Request.AlterUser(user, null);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ScramCredential c
                && iterations == c.iterations
                && Arrays.equals(salt, c.salt)
                && Arrays.equals(storedKey, c.storedKey)
                && Arrays.equals(serverKey, c.serverKey);
    }

    @Override
    public int hashCode() {
        return 31 * (31 * Arrays.hashCode(salt) + iterations) + Arrays.hashCode(storedKey);
    }

    @Override
    public String toString() {
        return "ScramCredential[iterations=" + iterations + "]";
    }
}
