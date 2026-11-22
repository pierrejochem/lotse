package dev.lotse.client;

/**
 * An access token just created (ADR 0133), from {@link LotseClient#createToken}.
 *
 * @param id names the token; listed with {@link LotseClient#describeTokens} and revoked with
 *     {@link LotseClient#revokeToken}, a {@code u64}
 * @param text the token's text, {@code lotse_tok_...}: shown once, since the cluster keeps only the hash of its
 *     secret; sign in with it through {@link dev.lotse.auth.Credentials#token}
 * @param expiresAtMs when the token stops being valid, milliseconds since the Unix epoch
 */
public record CreatedToken(long id, String text, long expiresAtMs) {
    /** Names the id and the end only: the text is the secret and stays out of logs. */
    @Override
    public String toString() {
        return "CreatedToken[id=" + Long.toUnsignedString(id) + ", expiresAtMs=" + expiresAtMs + "]";
    }
}
