package dev.lotse.auth;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.lotse.proto.ErrorCode;
import dev.lotse.proto.Request;
import dev.lotse.proto.Response;
import dev.lotse.record.ByteString;
import dev.lotse.record.Vectors;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/** An access token's text and hash (ADR 0133), checked against texts and hashes computed outside this code. */
class AccessTokenTest {
    /** The secret of {@code lotse-auth}'s own token tests: byte {@code i} is {@code i * 37 + 11}. */
    private static byte[] sampleSecret() {
        byte[] s = new byte[32];
        for (int i = 0; i < 32; i++) {
            s[i] = (byte) (i * 37 + 11);
        }
        return s;
    }

    private static final long SAMPLE_ID = 0x0123_4567_89ab_cdefL;
    // Written by Python's base64.urlsafe_b64encode and hashlib.sha256 for the sample secret, not by this code.
    private static final String SAMPLE_TEXT = "lotse_tok_0123456789abcdef_CzBVep_E6Q4zWH2ix-wRNluApcrvFDleg6jN8hc8YYY";
    private static final String SAMPLE_HASH = "83b7a8ed859053c81d818870fab1f8b1ae44d06a98a9665d369a8fd7d2838ded";

    private static AccessToken sample() {
        return new AccessToken(SAMPLE_ID, sampleSecret());
    }

    @Test
    void tokenTextRoundTripsAndTrims() {
        AccessToken t = sample();
        assertEquals(SAMPLE_TEXT, t.text());
        assertEquals(Optional.of(t), AccessToken.parse(SAMPLE_TEXT));
        // A pasted token often carries a newline; Rust's trim_ascii takes space, tab, LF, FF and CR.
        assertEquals(Optional.of(t), AccessToken.parse("  " + SAMPLE_TEXT + "\n"));
        assertEquals(Optional.of(t), AccessToken.parse("\t\r\n\f" + SAMPLE_TEXT + " \r\n"));
        // Not whitespace to the node: a vertical tab or a no-break space stays and spoils the text.
        assertEquals(Optional.empty(), AccessToken.parse("\u000b" + SAMPLE_TEXT));
        assertEquals(Optional.empty(), AccessToken.parse(SAMPLE_TEXT + " "));
        assertArrayEquals(sampleSecret(), AccessToken.parse(SAMPLE_TEXT).orElseThrow().secret());
        assertEquals(SAMPLE_ID, AccessToken.parse(SAMPLE_TEXT).orElseThrow().id());
    }

    @Test
    void theHighestIdAndAnyRandomSecretRoundTrip() {
        SecureRandom random = new SecureRandom();
        for (long id : new long[] {0, 1, 0x2a, Long.MAX_VALUE, -1L}) {
            byte[] secret = new byte[32];
            random.nextBytes(secret);
            AccessToken t = new AccessToken(id, secret);
            String text = t.text();
            assertEquals(10 + 16 + 1 + 43, text.length(), text);
            assertEquals(Optional.of(t), AccessToken.parse(text), text);
        }
        assertTrue(new AccessToken(-1L, new byte[32]).text().startsWith("lotse_tok_ffffffffffffffff_"));
    }

    @Test
    void malformedTextsDoNotParse() {
        String secret = SAMPLE_TEXT.substring(SAMPLE_TEXT.length() - 43);
        List<String> bad = List.of(
                SAMPLE_TEXT.replace("lotse_tok_", "lotse_key_"),
                "lotse_tok_0123456789abcde_" + secret,
                "lotse_tok_0123456789abcdef_" + secret.substring(0, 42),
                "lotse_tok_0123456789abcdef_" + secret.substring(0, 42) + "!",
                // 'Z' ends in non-zero spare bits: not the canonical text of any secret.
                "lotse_tok_0123456789abcdef_" + secret.substring(0, 42) + "Z",
                "lotse_tok_0123456789abcdef_" + secret.substring(0, 42) + "=",
                "lotse_tok_0123456789ABCDEF_" + secret,
                "lotse_tok_+123456789abcdef_" + secret,
                "lotse_tok_0123456789abcdef_" + secret + "x",
                "lotse_tok_0123456789abcdef" + secret,
                "LOTSE_TOK_0123456789abcdef_" + secret,
                "");
        for (String b : bad) {
            assertEquals(Optional.empty(), AccessToken.parse(b), b);
        }
    }

    @Test
    void theHashIsTheSha256OfTheSecretBytes() {
        AccessToken t = sample();
        assertEquals(SAMPLE_HASH, Vectors.hex(t.hash()));
        assertFalse(java.util.Arrays.equals(t.secret(), t.hash()));
        assertThrows(IllegalArgumentException.class, () -> new AccessToken(1, new byte[31]));
    }

    @Test
    void theSecretIsNotPrinted() {
        AccessToken t = sample();
        String secret = SAMPLE_TEXT.substring(SAMPLE_TEXT.length() - 43);
        assertFalse(t.toString().contains(secret), t.toString());
        assertTrue(t.toString().contains("0123456789abcdef"), t.toString());
        String shown = Credentials.token(SAMPLE_TEXT).toString();
        assertFalse(shown.contains(secret), shown);
        assertFalse(shown.contains("CzBVep"), shown);
    }

    @Test
    void lifetimesAreTheFiveSteps() {
        long[] days = {7, 30, 90, 180, 365};
        for (int code = 1; code <= 5; code++) {
            TokenLifetime l = TokenLifetime.fromCode(code);
            assertEquals(code, l.code());
            assertEquals(days[code - 1], l.duration().toDays());
        }
        assertThrows(IllegalArgumentException.class, () -> TokenLifetime.fromCode(0));
        assertThrows(IllegalArgumentException.class, () -> TokenLifetime.fromCode(6));
    }

    /** {@code LOTSE-TOKEN} is one round whose message is the token's text, as the client was given it. */
    @Test
    void aTokenSignsInWithItsTextAsTheOneMessage() {
        List<Request> sent = new ArrayList<>();
        long lifetime = Authenticator.authenticate(
                r -> {
                    sent.add(r);
                    if (r instanceof Request.SaslHandshake) {
                        return new Response.SaslHandshake(ErrorCode.NONE, List.of("LOTSE-TOKEN"));
                    }
                    return new Response.SaslAuthenticate(ErrorCode.NONE, null, ByteString.EMPTY, 60_000);
                },
                Credentials.token(SAMPLE_TEXT),
                new SecureRandom());
        assertEquals(60_000, lifetime);
        assertEquals(2, sent.size());
        assertEquals(new Request.SaslHandshake("LOTSE-TOKEN"), sent.get(0));
        Request.SaslAuthenticate auth = (Request.SaslAuthenticate) sent.get(1);
        assertEquals(SAMPLE_TEXT, new String(auth.authBytes().toByteArray(), StandardCharsets.UTF_8));
    }

    @Test
    void aRefusedTokenFailsTheLogin() {
        AuthenticationException e = assertThrows(AuthenticationException.class, () -> Authenticator.authenticate(
                r -> r instanceof Request.SaslHandshake
                        ? new Response.SaslHandshake(ErrorCode.NONE, List.of("LOTSE-TOKEN"))
                        : new Response.SaslAuthenticate(
                                ErrorCode.AUTHENTICATION_FAILED, "authentication failed", ByteString.EMPTY, 0),
                Credentials.token(SAMPLE_TEXT),
                new SecureRandom()));
        assertEquals("authentication failed", e.getMessage());
    }
}
