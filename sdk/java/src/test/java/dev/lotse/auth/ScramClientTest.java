package dev.lotse.auth;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.lotse.proto.Request;
import dev.lotse.record.Vectors;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.List;
import org.junit.jupiter.api.Test;

class ScramClientTest {
    private static final byte[] SALT = "0123456789abcdef".getBytes(StandardCharsets.US_ASCII);

    private static String text(byte[] b) {
        return new String(b, StandardCharsets.UTF_8);
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void everyMessageOfTheBrokersExchangeIsReproducedByteForByte() {
        List<String[]> cases = Vectors.lines("scram.txt");
        assertEquals(5, cases.size());
        for (String[] c : cases) {
            String label = c[0];
            byte[] user = Vectors.hex(c[1]);
            byte[] password = c[2].length() == 1 ? new byte[0] : Vectors.hex(c[2].substring(1));
            byte[] salt = Vectors.hex(c[3]);
            int iterations = Integer.parseInt(c[4]);
            String clientNonce = c[5];
            String serverNonce = c[6];

            ScramClient client = new ScramClient(text(user), text(password), clientNonce);
            assertArrayEquals(Vectors.hex(c[7]), client.first(), label + ": client first");
            byte[] clientFinal = client.respond(Vectors.hex(c[8]));
            assertArrayEquals(Vectors.hex(c[9]), clientFinal, label + ": client final (the proof)");
            client.verify(Vectors.hex(c[10]));

            ScramCredential credential = ScramCredential.of(text(password), salt, iterations);
            assertEquals(c[11], Vectors.hex(credential.storedKey()), label + ": StoredKey");
            assertEquals(c[12], Vectors.hex(credential.serverKey()), label + ": ServerKey");

            // The test server, written from the RFC, agrees with the broker's bytes too.
            TestScramServer server = new TestScramServer(u -> credential, serverNonce);
            assertArrayEquals(Vectors.hex(c[8]), server.first(Vectors.hex(c[7])), label + ": server first");
            assertArrayEquals(Vectors.hex(c[10]), server.last(clientFinal).serverFinal(), label + ": server final");
        }
    }

    @Test
    void theUserNameIsEscaped() {
        assertEquals("n,,n=a=2Cb=3Dc,r=x", text(new ScramClient("a,b=c", "pw", "x").first()));
    }

    private static ScramClient afterFirst(String nonce) {
        ScramClient c = new ScramClient("alice", "pw", nonce);
        c.first();
        return c;
    }

    private static byte[] serverFirst(String nonce, int iterations) {
        return bytes("r=" + nonce + ",s=" + Base64.getEncoder().encodeToString(SALT) + ",i=" + iterations);
    }

    @Test
    void theServerNonceMustExtendTheClientsStrictly() {
        // equal: a server that adds nothing is not contributing a nonce
        assertEquals(ScramException.Kind.MALFORMED, assertThrows(ScramException.class, () -> afterFirst("abc").respond(serverFirst("abc", 4096))).kind());
        // does not start with the client's nonce
        assertThrows(ScramException.class, () -> afterFirst("abc").respond(serverFirst("xbcdef", 4096)));
        // shorter
        assertThrows(ScramException.class, () -> afterFirst("abc").respond(serverFirst("ab", 4096)));
        // a nonce with a character outside printable ASCII
        assertThrows(ScramException.class, () -> afterFirst("abc").respond(serverFirst("abc d", 4096)));
        // accepted
        afterFirst("abc").respond(serverFirst("abcd", 4096));
    }

    @Test
    void theIterationCountIsBounded() {
        assertThrows(ScramException.class, () -> afterFirst("abc").respond(serverFirst("abcd", 4095)));
        assertThrows(ScramException.class, () -> afterFirst("abc").respond(serverFirst("abcd", 0)));
        assertThrows(ScramException.class, () -> afterFirst("abc").respond(serverFirst("abcd", (1 << 20) + 1)));
        assertThrows(ScramException.class, () -> afterFirst("abc").respond(serverFirst("abcd", Integer.MAX_VALUE)));
        assertThrows(ScramException.class, () -> afterFirst("abc").respond(bytes("r=abcd,s=AAAA,i=99999999999")));
        assertThrows(ScramException.class, () -> afterFirst("abc").respond(bytes("r=abcd,s=AAAA,i=-4096")));
        afterFirst("abc").respond(serverFirst("abcd", 4096));
        // The largest count is accepted, and costs a second or two of PBKDF2.
        afterFirst("abc").respond(serverFirst("abcd", 1 << 20));
    }

    @Test
    void aMalformedServerFirstMessageIsRefused() {
        for (String bad : List.of("", "r=abcd", "r=abcd,s=AAAA", "s=AAAA,r=abcd,i=4096", "r=abcd,s=!!!!,i=4096", "r=abcd,s=AAAA,i=x")) {
            assertEquals(
                    ScramException.Kind.MALFORMED,
                    assertThrows(ScramException.class, () -> afterFirst("abc").respond(bytes(bad)), bad).kind());
        }
        assertThrows(ScramException.class, () -> afterFirst("abc").respond(new byte[] {(byte) 0xff, (byte) 0xfe}));
    }

    @Test
    void theServersSignatureIsChecked() {
        ScramCredential credential = ScramCredential.of("pw", SALT, 4096);
        for (String mode : List.of("good", "wrong signature", "no v=", "not base64", "empty", "error message")) {
            ScramClient client = new ScramClient("alice", "pw", "cn");
            TestScramServer server = new TestScramServer(u -> credential, "sn");
            byte[] first = server.first(client.first());
            byte[] last = client.respond(first);
            byte[] goodFinal = server.last(last).serverFinal();
            byte[] given =
                    switch (mode) {
                        case "good" -> goodFinal;
                        case "wrong signature" -> {
                            byte[] b = goodFinal.clone();
                            b[10] = (byte) (b[10] == 'A' ? 'B' : 'A');
                            yield b;
                        }
                        case "no v=" -> bytes(text(goodFinal).substring(2));
                        case "not base64" -> bytes("v=%%%%");
                        case "empty" -> new byte[0];
                        default -> bytes("e=invalid-proof");
                    };
            if (mode.equals("good")) {
                client.verify(given);
            } else {
                assertEquals(
                        ScramException.Kind.SERVER_NOT_VERIFIED,
                        assertThrows(ScramException.class, () -> client.verify(given), mode).kind());
            }
        }
    }

    @Test
    void aServerThatDoesNotKnowTheCredentialCannotProveItself() {
        ScramClient client = new ScramClient("alice", "pw", "cn");
        TestScramServer server = new TestScramServer(u -> ScramCredential.of("another password", SALT, 4096), "sn");
        byte[] last = client.respond(server.first(client.first()));
        assertThrows(TestScramServer.Failed.class, () -> server.last(last), "the proof is wrong");
        // A server that skips the check and answers anything is caught by the client.
        assertThrows(ScramException.class, () -> client.verify(bytes("v=" + Base64.getEncoder().encodeToString(new byte[64]))));
    }

    @Test
    void stepsOutOfOrderAreRefused() {
        ScramClient fresh = new ScramClient("alice", "pw", "cn");
        assertEquals(ScramException.Kind.OUT_OF_ORDER, assertThrows(ScramException.class, () -> fresh.respond(serverFirst("cnx", 4096))).kind());
        assertEquals(ScramException.Kind.OUT_OF_ORDER, assertThrows(ScramException.class, () -> fresh.verify(bytes("v=AA"))).kind());
        ScramClient client = afterFirst("cn");
        client.respond(serverFirst("cnx", 4096));
        assertThrows(ScramException.class, () -> client.respond(serverFirst("cnx", 4096)), "a second respond");
        // A failed step ends the exchange.
        ScramClient failed = afterFirst("cn");
        assertThrows(ScramException.class, () -> failed.respond(serverFirst("cn", 4096)));
        assertThrows(ScramException.class, () -> failed.respond(serverFirst("cnx", 4096)));
    }

    @Test
    void theCredentialForAlterUserHasASaltOfSixteenBytesAndTheMinimumIterations() {
        ScramCredential a = ScramCredential.forPassword("secret", new SecureRandom());
        ScramCredential b = ScramCredential.forPassword("secret", new SecureRandom());
        assertEquals(16, a.salt().length);
        assertEquals(4096, a.iterations());
        assertEquals(64, a.storedKey().length);
        assertEquals(64, a.serverKey().length);
        assertEquals(false, java.util.Arrays.equals(a.salt(), b.salt()), "a fresh salt each time");
        Request.AlterUser request = a.alterUser("bob");
        assertEquals("bob", request.user());
        assertEquals(4096, request.credential().iterations());
        assertArrayEquals(a.storedKey(), request.credential().storedKey().toByteArray());
        assertEquals(null, ScramCredential.removal("bob").credential());
        assertThrows(IllegalArgumentException.class, () -> ScramCredential.of("pw", SALT, 4095));
        assertThrows(IllegalArgumentException.class, () -> ScramCredential.of("pw", SALT, (1 << 20) + 1));
    }

    @Test
    void oauthBearerSendsTheTokenInTheRfc7628Form() {
        assertEquals("n,,\u0001auth=Bearer abc.def\u0001\u0001", text(Credentials.OAuthBearer.initialResponse("abc.def")));
    }

    @Test
    void credentialsDoNotShowTheirSecrets() {
        assertEquals("Scram[user=alice]", Credentials.scram("alice", "hunter2").toString());
        assertEquals(false, Credentials.oauthBearer(() -> "secret-token").toString().contains("secret"));
    }
}
