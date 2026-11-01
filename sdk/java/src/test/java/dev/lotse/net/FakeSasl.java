package dev.lotse.net;

import dev.lotse.auth.Credentials;
import dev.lotse.auth.ScramCredential;
import dev.lotse.auth.TestScramServer;
import dev.lotse.proto.ErrorCode;
import dev.lotse.proto.Request;
import dev.lotse.proto.Response;
import dev.lotse.record.ByteString;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/** The authentication half of a fake broker: SCRAM against a user table, or OAUTHBEARER accepting any token. */
final class FakeSasl {
    static final ScramCredential ALICE =
            ScramCredential.of("pw", "0123456789abcdef".getBytes(StandardCharsets.US_ASCII), 4096);

    final Map<String, ScramCredential> users = Map.of("alice", ALICE);
    final AtomicLong lifetimeMs;
    final AtomicInteger handshakes = new AtomicInteger();
    final AtomicInteger finals = new AtomicInteger();
    final List<String> tokens = new CopyOnWriteArrayList<>();
    /** Called with the number of handshakes so far, when a handshake arrives. */
    volatile java.util.function.IntConsumer onHandshake = n -> {};
    /** Mechanisms the broker offers. */
    volatile List<String> mechanisms = List.of(Credentials.Scram.MECHANISM, Credentials.OAuthBearer.MECHANISM);

    private final Map<FakeBroker.Peer, TestScramServer> servers = new ConcurrentHashMap<>();
    private final Map<FakeBroker.Peer, String> chosen = new ConcurrentHashMap<>();

    FakeSasl(long lifetimeMs) {
        this.lifetimeMs = new AtomicLong(lifetimeMs);
    }

    /** Handles SASL requests; returns false for any other. */
    boolean handle(FakeBroker.Peer peer, dev.lotse.proto.Frames.DecodedRequest decoded) throws Exception {
        long id = decoded.header().correlationId();
        if (decoded.request() instanceof Request.SaslHandshake h) {
            onHandshake.accept(handshakes.incrementAndGet());
            if (mechanisms.contains(h.mechanism())) {
                chosen.put(peer, h.mechanism());
                peer.reply(id, new Response.SaslHandshake(ErrorCode.NONE, mechanisms));
            } else {
                peer.reply(id, new Response.SaslHandshake(ErrorCode.UNSUPPORTED_SASL_MECHANISM, mechanisms));
            }
            return true;
        }
        if (decoded.request() instanceof Request.SaslAuthenticate a) {
            byte[] bytes = a.authBytes().toByteArray();
            if (Credentials.Scram.MECHANISM.equals(chosen.get(peer))) {
                scram(peer, id, bytes);
            } else {
                String message = new String(bytes, StandardCharsets.UTF_8);
                tokens.add(message.substring("n,,\u0001auth=Bearer ".length(), message.length() - 2));
                finals.incrementAndGet();
                peer.reply(id, new Response.SaslAuthenticate(ErrorCode.NONE, null, ByteString.EMPTY, lifetimeMs.get()));
            }
            return true;
        }
        return false;
    }

    private void scram(FakeBroker.Peer peer, long id, byte[] bytes) throws Exception {
        try {
            TestScramServer server = servers.get(peer);
            if (server == null) {
                server = new TestScramServer(users::get, "servernonce");
                servers.put(peer, server);
                byte[] first = server.first(bytes);
                peer.reply(id, new Response.SaslAuthenticate(ErrorCode.NONE, null, ByteString.copyOf(first), 0));
            } else {
                servers.remove(peer);
                TestScramServer.Result r = server.last(bytes);
                finals.incrementAndGet();
                peer.reply(id, new Response.SaslAuthenticate(
                        ErrorCode.NONE, null, ByteString.copyOf(r.serverFinal()), lifetimeMs.get()));
            }
        } catch (TestScramServer.Failed e) {
            servers.remove(peer);
            peer.reply(id, new Response.SaslAuthenticate(
                    ErrorCode.AUTHENTICATION_FAILED, "authentication failed", ByteString.EMPTY, 0));
            peer.closeSocket();
        }
    }
}
