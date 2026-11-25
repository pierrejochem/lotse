package dev.lotse.proto;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.lotse.record.Vectors;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Every frame the broker's codec wrote is decoded here and written again byte for byte. */
class ConformanceTest {
    private static String hex(byte[] b) {
        return Vectors.hex(b);
    }

    @Test
    void everyRequestRoundTrips() {
        List<String[]> vectors = Vectors.lines("requests.hex");
        assertTrue(vectors.size() >= 40, "the vectors cover every client request");
        for (String[] v : vectors) {
            if (tagged(v)) {
                continue; // aHeaderTagRidesOnItsFrame
            }
            byte[] frame = Vectors.hex(v[1]);
            Frames.DecodedRequest decoded = Frames.decodeRequest(frame);
            assertEquals(List.of(), decoded.readAtLeast(), v[0]);
            long correlationId = Long.parseLong(v[0].split("-")[1]) + 1;
            assertEquals(correlationId, decoded.header().correlationId(), v[0]);
            assertEquals(0, decoded.header().apiVersion(), v[0]);
            assertEquals(0, decoded.header().flags(), v[0]);
            assertEquals(decoded.request().apiKey(), decoded.header().apiKey(), v[0]);
            assertEquals(v[1], hex(Frames.encodeRequest(correlationId, decoded.request())), v[0]);
        }
    }

    /** The samples of header tag 0 ride on a frame of their own, labelled {@code -tagged-} with correlation id 900. */
    private static boolean tagged(String[] v) {
        return v[0].contains("-tagged-");
    }

    /**
     * Header tag 0: {@code read_at_least} on a request, {@code applied} on a response, read as the broker wrote them
     * and written again byte for byte; an empty list writes no tag.
     */
    @Test
    void aHeaderTagRidesOnItsFrame() {
        String[] req = Vectors.lines("requests.hex").stream().filter(ConformanceTest::tagged).findFirst().orElseThrow();
        Frames.DecodedRequest r = Frames.decodeRequest(Vectors.hex(req[1]));
        assertEquals(900, r.header().correlationId());
        assertEquals(new Request.DescribeNodes(), r.request());
        List<GroupIndex> wanted = List.of(new GroupIndex(1, 7), new GroupIndex(2, -1L));
        assertEquals(wanted, r.readAtLeast());
        assertEquals(req[1], hex(Frames.encodeRequest(900, wanted, r.request())));
        assertArrayEquals(Frames.encodeRequest(900, r.request()), Frames.encodeRequest(900, List.of(), r.request()));

        String[] res = Vectors.lines("responses.hex").stream().filter(ConformanceTest::tagged).findFirst().orElseThrow();
        ApiKey key = new ApiKey(Integer.parseInt(res[1]));
        Frames.DecodedResponse d = Frames.decodeResponse(Vectors.hex(res[2]), key);
        assertEquals(900, d.correlationId());
        assertEquals(new Response.Outcome(ErrorCode.NONE, null), d.response());
        assertEquals(List.of(new GroupIndex(1, 9)), d.applied());
        assertEquals(res[2], hex(Frames.encodeResponse(900, d.applied(), d.response())));
    }

    /** A header index count larger than the field holds is refused, not trusted. */
    @Test
    void aHeaderIndexCountBeyondTheFieldIsRefused() {
        Writer w = new Writer();
        w.u16(ApiKey.DESCRIBE_NODES.id()).u16(0).u32(1).u16(0);
        Writer v = new Writer();
        v.u32(2).u64(1).u64(7);
        w.tags(List.of(new Tag(0, dev.lotse.record.ByteString.copyOf(v.toByteArray()))));
        new Request.DescribeNodes().write(w);
        DecodeException e = assertThrows(DecodeException.class, () -> Frames.decodeRequest(w.toByteArray()));
        assertEquals(DecodeException.Kind.MALFORMED, e.kind());
    }

    /**
     * The zone reads' message tags: {@code Fetch} tag 1 {@code zone}, a fetched partition's tag 1
     * {@code preferred_replica} and {@code FindCoordinator}'s tag 0 {@code voters}.
     */
    @Test
    void zoneTagsAreReadAsTheBrokerWroteThem() {
        boolean zone = false;
        for (String[] v : Vectors.lines("requests.hex")) {
            if (Frames.decodeRequest(Vectors.hex(v[1])).request() instanceof Request.Fetch f && f.zone() != null) {
                zone = true;
                assertEquals("eu-1", f.zone());
                assertTrue(f.allowFollower());
            }
        }
        boolean preferred = false;
        boolean voters = false;
        for (String[] v : Vectors.lines("responses.hex")) {
            Response r = Frames.decodeResponse(Vectors.hex(v[2]), new ApiKey(Integer.parseInt(v[1]))).response();
            if (r instanceof Response.Fetch f) {
                for (Messages.FetchResourceResponse res : f.resources()) {
                    for (Messages.FetchPartitionResponse p : res.partitions()) {
                        if (p.preferredReplica() != null) {
                            preferred = true;
                            assertEquals(2L, p.preferredReplica());
                        }
                    }
                }
            }
            if (r instanceof Response.FindCoordinator c && !c.voters().isEmpty()) {
                voters = true;
                assertEquals(List.of(1L, 2L, 3L), c.voters());
            }
        }
        assertTrue(zone && preferred && voters);
    }

    @Test
    void everyRequestKeyIsCovered() {
        java.util.Set<Integer> seen = new java.util.TreeSet<>();
        for (String[] v : Vectors.lines("requests.hex")) {
            seen.add(Frames.decodeRequest(Vectors.hex(v[1])).header().apiKey().id());
        }
        for (ApiVersionRange r : ApiKey.IMPLEMENTED) {
            assertTrue(seen.contains(r.apiKey().id()), "no vector for API key " + r.apiKey().id());
        }
    }

    @Test
    void everyResponseRoundTrips() {
        List<String[]> vectors = Vectors.lines("responses.hex");
        assertTrue(vectors.size() >= 40);
        for (String[] v : vectors) {
            if (tagged(v)) {
                continue; // aHeaderTagRidesOnItsFrame
            }
            byte[] frame = Vectors.hex(v[2]);
            ApiKey key = new ApiKey(Integer.parseInt(v[1]));
            Frames.DecodedResponse decoded = Frames.decodeResponse(frame, key);
            assertEquals(List.of(), decoded.applied(), v[0]);
            long correlationId = Long.parseLong(v[0].split("-")[1]);
            assertEquals(correlationId, decoded.correlationId(), v[0]);
            assertEquals(correlationId, Frames.responseCorrelationId(frame), v[0]);
            assertEquals(v[2], hex(Frames.encodeResponse(correlationId, decoded.response())), v[0]);
        }
    }

    @Test
    void everyPushRoundTrips() {
        for (String[] v : Vectors.lines("pushes.hex")) {
            byte[] frame = Vectors.hex(v[1]);
            assertTrue(Frames.isPush(frame), v[0]);
            Frames.DeliverPush push = Frames.decodePush(frame);
            assertEquals(v[1], hex(Frames.encodePush(push)), v[0]);
        }
    }

    @Test
    void thePushLayoutPutsTheSubscriptionIdFirstInTheBody() {
        // correlation 0 | flags PUSH | no tags | api key 11 | version 0 | subscription id | error | ...
        byte[] frame = Vectors.hex(Vectors.lines("pushes.hex").get(0)[1]);
        assertArrayEquals(new byte[] {0, 0, 0, 0, 1, 0, 0, 11, 0, 0, 0, 3, 0, 0, 0, 0, 0}, Arrays.copyOf(frame, 17));
        Frames.DeliverPush push = Frames.decodePush(frame);
        assertEquals(3, push.subscriptionId());
        assertEquals(ErrorCode.NONE, push.error());
        assertEquals(2, push.deliveries().size());
        assertEquals(42, push.deliveries().get(1).offset());
        assertEquals(3, push.deliveries().get(1).deliveryCount());
        Frames.DeliverPush ended = Frames.decodePush(Vectors.hex(Vectors.lines("pushes.hex").get(1)[1]));
        assertEquals(ErrorCode.NOT_LEADER, ended.error());
        assertEquals(new LeaderHint(1, 7), ended.leaderHint());
    }

    @Test
    void aTruncatedFrameIsRefusedAtEveryCut() {
        for (String[] v : Vectors.lines("requests.hex")) {
            byte[] frame = Vectors.hex(v[1]);
            for (int cut = 0; cut < frame.length; cut++) {
                byte[] part = Arrays.copyOf(frame, cut);
                assertThrows(DecodeException.class, () -> Frames.decodeRequest(part), v[0] + " cut at " + cut);
            }
        }
        for (String[] v : Vectors.lines("responses.hex")) {
            byte[] frame = Vectors.hex(v[2]);
            ApiKey key = new ApiKey(Integer.parseInt(v[1]));
            for (int cut = 0; cut < frame.length; cut++) {
                byte[] part = Arrays.copyOf(frame, cut);
                assertThrows(DecodeException.class, () -> Frames.decodeResponse(part, key), v[0] + " cut at " + cut);
            }
        }
        for (String[] v : Vectors.lines("pushes.hex")) {
            byte[] frame = Vectors.hex(v[1]);
            for (int cut = 0; cut < frame.length; cut++) {
                byte[] part = Arrays.copyOf(frame, cut);
                assertThrows(DecodeException.class, () -> Frames.decodePush(part), v[0] + " cut at " + cut);
            }
        }
    }

    @Test
    void aFrameWithTrailingBytesIsRefused() {
        for (String[] v : Vectors.lines("requests.hex")) {
            byte[] longer = Arrays.copyOf(Vectors.hex(v[1]), Vectors.hex(v[1]).length + 1);
            DecodeException e = assertThrows(DecodeException.class, () -> Frames.decodeRequest(longer), v[0]);
            assertEquals(DecodeException.Kind.MALFORMED, e.kind(), v[0]);
        }
        for (String[] v : Vectors.lines("responses.hex")) {
            byte[] frame = Vectors.hex(v[2]);
            byte[] longer = Arrays.copyOf(frame, frame.length + 1);
            ApiKey key = new ApiKey(Integer.parseInt(v[1]));
            DecodeException e = assertThrows(DecodeException.class, () -> Frames.decodeResponse(longer, key), v[0]);
            assertEquals(DecodeException.Kind.MALFORMED, e.kind(), v[0]);
        }
        for (String[] v : Vectors.lines("pushes.hex")) {
            byte[] frame = Vectors.hex(v[1]);
            byte[] longer = Arrays.copyOf(frame, frame.length + 1);
            assertThrows(DecodeException.class, () -> Frames.decodePush(longer), v[0]);
        }
    }

    @Test
    void errorCodesHaveTheBrokersNamesAndRetryClasses() {
        for (String[] v : Vectors.lines("error-codes.txt")) {
            ErrorCode code = new ErrorCode(Integer.parseInt(v[0]));
            assertEquals(v[1], code.codeName(), v[0]);
            assertEquals(Boolean.parseBoolean(v[2]), code.retriable(), v[0]);
        }
    }

    @Test
    void theErrorFlagIsCheckedBeforeTheTypedBody() {
        // A response with the ERROR flag whose body would be nonsense as a Produce response.
        for (String[] v : Vectors.lines("responses.hex")) {
            byte[] frame = Vectors.hex(v[2]);
            boolean error = (frame[4] & Flags.ERROR) != 0;
            if (!error) {
                continue;
            }
            for (ApiKey key : new ApiKey[] {ApiKey.PRODUCE, ApiKey.FETCH, ApiKey.METADATA, ApiKey.DESCRIBE_METRICS}) {
                Response r = Frames.decodeResponse(frame, key).response();
                assertTrue(r instanceof Response.Error || r instanceof Response.Refused, v[0]);
            }
        }
        // And without the flag the same bytes are read as the typed body, which fails for the wrong key.
        byte[] frame = Vectors.hex(Vectors.lines("responses.hex").stream()
                .filter(v -> v[0].endsWith("Error")).findFirst().orElseThrow()[2]);
        frame[4] = 0;
        assertThrows(DecodeException.class, () -> Frames.decodeResponse(frame, ApiKey.PRODUCE));
    }

    @Test
    void aRefusalCarriesItsReason() {
        boolean found = false;
        for (String[] v : Vectors.lines("responses.hex")) {
            Response r = Frames.decodeResponse(Vectors.hex(v[2]), new ApiKey(Integer.parseInt(v[1]))).response();
            if (r instanceof Response.Refused refused) {
                found = true;
                assertEquals(ErrorCode.UNAUTHORIZED, refused.code());
                assertEquals("no producer role for orders in shop", refused.message());
            }
        }
        assertTrue(found);
    }

    /**
     * The access token messages (ADR 0133), keys 66 to 68, read into the values the broker's samples hold; the
     * round trip byte for byte is {@link #everyRequestRoundTrips} and {@link #everyResponseRoundTrips}.
     */
    @Test
    void tokenMessagesReadAsTheBrokerWroteThem() {
        List<Request> requests = new java.util.ArrayList<>();
        for (String[] v : Vectors.lines("requests.hex")) {
            Request r = Frames.decodeRequest(Vectors.hex(v[1])).request();
            if (r instanceof Request.CreateToken || r instanceof Request.DescribeTokens
                    || r instanceof Request.RevokeToken) {
                requests.add(r);
            }
        }
        byte[] hash = new byte[32];
        Arrays.fill(hash, (byte) 0xab);
        assertEquals(
                List.of(
                        new Request.CreateToken("ci", "alice", 2, dev.lotse.record.ByteString.copyOf(hash)),
                        new Request.DescribeTokens(null),
                        new Request.DescribeTokens("alice"),
                        new Request.RevokeToken(41)),
                requests);

        List<Response> responses = new java.util.ArrayList<>();
        for (String[] v : Vectors.lines("responses.hex")) {
            int key = Integer.parseInt(v[1]);
            if (key >= 66 && key <= 68) {
                responses.add(Frames.decodeResponse(Vectors.hex(v[2]), new ApiKey(key)).response());
            }
        }
        assertEquals(
                List.of(
                        new Response.CreateToken(ErrorCode.NONE, null, 41, 1_757_776_000_000L, null),
                        new Response.CreateToken(ErrorCode.NOT_LEADER, null, 0, 0, new LeaderHint(2, 7)),
                        new Response.DescribeTokens(
                                ErrorCode.NONE,
                                null,
                                List.of(
                                        new Messages.TokenEntry(
                                                41, "ci", "alice", 1_749_995_000_000L, 1_757_776_000_000L, "admin"),
                                        new Messages.TokenEntry(
                                                42, "laptop", "bob", 1_749_996_000_000L, 1_752_588_000_000L, "bob"))),
                        new Response.Admin(ErrorCode.NONE, null, null)),
                responses);
    }

    /** Tag 0 of a session entry: the id of the access token it signed in with, only for mechanism 4 (ADR 0133). */
    @Test
    void aSessionNamesItsTokenId() {
        boolean found = false;
        for (String[] v : Vectors.lines("responses.hex")) {
            Response r = Frames.decodeResponse(Vectors.hex(v[2]), new ApiKey(Integer.parseInt(v[1]))).response();
            if (r instanceof Response.DescribeSessions d) {
                found = true;
                assertEquals(3, d.sessions().size());
                for (Messages.SessionEntry s : d.sessions()) {
                    boolean token = s.mechanism() == Messages.SessionMechanism.TOKEN;
                    assertEquals(token ? Long.valueOf(41) : null, s.tokenId(), s.toString());
                }
                assertEquals(Messages.SessionMechanism.TOKEN, d.sessions().get(1).mechanism());
            }
        }
        assertTrue(found);
    }

    /** Tags 0 and 1 of a partition in a metadata answer: its pinned leader and placement (ADR 0121). */
    @Test
    void pinsAndPlacementsAreReadFromTheirPartitions() {
        boolean found = false;
        for (String[] v : Vectors.lines("responses.hex")) {
            Response r = Frames.decodeResponse(Vectors.hex(v[2]), new ApiKey(Integer.parseInt(v[1]))).response();
            if (r instanceof Response.Metadata m) {
                for (Messages.ResourceMetadata res : m.resources()) {
                    for (Messages.PartitionMetadata p : res.partitions()) {
                        // Only partition 0 of "orders" has a pin (node 2) and a placement.
                        boolean set = res.name().equals("orders") && p.partition() == 0;
                        found |= set;
                        assertEquals(set ? 2L : 0L, p.pinnedLeader());
                        assertEquals(set, p.placed());
                    }
                }
            }
        }
        assertTrue(found);
    }

    /** Tag 3 of a metrics answer: ready messages per priority level and the subscriptions' states (ADR 0120). */
    @Test
    void queueDetailsAreReadIntoTheirQueues() {
        boolean found = false;
        for (String[] v : Vectors.lines("responses.hex")) {
            Response r = Frames.decodeResponse(Vectors.hex(v[2]), new ApiKey(Integer.parseInt(v[1]))).response();
            if (r instanceof Response.DescribeMetrics m && !m.queues().isEmpty()) {
                found = true;
                Messages.QueueMetrics q = m.queues().get(0);
                assertEquals(java.util.List.of(2L, 1L, 1L), q.readyByLevel());
                assertEquals(1L, q.readyLevelUnread());
                assertEquals(Messages.ConsumerState.ACTIVE, q.consumers().get(0).state());
                assertEquals(Messages.ConsumerState.STANDBY, q.consumers().get(1).state());
                assertEquals("10.0.0.9:40100", q.consumers().get(1).peer());
                // Tags 4 to 6 (ADR 0122).
                Messages.PartitionMetrics p = m.partitions().get(0);
                assertEquals(Long.valueOf(1_763_000_000_000L), p.oldestTimestampMs());
                assertEquals(1L, p.openTransactions());
                assertEquals(2048L, q.consumers().get(0).inFlightBytes());
                assertEquals(0L, q.consumers().get(1).inFlightBytes());
                assertEquals(new Messages.DiskMetrics(1L << 30, true), m.disk());
            }
        }
        assertTrue(found);
    }

    @Test
    void leaderHintsAreDecodedWhereTheyAreTagBlocks() {
        boolean produce = false;
        boolean describe = false;
        for (String[] v : Vectors.lines("responses.hex")) {
            Response r = Frames.decodeResponse(Vectors.hex(v[2]), new ApiKey(Integer.parseInt(v[1]))).response();
            if (r instanceof Response.Produce p) {
                for (Messages.ProduceResourceResponse res : p.resources()) {
                    for (Messages.ProducePartitionResponse part : res.partitions()) {
                        if (part.error().equals(ErrorCode.NOT_LEADER)) {
                            assertEquals(new LeaderHint(2, 9), part.leaderHint());
                            produce = true;
                        } else {
                            assertEquals(null, part.leaderHint());
                        }
                    }
                }
            }
            if (r instanceof Response.DescribeRoleBindings d && d.leaderHint() != null) {
                assertEquals(new LeaderHint(3, 9), d.leaderHint());
                describe = true;
            }
        }
        assertTrue(produce && describe);
        assertFalse(Frames.isPush(Vectors.hex(Vectors.lines("responses.hex").get(0)[2])));
    }
}
