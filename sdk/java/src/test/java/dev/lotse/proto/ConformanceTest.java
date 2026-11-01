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
            byte[] frame = Vectors.hex(v[1]);
            Frames.DecodedRequest decoded = Frames.decodeRequest(frame);
            long correlationId = Long.parseLong(v[0].split("-")[1]) + 1;
            assertEquals(correlationId, decoded.header().correlationId(), v[0]);
            assertEquals(0, decoded.header().apiVersion(), v[0]);
            assertEquals(0, decoded.header().flags(), v[0]);
            assertEquals(decoded.request().apiKey(), decoded.header().apiKey(), v[0]);
            assertEquals(v[1], hex(Frames.encodeRequest(correlationId, decoded.request())), v[0]);
        }
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
            byte[] frame = Vectors.hex(v[2]);
            ApiKey key = new ApiKey(Integer.parseInt(v[1]));
            Frames.DecodedResponse decoded = Frames.decodeResponse(frame, key);
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
