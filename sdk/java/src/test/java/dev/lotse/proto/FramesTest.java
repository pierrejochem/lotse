package dev.lotse.proto;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.lotse.record.ByteString;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class FramesTest {
    @Test
    void theLengthPrefixIsBigEndianAndTheRestLittleEndian() {
        byte[] frame = Frames.encodeRequest(0x01020304L, new Request.ApiVersions());
        // api key 0, version 0, correlation 04 03 02 01 (little-endian), flags 0, empty tags, empty body tags
        assertArrayEquals(new byte[] {0, 0, 0, 0, 4, 3, 2, 1, 0, 0, 0, 0}, frame);
        byte[] wire = Frames.withLengthPrefix(frame);
        assertArrayEquals(new byte[] {0, 0, 0, 12}, Arrays.copyOf(wire, 4));
        assertEquals(12, Frames.readLength(wire));
        assertEquals(0x010203, Frames.readLength(new byte[] {0, 1, 2, 3}), "big-endian: the first byte is the most significant");
        assertEquals(256, Frames.readLength(new byte[] {0, 0, 1, 0}));
    }

    @Test
    void aLengthOverTheLimitEndsTheConnection() {
        assertEquals(Frames.MAX_FRAME, Frames.readLength(new byte[] {1, 0, 0, 0}));
        assertThrows(DecodeException.class, () -> Frames.readLength(new byte[] {1, 0, 0, 1}));
        assertThrows(DecodeException.class, () -> Frames.readLength(new byte[] {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff}));
    }

    @Test
    void anUnknownKeyOrVersionIsReportedWithItsHeader() {
        byte[] frame = Frames.encodeRequest(77, new Request.ApiVersions());
        frame[2] = 9; // version 9
        DecodeException e = assertThrows(DecodeException.class, () -> Frames.decodeRequest(frame));
        assertEquals(DecodeException.Kind.UNSUPPORTED, e.kind());
        assertEquals(77, e.header().correlationId());
        assertEquals(9, e.header().apiVersion());
        byte[] other = Frames.encodeRequest(5, new Request.ApiVersions());
        other[0] = 5; // ListOffsets: not yet
        assertEquals(DecodeException.Kind.UNSUPPORTED, assertThrows(DecodeException.class, () -> Frames.decodeRequest(other)).kind());
    }

    @Test
    void theNodeInternalKeysAreOnlyConstants() {
        for (ApiKey key : List.of(ApiKey.SETTLE_VOTERS, ApiKey.DEAD_LETTER, ApiKey.WRITE_TXN_MARKERS)) {
            assertFalse(key.isImplemented(0));
            byte[] frame = Frames.encodeRequest(1, new Request.ApiVersions());
            frame[0] = (byte) key.id();
            frame[1] = (byte) (key.id() >> 8);
            assertEquals(DecodeException.Kind.UNSUPPORTED, assertThrows(DecodeException.class, () -> Frames.decodeRequest(frame)).kind());
        }
        assertEquals(43, ApiKey.SETTLE_VOTERS.id());
        assertEquals(44, ApiKey.DEAD_LETTER.id());
        assertEquals(1000, ApiKey.WRITE_TXN_MARKERS.id());
    }

    @Test
    void unknownTagsAreSkipped() {
        Writer w = new Writer();
        w.u32(1).u16(0);
        w.tags(List.of(new Tag(42, ByteString.utf8("from the future"))));
        new Response.Metadata(List.of(), List.of()).write(w);
        Frames.DecodedResponse r = Frames.decodeResponse(w.toByteArray(), ApiKey.METADATA);
        assertEquals(1, r.correlationId());
        assertEquals(new Response.Metadata(List.of(), List.of()), r.response());
    }

    @Test
    void aPushIsToldFromAResponse() {
        byte[] response = Frames.encodeResponse(5, new Response.Outcome(ErrorCode.NONE, null));
        assertFalse(Frames.isPush(response));
        assertFalse(Frames.isPush(new byte[] {0, 0, 0, 0, 2}));
        assertFalse(Frames.isPush(new byte[] {0, 0, 0, 0, 2, 0}), "ERROR is not PUSH");
        assertTrue(Frames.isPush(new byte[] {0, 0, 0, 0, 1, 0}));
        assertTrue(Frames.isPush(new byte[] {0, 0, 0, 0, 3, 0}));
        for (int i = 0; i < 4; i++) {
            byte[] frame = {0, 0, 0, 0, 1, 0};
            frame[i] = 1;
            assertFalse(Frames.isPush(frame), "a nonzero correlation id is a response, byte " + i);
        }
        assertThrows(DecodeException.class, () -> Frames.decodePush(response));
    }

    @Test
    void aPushNeedsItsFlagAndAZeroCorrelationId() {
        byte[] push = Frames.encodePush(new Frames.DeliverPush(1, ErrorCode.NONE, ByteString.EMPTY, List.of(), null));
        assertEquals(1, Frames.decodePush(push).subscriptionId());
        for (int flags : new int[] {0, 2}) {
            byte[] bad = push.clone();
            bad[4] = (byte) flags;
            assertThrows(DecodeException.class, () -> Frames.decodePush(bad), "flags " + flags);
        }
        byte[] bad = push.clone();
        bad[0] = 1;
        assertThrows(DecodeException.class, () -> Frames.decodePush(bad));
    }

    @Test
    void correlationIdsUseTheWholeU32Range() {
        byte[] frame = Frames.encodeRequest(0xFFFF_FFFFL, new Request.WhoAmI());
        assertEquals(0xFFFF_FFFFL, Frames.decodeRequest(frame).header().correlationId());
        assertThrows(IllegalArgumentException.class, () -> Frames.encodeRequest(1L << 32, new Request.WhoAmI()));
        assertEquals(0xFFFF_FFFFL, Frames.responseCorrelationId(Frames.encodeResponse(0xFFFF_FFFFL, new Response.Error(ErrorCode.UNAUTHORIZED))));
    }

    @Test
    void anErrorResponseIsOnlyACodeAndATagBlock() {
        byte[] frame = Frames.encodeResponse(9, new Response.Error(ErrorCode.UNSUPPORTED_VERSION));
        // correlation 9, flags ERROR, no tags, code 14, no tags
        assertArrayEquals(new byte[] {9, 0, 0, 0, 2, 0, 0, 14, 0, 0}, frame);
        assertEquals(new Response.Error(ErrorCode.UNSUPPORTED_VERSION), Frames.decodeResponse(frame, ApiKey.FETCH).response());
    }

    @Test
    void u64ValuesKeepTheirBitsAndNoOffsetIsAllOnes() {
        Messages.OffsetFetchPartition p =
                new Messages.OffsetFetchPartition(0, ErrorCode.NONE, Messages.NO_OFFSET, 0, null);
        byte[] frame = Frames.encodeResponse(
                1, new Response.OffsetFetch(List.of(new Messages.OffsetFetchResource(7, List.of(p)))));
        Response.OffsetFetch back = (Response.OffsetFetch) Frames.decodeResponse(frame, ApiKey.OFFSET_FETCH).response();
        assertEquals(-1L, back.resources().get(0).partitions().get(0).offset());
        assertEquals("18446744073709551615", dev.lotse.record.Unsigned.toString(Messages.NO_OFFSET));
        assertNull(back.resources().get(0).partitions().get(0).metadata());
    }
}
