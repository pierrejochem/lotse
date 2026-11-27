package dev.lotse.proto;

import dev.lotse.record.ByteString;
import java.util.List;

/**
 * Frames: headers, the length prefix, and encoding and decoding of whole requests, responses and pushes.
 *
 * <p>On a connection every frame is preceded by its length as a {@code u32} in <b>big-endian</b> order; use
 * {@link #withLengthPrefix} and {@link #readLength}. Everything inside the frame is little-endian. The largest
 * frame is {@link #MAX_FRAME} bytes.
 *
 * <pre>
 * Request:  api_key u16 | api_version u16 | correlation_id u32 | flags u16 | tagged_fields | body
 * Response: correlation_id u32 | flags u16 | tagged_fields | body
 * Push:     correlation_id u32 (0) | flags u16 (PUSH) | tagged_fields | api_key u16 | api_version u16 | body
 * </pre>
 *
 * <p>The header's tagged fields have one tag, 0: {@code read_at_least} in a request and {@code applied} in a
 * response, each a list of {@link GroupIndex}. A frame without it reads as an empty list.
 *
 * <p>Check the {@link Flags#ERROR} flag of a response <em>before</em> decoding a typed body: such a response
 * carries only an error code (and optionally a reason), whatever the request was. {@link #decodeResponse} does.
 * Responses on a connection may come back in another order than the requests; match them by correlation id.
 */
public final class Frames {
    /** The largest frame a node sends or accepts: 16 MiB. */
    public static final int MAX_FRAME = 16 * 1024 * 1024;

    private Frames() {}

    /**
     * A decoded request frame.
     *
     * @param header the header
     * @param request the request
     * @param readAtLeast header tag 0: the indexes the answer must reflect, empty without the tag
     */
    public record DecodedRequest(RequestHeader header, Request request, List<GroupIndex> readAtLeast) {
        /** Creates the frame. */
        public DecodedRequest {
            readAtLeast = List.copyOf(readAtLeast);
        }

        /** A frame without header tags. */
        public DecodedRequest(RequestHeader header, Request request) {
            this(header, request, List.of());
        }
    }

    /**
     * A decoded response frame.
     *
     * @param correlationId the request it answers
     * @param response the response
     * @param applied header tag 0: the indexes the node had applied when it answered, empty without the tag
     */
    public record DecodedResponse(long correlationId, Response response, List<GroupIndex> applied) {
        /** Creates the frame. */
        public DecodedResponse {
            applied = List.copyOf(applied);
        }

        /** A frame without header tags. */
        public DecodedResponse(long correlationId, Response response) {
            this(correlationId, response, List.of());
        }
    }

    // ---- header tags ----

    /** Header tag 0: {@code read_at_least} in a request, {@code applied} in a response. */
    static final long TAG_HEADER_INDEXES = 0;

    /** Writes a header's tags: none for an empty list, else tag 0 with a {@code u32} count and the entries. */
    private static void putHeaderTags(Writer w, List<GroupIndex> indexes) {
        if (indexes.isEmpty()) {
            w.noTags();
            return;
        }
        Writer v = new Writer();
        v.u32(indexes.size());
        for (GroupIndex e : indexes) {
            v.u64(e.group()).u64(e.index());
        }
        w.tags(List.of(new Tag(TAG_HEADER_INDEXES, ByteString.copyOf(v.toByteArray()))));
    }

    /** The indexes of header tag 0; other tags come from newer versions and are skipped. */
    private static List<GroupIndex> getHeaderTags(List<Tag> raw) {
        List<GroupIndex> out = new java.util.ArrayList<>();
        for (Tag t : raw) {
            if (t.tag() != TAG_HEADER_INDEXES) {
                continue;
            }
            Reader v = new Reader(t.value().toByteArray());
            long n = v.u32();
            // Each entry takes sixteen bytes, so a count beyond what is left is a lie.
            if (n > v.remaining() / 16) {
                throw DecodeException.malformed("index count exceeds the field");
            }
            for (long i = 0; i < n; i++) {
                out.add(new GroupIndex(v.u64(), v.u64()));
            }
            v.finish();
        }
        return out;
    }

    // ---- length prefix ----

    /** The frame preceded by its length as a big-endian {@code u32}, ready to write to the connection. */
    public static byte[] withLengthPrefix(byte[] frame) {
        if (frame.length > MAX_FRAME) {
            throw new IllegalArgumentException("frame of " + frame.length + " bytes exceeds the limit");
        }
        byte[] out = new byte[4 + frame.length];
        out[0] = (byte) (frame.length >>> 24);
        out[1] = (byte) (frame.length >>> 16);
        out[2] = (byte) (frame.length >>> 8);
        out[3] = (byte) frame.length;
        System.arraycopy(frame, 0, out, 4, frame.length);
        return out;
    }

    /**
     * Reads the length prefix of {@code prefix[0, 4)}, a big-endian {@code u32}.
     *
     * @throws DecodeException if it exceeds {@link #MAX_FRAME}; the connection cannot go on
     */
    public static int readLength(byte[] prefix) {
        long len = ((prefix[0] & 0xFFL) << 24) | ((prefix[1] & 0xFF) << 16) | ((prefix[2] & 0xFF) << 8) | (prefix[3] & 0xFF);
        if (len > MAX_FRAME) {
            throw new DecodeException(
                    DecodeException.Kind.MALFORMED, "frame of " + len + " bytes exceeds the limit");
        }
        return (int) len;
    }

    // ---- requests ----

    /** Encodes a version-0 request. {@code correlationId} is a {@code u32} and must not be 0, which is reserved for pushes. */
    public static byte[] encodeRequest(long correlationId, Request request) {
        return encodeRequest(correlationId, List.of(), request);
    }

    /**
     * Encodes a version-0 request with header tag 0, {@code read_at_least}: a read the node may serve only once it has
     * applied at least these indexes. An empty list writes no tag, so the frame is the same as without it.
     */
    public static byte[] encodeRequest(long correlationId, List<GroupIndex> readAtLeast, Request request) {
        Writer w = new Writer();
        w.u16(request.apiKey().id());
        w.u16(0);
        w.u32(correlationId);
        w.u16(0);
        putHeaderTags(w, readAtLeast);
        request.write(w);
        return w.toByteArray();
    }

    /**
     * Decodes a request. A header this library does not speak gives a {@link DecodeException} of kind
     * {@link DecodeException.Kind#UNSUPPORTED} that carries the header, so the caller can still answer.
     */
    public static DecodedRequest decodeRequest(byte[] frame) {
        Reader r = new Reader(frame);
        RequestHeader header = new RequestHeader(new ApiKey(r.u16()), r.u16(), r.u32(), r.u16());
        // The tags' content is read after the support check, so a header this library does not speak still gives
        // UNSUPPORTED and the caller can answer.
        List<Tag> rawTags = r.tags();
        if (header.apiVersion() != 0 || !ApiKey.isClientKey(header.apiKey())) {
            throw new DecodeException(
                    DecodeException.Kind.UNSUPPORTED,
                    "unsupported API key " + header.apiKey().id() + " version " + header.apiVersion(),
                    header);
        }
        List<GroupIndex> readAtLeast = getHeaderTags(rawTags);
        Request request;
        switch (header.apiKey().id()) {
            case 0 -> request = Request.ApiVersions.read(r);
            case 1 -> request = Request.Metadata.read(r);
            case 2 -> request = Request.FindCoordinator.read(r);
            case 3 -> request = Request.Produce.read(r);
            case 4 -> request = Request.Fetch.read(r);
            case 5 -> request = Request.ListOffsets.read(r);
            case 6 -> request = Request.GroupHeartbeat.read(r);
            case 7 -> request = Request.CommitOffsets.read(r);
            case 8 -> request = Request.OffsetFetch.read(r);
            case 10 -> request = Request.Subscribe.read(r);
            case 12 -> request = Request.Credit.read(r);
            case 13 -> request = Request.Ack.read(r);
            case 14 -> request = Request.Nack.read(r);
            case 15 -> request = Request.Unsubscribe.read(r);
            case 16 -> request = Request.InitProducer.read(r);
            case 17 -> request = Request.AddPartitionsToTxn.read(r);
            case 19 -> request = Request.AddOffsetsToTxn.read(r);
            case 20 -> request = Request.EndTxn.read(r);
            case 21 -> request = Request.CreateStream.read(r);
            case 22 -> request = Request.CreateQueue.read(r);
            case 23 -> request = Request.DeclareExchange.read(r);
            case 24 -> request = Request.Bind.read(r);
            case 25 -> request = Request.DeleteResource.read(r);
            case 26 -> request = Request.DescribeConfigs.read(r);
            case 27 -> request = Request.AlterConfigs.read(r);
            case 28 -> request = Request.Unbind.read(r);
            case 29 -> request = Request.DescribeMetrics.read(r);
            case 30 -> request = Request.SaslHandshake.read(r);
            case 31 -> request = Request.SaslAuthenticate.read(r);
            case 32 -> request = Request.AlterUser.read(r);
            case 33 -> request = Request.DescribeUsers.read(r);
            case 34 -> request = Request.CreateTenant.read(r);
            case 35 -> request = Request.DeleteTenant.read(r);
            case 36 -> request = Request.DescribeTenants.read(r);
            case 37 -> request = Request.AlterRoleBindings.read(r);
            case 38 -> request = Request.DescribeRoleBindings.read(r);
            case 39 -> request = Request.WhoAmI.read(r);
            case 40 -> request = Request.AddNode.read(r);
            case 41 -> request = Request.RemoveNode.read(r);
            case 42 -> request = Request.DescribeNodes.read(r);
            case 47 -> request = Request.QueueAction.read(r, false);
            case 48 -> request = Request.QueueAction.read(r, true);
            case 49 -> request = Request.MoveLeader.read(r);
            case 50 -> request = Request.RevokeSessions.read(r);
            case 51 -> request = Request.CreatePartitions.read(r);
            case 52 -> request = Request.RevokeCertificate.read(r);
            case 53 -> request = Request.DescribeCertificates.read(r);
            case 54 -> request = Request.RecordCertificates.read(r);
            case 55 -> request = Request.RequestCertificate.read(r);
            case 56 -> request = Request.DescribeCertificateRequests.read(r);
            case 57 -> request = Request.RecordCertificate.read(r);
            case 58 -> request = Request.ReassignPartition.read(r);
            case 59 -> request = Request.DescribeSessions.read(r);
            case 60 -> request = Request.RenewCertificate.read(r);
            case 64 -> request = Request.AlterUpgrade.read(r);
            case 65 -> request = Request.DescribeUpgrade.read(r);
            case 66 -> request = Request.CreateToken.read(r);
            case 67 -> request = Request.DescribeTokens.read(r);
            case 68 -> request = Request.RevokeToken.read(r);
            case 69 -> request = Request.AlterQuota.read(r);
            case 70 -> request = Request.DescribeQuotas.read(r);
            case 9 -> request = Request.Publish.read(r);
            default -> throw new IllegalStateException("checked against the implemented keys");
        }
        r.finish();
        return new DecodedRequest(header, request, readAtLeast);
    }

    // ---- responses ----

    /** Encodes a response to the request with {@code correlationId}. */
    public static byte[] encodeResponse(long correlationId, Response response) {
        return encodeResponse(correlationId, List.of(), response);
    }

    /**
     * Encodes a response with header tag 0, {@code applied}: the indexes the node had applied when it answered. An
     * empty list writes no tag.
     */
    public static byte[] encodeResponse(long correlationId, List<GroupIndex> applied, Response response) {
        Writer w = new Writer();
        w.u32(correlationId);
        w.u16(response instanceof Response.Error || response instanceof Response.Refused ? Flags.ERROR : 0);
        putHeaderTags(w, applied);
        response.write(w);
        return w.toByteArray();
    }

    /** The correlation id of a response frame, to find the request it answers. */
    public static long responseCorrelationId(byte[] frame) {
        if (frame.length < 4) {
            throw DecodeException.truncated();
        }
        return new Reader(frame, 0, 4).u32();
    }

    /**
     * Decodes a response to a request of type {@code apiKey}. A frame with the {@link Flags#ERROR} flag becomes
     * a {@link Response.Error} or, when it carries a reason, a {@link Response.Refused}, whatever the key is.
     */
    public static DecodedResponse decodeResponse(byte[] frame, ApiKey apiKey) {
        Reader r = new Reader(frame);
        long correlationId = r.u32();
        int flags = r.u16();
        List<GroupIndex> applied = getHeaderTags(r.tags());
        if ((flags & Flags.ERROR) != 0) {
            ErrorCode code = new ErrorCode(r.u16());
            ByteString reason = null;
            for (Tag t : r.tags()) {
                if (t.tag() == Messages.TAG_REASON) {
                    reason = t.value();
                    break;
                }
            }
            r.finish();
            if (reason == null) {
                return new DecodedResponse(correlationId, new Response.Error(code), applied);
            }
            return new DecodedResponse(correlationId, new Response.Refused(code, strictUtf8(reason)), applied);
        }
        Response response;
        switch (apiKey.id()) {
            case 0 -> response = Response.ApiVersions.read(r);
            case 1 -> response = Response.Metadata.read(r);
            case 2 -> response = Response.FindCoordinator.read(r);
            case 16 -> response = Response.InitProducer.read(r);
            case 17 -> response = Response.AddPartitionsToTxn.read(r);
            case 20 -> response = Response.EndTxn.read(r);
            case 10 -> response = Response.Subscribe.read(r);
            case 13, 14, 15, 24, 28, 44 -> response = Response.Outcome.read(r);
            case 21, 22, 23 -> response = Response.Create.read(r);
            case 25, 27, 32, 34, 35, 37, 40, 41, 43, 49, 50, 51, 52, 54, 55, 57, 58, 64, 68, 69 -> response =
                    Response.Admin.read(r);
            case 42 -> response = Response.DescribeNodes.read(r);
            case 26 -> response = Response.DescribeConfigs.read(r);
            case 29 -> response = Response.DescribeMetrics.read(r);
            case 30 -> response = Response.SaslHandshake.read(r);
            case 31 -> response = Response.SaslAuthenticate.read(r);
            case 33 -> response = Response.DescribeUsers.read(r);
            case 53 -> response = Response.DescribeCertificates.read(r);
            case 59 -> response = Response.DescribeSessions.read(r);
            case 60 -> response = Response.RenewCertificate.read(r);
            case 65 -> response = Response.DescribeUpgrade.read(r);
            case 66 -> response = Response.CreateToken.read(r);
            case 67 -> response = Response.DescribeTokens.read(r);
            case 70 -> response = Response.DescribeQuotas.read(r);
            case 56 -> response = Response.DescribeCertificateRequests.read(r);
            case 36 -> response = Response.DescribeTenants.read(r);
            case 38 -> response = Response.DescribeRoleBindings.read(r);
            case 39 -> response = Response.WhoAmI.read(r);
            case 6 -> response = Response.GroupHeartbeat.read(r);
            case 7, 19 -> response = Response.CommitOffsets.read(r);
            case 8 -> response = Response.OffsetFetch.read(r);
            case 3 -> response = Response.Produce.read(r);
            case 4 -> response = Response.Fetch.read(r);
            case 5 -> response = Response.ListOffsets.read(r);
            case 47, 48 -> response = Response.QueueAction.read(r);
            case 9 -> response = Response.Publish.read(r);
            default -> throw DecodeException.malformed("unknown API key");
        }
        r.finish();
        return new DecodedResponse(correlationId, response, applied);
    }

    private static String strictUtf8(ByteString s) {
        try {
            return java.nio.charset.StandardCharsets.UTF_8
                    .newDecoder()
                    .onMalformedInput(java.nio.charset.CodingErrorAction.REPORT)
                    .onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPORT)
                    .decode(java.nio.ByteBuffer.wrap(s.toByteArray()))
                    .toString();
        } catch (java.nio.charset.CharacterCodingException e) {
            throw DecodeException.malformed("reason is not UTF-8");
        }
    }

    // ---- pushes ----

    /**
     * A delivery to a queue consumer.
     *
     * @param subscriptionId the subscription, a {@code u32}
     * @param error {@code NONE} for a delivery; otherwise why the subscription ended (it then carries no batch)
     * @param batch one record batch from the log, unchanged
     * @param deliveries the offsets in {@code batch} delivered to this subscription
     * @param leaderHint the queue's leader, after {@code NOT_LEADER}, or {@code null}
     */
    public record DeliverPush(
            long subscriptionId,
            ErrorCode error,
            ByteString batch,
            List<Messages.DeliveredOffset> deliveries,
            LeaderHint leaderHint) {
        /** Creates the push. */
        public DeliverPush {
            deliveries = List.copyOf(deliveries);
        }

        /**
         * Whether this push says the subscription became its queue's single active consumer (ADR 0076): no error, no
         * batch, no deliveries. Only a subscription that asked for it gets one.
         */
        public boolean isActivation() {
            return error.code() == 0 && batch.size() == 0 && deliveries.isEmpty();
        }
    }

    /**
     * Encodes a push frame: correlation id 0, flag {@link Flags#PUSH}, an empty block of tagged fields, then the
     * API key ({@link ApiKey#DELIVER}) and version 0 and the body. The subscription id is the first field of the
     * body.
     */
    public static byte[] encodePush(DeliverPush push) {
        Writer w = new Writer();
        w.u32(0);
        w.u16(Flags.PUSH);
        w.noTags();
        w.u16(ApiKey.DELIVER.id());
        w.u16(0);
        w.u32(push.subscriptionId());
        w.u16(push.error().code());
        w.bytes(push.batch());
        w.array(push.deliveries(), (w1, d) -> w1.u64(d.offset()).u16(d.deliveryCount()));
        Messages.putHint(w, push.leaderHint());
        return w.toByteArray();
    }

    /** Whether a frame from the broker is a push rather than a response: correlation id 0 and the push flag. */
    public static boolean isPush(byte[] frame) {
        return frame.length >= 6
                && frame[0] == 0
                && frame[1] == 0
                && frame[2] == 0
                && frame[3] == 0
                && (((frame[4] & 0xFF) | (frame[5] & 0xFF) << 8) & Flags.PUSH) != 0;
    }

    /** Decodes a push frame. */
    public static DeliverPush decodePush(byte[] frame) {
        Reader r = new Reader(frame);
        long correlationId = r.u32();
        int flags = r.u16();
        if (correlationId != 0 || (flags & Flags.PUSH) == 0) {
            throw DecodeException.malformed("not a push frame");
        }
        r.tags();
        if (r.u16() != ApiKey.DELIVER.id() || r.u16() != 0) {
            throw DecodeException.malformed("unknown push");
        }
        long subscriptionId = r.u32();
        ErrorCode error = new ErrorCode(r.u16());
        ByteString batch = r.bytes();
        List<Messages.DeliveredOffset> deliveries =
                r.array(10, r1 -> new Messages.DeliveredOffset(r1.u64(), r1.u16()));
        LeaderHint hint = Messages.getHint(r);
        r.finish();
        return new DeliverPush(subscriptionId, error, batch, deliveries, hint);
    }
}
