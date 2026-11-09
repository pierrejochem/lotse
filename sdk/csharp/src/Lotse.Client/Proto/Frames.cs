using System;
using System.Collections.Generic;
using System.Text;
using Lotse.Record;

namespace Lotse.Proto;

/// <summary>
/// Frames: headers, the length prefix, and encoding and decoding of whole requests, responses and pushes.
/// <para>
/// On a connection every frame is preceded by its length as a <c>u32</c> in <b>big-endian</b> order; use
/// <see cref="WithLengthPrefix"/> and <see cref="ReadLength"/>. Everything inside the frame is little-endian. The
/// largest frame is <see cref="MaxFrame"/> bytes.
/// </para>
/// <code>
/// Request:  api_key u16 | api_version u16 | correlation_id u32 | flags u16 | tagged_fields | body
/// Response: correlation_id u32 | flags u16 | tagged_fields | body
/// Push:     correlation_id u32 (0) | flags u16 (PUSH) | tagged_fields | api_key u16 | api_version u16 | body
/// </code>
/// <para>
/// Check the error flag of a response <em>before</em> decoding a typed body: such a response carries only an error
/// code (and optionally a reason), whatever the request was. <see cref="DecodeResponse"/> does. Responses on a
/// connection may come back in another order than the requests; match them by correlation id.
/// </para>
/// </summary>
public static class Frames
{
    private static readonly UTF8Encoding StrictUtf8 = new(false, true);

    /// <summary>The largest frame a node sends or accepts: 16 MiB.</summary>
    public const int MaxFrame = 16 * 1024 * 1024;

    /// <summary>A decoded request frame.</summary>
    public sealed record DecodedRequest(RequestHeader Header, Request Request);

    /// <summary>A decoded response frame.</summary>
    public sealed record DecodedResponse(uint CorrelationId, Response Response);

    /// <summary>
    /// A delivery to a queue consumer.
    /// </summary>
    /// <param name="SubscriptionId">The subscription.</param>
    /// <param name="Error"><c>NONE</c> for a delivery; otherwise why the subscription ended (it then carries no batch).</param>
    /// <param name="Batch">One record batch from the log, unchanged.</param>
    /// <param name="Deliveries">The offsets in <paramref name="Batch"/> delivered to this subscription.</param>
    /// <param name="LeaderHint">The queue's leader, after <c>NOT_LEADER</c>, or <c>null</c>.</param>
    public sealed record DeliverPush(
        uint SubscriptionId,
        ErrorCode Error,
        ByteString Batch,
        IReadOnlyList<Messages.DeliveredOffset> Deliveries,
        LeaderHint? LeaderHint)
    {
        /// <summary>
        /// Whether this push says the subscription became its queue's single active consumer (ADR 0076): no error, no
        /// batch, no deliveries. Only a subscription that asked for it gets one.
        /// </summary>
        public bool IsActivation => Error.Code == 0 && Batch.Size == 0 && Deliveries.Count == 0;
    }

    // ---- length prefix ----

    /// <summary>The frame preceded by its length as a big-endian <c>u32</c>, ready to write to the connection.</summary>
    public static byte[] WithLengthPrefix(byte[] frame)
    {
        if (frame.Length > MaxFrame)
        {
            throw new ArgumentException($"frame of {frame.Length} bytes exceeds the limit", nameof(frame));
        }
        byte[] output = new byte[4 + frame.Length];
        output[0] = (byte)(frame.Length >> 24);
        output[1] = (byte)(frame.Length >> 16);
        output[2] = (byte)(frame.Length >> 8);
        output[3] = (byte)frame.Length;
        Buffer.BlockCopy(frame, 0, output, 4, frame.Length);
        return output;
    }

    /// <summary>Reads the length prefix of <c>prefix[0, 4)</c>, a big-endian <c>u32</c>.</summary>
    /// <exception cref="DecodeException">It exceeds <see cref="MaxFrame"/>; the connection cannot go on.</exception>
    public static int ReadLength(byte[] prefix)
    {
        long len = ((long)prefix[0] << 24) | ((long)prefix[1] << 16) | ((long)prefix[2] << 8) | prefix[3];
        if (len > MaxFrame)
        {
            throw new DecodeException(DecodeErrorKind.Malformed, $"frame of {len} bytes exceeds the limit");
        }
        return (int)len;
    }

    // ---- requests ----

    /// <summary>Encodes a version-0 request. <paramref name="correlationId"/> must not be 0, which is reserved for pushes.</summary>
    public static byte[] EncodeRequest(uint correlationId, Request request)
    {
        var w = new Writer();
        w.U16(request.ApiKey.Id);
        w.U16(0);
        w.U32(correlationId);
        w.U16(0);
        w.NoTags();
        request.Write(w);
        return w.ToByteArray();
    }

    /// <summary>
    /// Decodes a request. A header this library does not speak gives a <see cref="DecodeException"/> of kind
    /// <see cref="DecodeErrorKind.Unsupported"/> that carries the header, so the caller can still answer.
    /// </summary>
    public static DecodedRequest DecodeRequest(byte[] frame)
    {
        var r = new Reader(frame);
        var header = new RequestHeader(new ApiKey(r.U16()), r.U16(), r.U32(), r.U16());
        r.Tags();
        if (header.ApiVersion != 0 || !ApiKey.IsClientKey(header.ApiKey))
        {
            throw new DecodeException(
                DecodeErrorKind.Unsupported,
                $"unsupported API key {header.ApiKey.Id} version {header.ApiVersion}",
                header);
        }
        Request request = header.ApiKey.Id switch
        {
            0 => Request.ApiVersions.Read(r),
            1 => Request.Metadata.Read(r),
            2 => Request.FindCoordinator.Read(r),
            3 => Request.Produce.Read(r),
            4 => Request.Fetch.Read(r),
            5 => Request.ListOffsets.Read(r),
            6 => Request.GroupHeartbeat.Read(r),
            7 => Request.CommitOffsets.Read(r),
            8 => Request.OffsetFetch.Read(r),
            9 => Request.Publish.Read(r),
            10 => Request.Subscribe.Read(r),
            12 => Request.Credit.Read(r),
            13 => Request.Ack.Read(r),
            14 => Request.Nack.Read(r),
            15 => Request.Unsubscribe.Read(r),
            16 => Request.InitProducer.Read(r),
            17 => Request.AddPartitionsToTxn.Read(r),
            19 => Request.AddOffsetsToTxn.Read(r),
            20 => Request.EndTxn.Read(r),
            21 => Request.CreateStream.Read(r),
            22 => Request.CreateQueue.Read(r),
            23 => Request.DeclareExchange.Read(r),
            24 => Request.Bind.Read(r),
            25 => Request.DeleteResource.Read(r),
            26 => Request.DescribeConfigs.Read(r),
            27 => Request.AlterConfigs.Read(r),
            28 => Request.Unbind.Read(r),
            29 => Request.DescribeMetrics.Read(r),
            30 => Request.SaslHandshake.Read(r),
            31 => Request.SaslAuthenticate.Read(r),
            32 => Request.AlterUser.Read(r),
            33 => Request.DescribeUsers.Read(r),
            34 => Request.CreateTenant.Read(r),
            35 => Request.DeleteTenant.Read(r),
            36 => Request.DescribeTenants.Read(r),
            37 => Request.AlterRoleBindings.Read(r),
            38 => Request.DescribeRoleBindings.Read(r),
            39 => Request.WhoAmI.Read(r),
            40 => Request.AddNode.Read(r),
            41 => Request.RemoveNode.Read(r),
            42 => Request.DescribeNodes.Read(r),
            47 => Request.QueueAction.Read(r, false),
            48 => Request.QueueAction.Read(r, true),
            49 => Request.MoveLeader.Read(r),
            50 => Request.RevokeSessions.Read(r),
            51 => Request.CreatePartitions.Read(r),
            52 => Request.RevokeCertificate.Read(r),
            53 => Request.DescribeCertificates.Read(r),
            54 => Request.RecordCertificates.Read(r),
            55 => Request.RequestCertificate.Read(r),
            56 => Request.DescribeCertificateRequests.Read(r),
            57 => Request.RecordCertificate.Read(r),
            _ => throw new InvalidOperationException("checked against the implemented keys"),
        };
        r.Finish();
        return new DecodedRequest(header, request);
    }

    // ---- responses ----

    /// <summary>Encodes a response to the request with <paramref name="correlationId"/>.</summary>
    public static byte[] EncodeResponse(uint correlationId, Response response)
    {
        var w = new Writer();
        w.U32(correlationId);
        w.U16(response is Response.Failed or Response.Refused ? Flags.Error : (ushort)0);
        w.NoTags();
        response.Write(w);
        return w.ToByteArray();
    }

    /// <summary>The correlation id of a response frame, to find the request it answers.</summary>
    public static uint ResponseCorrelationId(byte[] frame)
    {
        if (frame.Length < 4)
        {
            throw DecodeException.TruncatedFrame();
        }
        return new Reader(frame, 0, 4).U32();
    }

    /// <summary>
    /// Decodes a response to a request of type <paramref name="apiKey"/>. A frame with the error flag becomes a
    /// <see cref="Response.Failed"/> or, when it carries a reason, a <see cref="Response.Refused"/>, whatever the key is.
    /// </summary>
    public static DecodedResponse DecodeResponse(byte[] frame, ApiKey apiKey)
    {
        var r = new Reader(frame);
        uint correlationId = r.U32();
        ushort flags = r.U16();
        r.Tags();
        if ((flags & Flags.Error) != 0)
        {
            var code = new ErrorCode(r.U16());
            ByteString? reason = null;
            foreach (Tag t in r.Tags())
            {
                if (t.Number == Messages.TagReason)
                {
                    reason = t.Value;
                    break;
                }
            }
            r.Finish();
            if (reason is null)
            {
                return new DecodedResponse(correlationId, new Response.Failed(code));
            }
            return new DecodedResponse(correlationId, new Response.Refused(code, StrictUtf8String(reason)));
        }
        Response response = apiKey.Id switch
        {
            0 => Response.ApiVersions.Read(r),
            1 => Response.Metadata.Read(r),
            2 => Response.FindCoordinator.Read(r),
            16 => Response.InitProducer.Read(r),
            17 => Response.AddPartitionsToTxn.Read(r),
            20 => Response.EndTxn.Read(r),
            10 => Response.Subscribe.Read(r),
            13 or 14 or 15 or 24 or 28 or 44 => Response.Outcome.Read(r),
            21 or 22 or 23 => Response.Create.Read(r),
            25 or 27 or 32 or 34 or 35 or 37 or 40 or 41 or 43 or 49 or 50 or 51 or 52 or 54 or 55 or 57 => Response.Admin.Read(r),
            42 => Response.DescribeNodes.Read(r),
            26 => Response.DescribeConfigs.Read(r),
            29 => Response.DescribeMetrics.Read(r),
            30 => Response.SaslHandshake.Read(r),
            31 => Response.SaslAuthenticate.Read(r),
            33 => Response.DescribeUsers.Read(r),
            53 => Response.DescribeCertificates.Read(r),
            56 => Response.DescribeCertificateRequests.Read(r),
            36 => Response.DescribeTenants.Read(r),
            38 => Response.DescribeRoleBindings.Read(r),
            39 => Response.WhoAmI.Read(r),
            6 => Response.GroupHeartbeat.Read(r),
            7 or 19 => Response.CommitOffsets.Read(r),
            8 => Response.OffsetFetch.Read(r),
            3 => Response.Produce.Read(r),
            4 => Response.Fetch.Read(r),
            5 => Response.ListOffsets.Read(r),
            47 or 48 => Response.QueueAction.Read(r),
            9 => Response.Publish.Read(r),
            _ => throw DecodeException.Malformed("unknown API key"),
        };
        r.Finish();
        return new DecodedResponse(correlationId, response);
    }

    private static string StrictUtf8String(ByteString s)
    {
        try
        {
            return StrictUtf8.GetString(s.Span);
        }
        catch (DecoderFallbackException)
        {
            throw DecodeException.Malformed("reason is not UTF-8");
        }
    }

    // ---- pushes ----

    /// <summary>
    /// Encodes a push frame: correlation id 0, flag <see cref="Flags.Push"/>, an empty block of tagged fields, then
    /// the API key (<see cref="ApiKey.Deliver"/>) and version 0 and the body. The subscription id is the first field
    /// of the body.
    /// </summary>
    public static byte[] EncodePush(DeliverPush push)
    {
        var w = new Writer();
        w.U32(0);
        w.U16(Flags.Push);
        w.NoTags();
        w.U16(ApiKey.Deliver.Id);
        w.U16(0);
        w.U32(push.SubscriptionId);
        w.U16(push.Error.Code);
        w.Bytes(push.Batch);
        w.Array(push.Deliveries, (w1, d) => w1.U64(d.Offset).U16(d.DeliveryCount));
        Messages.PutHint(w, push.LeaderHint);
        return w.ToByteArray();
    }

    /// <summary>Whether a frame from the broker is a push rather than a response: correlation id 0 and the push flag.</summary>
    public static bool IsPush(byte[] frame) =>
        frame.Length >= 6
        && frame[0] == 0
        && frame[1] == 0
        && frame[2] == 0
        && frame[3] == 0
        && ((frame[4] | frame[5] << 8) & Flags.Push) != 0;

    /// <summary>Decodes a push frame.</summary>
    public static DeliverPush DecodePush(byte[] frame)
    {
        var r = new Reader(frame);
        uint correlationId = r.U32();
        ushort flags = r.U16();
        if (correlationId != 0 || (flags & Flags.Push) == 0)
        {
            throw DecodeException.Malformed("not a push frame");
        }
        r.Tags();
        if (r.U16() != ApiKey.Deliver.Id || r.U16() != 0)
        {
            throw DecodeException.Malformed("unknown push");
        }
        uint subscriptionId = r.U32();
        var error = new ErrorCode(r.U16());
        ByteString batch = r.Bytes();
        List<Messages.DeliveredOffset> deliveries = r.Array(10, r1 => new Messages.DeliveredOffset(r1.U64(), r1.U16()));
        LeaderHint? hint = Messages.GetHint(r);
        r.Finish();
        return new DeliverPush(subscriptionId, error, batch, deliveries, hint);
    }
}
