using System;
using System.Collections.Generic;

namespace Lotse.Record;

/// <summary>
/// Builds one batch. <c>base_ts</c> is the first record's timestamp and <c>max_ts</c> the largest; the builder reads
/// no clock. <c>base_offset</c> and <c>term</c> stay zero: the leader stamps them. Not thread-safe.
/// </summary>
public sealed class BatchBuilder
{
    private static readonly IReadOnlyList<Header> NoHeaders = Array.Empty<Header>();

    private readonly BatchOptions _options;
    private bool _hasBase;
    private long _baseTs;
    private long _maxTs;
    private long _count;
    private readonly GrowableBytes _body = new(256);

    /// <summary>Starts a batch with the given header fields.</summary>
    public BatchBuilder(BatchOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <summary>Starts a plain batch: no producer, no sequence.</summary>
    public BatchBuilder()
        : this(BatchOptions.Default)
    {
    }

    /// <summary>How many records have been added.</summary>
    public long Size => _count;

    /// <summary>Whether no record has been added.</summary>
    public bool IsEmpty => _count == 0;

    /// <summary>The length the batch would have now, uncompressed.</summary>
    public int EncodedLength => Batch.HeaderLen + _body.Size;

    /// <summary>Adds a record; <paramref name="key"/> and <paramref name="value"/> may be null.</summary>
    public BatchBuilder Push(long timestamp, byte[]? key, byte[]? value, IReadOnlyList<Header>? headers = null) =>
        Push(
            timestamp,
            key is null ? null : ByteString.View(key, 0, key.Length),
            value is null ? null : ByteString.View(value, 0, value.Length),
            headers);

    /// <summary>Adds a record; <paramref name="key"/> and <paramref name="value"/> may be null.</summary>
    public BatchBuilder Push(long timestamp, ByteString? key, ByteString? value, IReadOnlyList<Header>? headers = null)
    {
        headers ??= NoHeaders;
        if (!_hasBase)
        {
            _hasBase = true;
            _baseTs = timestamp;
        }
        _maxTs = _count == 0 ? timestamp : Math.Max(_maxTs, timestamp);

        var rec = new GrowableBytes(16 + (key?.Size ?? 0) + (value?.Size ?? 0));
        rec.Put(0); // attributes
        rec.PutVarint(unchecked(timestamp - _baseTs)); // wraps like the broker's
        rec.PutVarint(_count);
        PutNullable(rec, key);
        PutNullable(rec, value);
        rec.PutVarint(headers.Count);
        foreach (Header h in headers)
        {
            PutNullable(rec, h.Key);
            PutNullable(rec, h.Value);
        }
        _body.PutVarint(rec.Size);
        _body.Put(rec.Storage, 0, rec.Size);
        _count++;
        return this;
    }

    private static void PutNullable(GrowableBytes buf, ByteString? v)
    {
        if (v is null)
        {
            buf.PutVarint(-1);
        }
        else
        {
            buf.PutVarint(v.Size);
            buf.Put(v);
        }
    }

    /// <summary>The encoded batch, checksum included.</summary>
    /// <exception cref="RecordFormatException">
    /// There is no record, the batch exceeds 1 MiB, or the options ask for a codec this SDK lacks.
    /// </exception>
    public ByteString Finish()
    {
        if (_count == 0)
        {
            throw RecordFormatException.MalformedBatch("empty batch");
        }
        Compression codec = _options.Attributes.Compression;
        if (codec != Compression.None)
        {
            throw Codecs.Unsupported(codec, "write");
        }
        long total = (long)Batch.HeaderLen + _body.Size;
        if (total > Batch.MaxBatchLen)
        {
            throw new RecordFormatException(RecordFormatErrorKind.TooLarge, $"batch of {total} bytes exceeds the limit");
        }
        byte[] buf = new byte[(int)total];
        Buffer.BlockCopy(_body.Storage, 0, buf, Batch.HeaderLen, _body.Size);
        Batch.PutU32(buf, 8, (uint)(total - Batch.LenPrefix));
        buf[20] = Batch.Magic;
        Batch.PutU16(buf, 25, (ushort)_options.Attributes.ToBits());
        Batch.PutU32(buf, 27, (uint)(_count - 1));
        Batch.PutU64(buf, 31, (ulong)(_hasBase ? _baseTs : 0));
        Batch.PutU64(buf, 39, (ulong)_maxTs);
        Batch.PutU64(buf, 47, _options.ProducerId);
        Batch.PutU16(buf, 55, _options.ProducerEpoch);
        Batch.PutU32(buf, 57, unchecked((uint)_options.BaseSequence));
        Batch.PutU32(buf, 61, (uint)_count);
        Batch.PutU32(buf, 21, Crc32c.Compute(buf, Batch.CrcStart, buf.Length));
        return ByteString.View(buf, 0, buf.Length);
    }
}
