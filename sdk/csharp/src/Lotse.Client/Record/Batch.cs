using System;
using System.Collections.Generic;

namespace Lotse.Record;

/// <summary>
/// One record batch as the broker stores it: a 65-byte header and a block of records, checked on parsing. The
/// fixed-width fields are little-endian; <c>base_offset</c> and <c>term</c> sit before the checksummed range, so the
/// leader stamps them without invalidating the checksum.
/// </summary>
public sealed class Batch : IEquatable<Batch>
{
    /// <summary>The header: 65 bytes.</summary>
    public const int HeaderLen = 65;

    /// <summary>The bytes before the batch's contents: <c>base_offset</c> and <c>batch_len</c>.</summary>
    public const int LenPrefix = 12;

    /// <summary>The only batch format version.</summary>
    public const int Magic = 1;

    /// <summary>The largest batch, header included: 1 MiB.</summary>
    public const int MaxBatchLen = 1024 * 1024;

    /// <summary>The most a record block may hold once decompressed: 16 MiB.</summary>
    public const int MaxRecordsLen = 16 * 1024 * 1024;

    internal const int CrcStart = 25;

    private readonly ByteString _bytes;
    private readonly Attributes _attributes;

    private Batch(ByteString bytes, Attributes attributes)
    {
        _bytes = bytes;
        _attributes = attributes;
    }

    /// <summary>A parsed batch and what follows it in the buffer.</summary>
    public sealed record Split(Batch Batch, ByteString Rest);

    /// <summary>
    /// Parses the batch at the start of <paramref name="buf"/>, which may hold more after it. The checks run in the
    /// order of the Rust parser: the length prefix, the size limits, the whole batch present, the magic, the
    /// checksum, the attributes, then the counts and timestamps.
    /// </summary>
    /// <exception cref="RecordFormatException">The bytes are not a valid batch.</exception>
    public static Split Parse(ByteString buf)
    {
        ArgumentNullException.ThrowIfNull(buf);
        long len = PeekLen(buf.Storage, buf.Offset, buf.Size);
        if (len < 0)
        {
            throw RecordFormatException.TruncatedBuffer();
        }
        if (len > MaxBatchLen)
        {
            throw new RecordFormatException(RecordFormatErrorKind.TooLarge, $"batch of {len} bytes exceeds the limit");
        }
        if (len < HeaderLen)
        {
            throw RecordFormatException.MalformedBatch("batch shorter than its header");
        }
        if (buf.Size < len)
        {
            throw RecordFormatException.TruncatedBuffer();
        }
        int n = (int)len;
        ByteString bytes = buf.Slice(0, n);
        byte[] a = bytes.Storage;
        int o = bytes.Offset;

        int magic = a[o + 20];
        if (magic != Magic)
        {
            throw new RecordFormatException(RecordFormatErrorKind.UnsupportedMagic, $"unsupported batch format version {magic}");
        }
        uint stored = GetU32(a, o + 21);
        uint computed = Crc(a, o + CrcStart, o + n);
        if (stored != computed)
        {
            throw new RecordFormatException(
                RecordFormatErrorKind.Checksum, $"checksum mismatch: stored 0x{stored:x8}, computed 0x{computed:x8}");
        }
        Attributes attributes = Attributes.FromBits(GetU16(a, o + 25));
        var batch = new Batch(bytes, attributes);
        uint count = batch.RecordCount;
        if (batch.IsCompacted)
        {
            // Compaction keeps a batch's offsets and drops records from it, down to none (ADR 0067).
            if (attributes.Control)
            {
                throw RecordFormatException.MalformedBatch("a control batch is never compacted");
            }
            if (count > batch.Span)
            {
                throw RecordFormatException.MalformedBatch("more records than offsets");
            }
        }
        else
        {
            if (count == 0)
            {
                throw RecordFormatException.MalformedBatch("empty batch");
            }
            if (batch.LastOffsetDelta != count - 1)
            {
                throw RecordFormatException.MalformedBatch("last_offset_delta does not match record_count");
            }
        }
        if (batch.MaxTimestamp < batch.BaseTimestamp)
        {
            throw RecordFormatException.MalformedBatch("max_ts before base_ts");
        }
        if (attributes.Control && count != 1)
        {
            throw RecordFormatException.MalformedBatch("control batch must hold one record");
        }
        return new Split(batch, buf.Slice(n, buf.Size));
    }

    /// <summary>
    /// Parses a buffer of whole batches in a row, such as the records of a fetch response. A cut-off last batch is
    /// a truncation, not a shorter list.
    /// </summary>
    public static List<Batch> ParseAll(ByteString buf)
    {
        var batches = new List<Batch>();
        ByteString rest = buf;
        while (!rest.IsEmpty)
        {
            Split s = Parse(rest);
            batches.Add(s.Batch);
            rest = s.Rest;
        }
        return batches;
    }

    /// <summary>The batch as it is on the wire.</summary>
    public ByteString AsBytes() => _bytes;

    /// <summary>The batch's length in bytes, header included.</summary>
    public int Length => _bytes.Size;

    /// <summary>Offset of the first record, as the leader stamped it (a <c>u64</c>).</summary>
    public ulong BaseOffset => GetU64(0);

    /// <summary>Offset of the last offset this batch spans. Wraps like the broker's arithmetic.</summary>
    public ulong LastOffset => unchecked(BaseOffset + LastOffsetDelta);

    /// <summary>The offset after this batch.</summary>
    public ulong NextOffset => unchecked(LastOffset + 1);

    /// <summary>The leader's term when it stamped the batch.</summary>
    public ulong Term => GetU64(12);

    /// <summary>The unpacked attributes.</summary>
    public Attributes Attributes => _attributes;

    /// <summary>Whether compaction rewrote the batch (attribute bit 8, ADR 0067): its offsets have gaps.</summary>
    public bool IsCompacted => (GetU16(_bytes.Storage, _bytes.Offset + 25) & (1 << 8)) != 0;

    /// <summary>How many offsets the batch spans: <c>last_offset_delta + 1</c>.</summary>
    public long Span => (long)LastOffsetDelta + 1;

    /// <summary><c>record_count - 1</c> in a batch compaction has not touched.</summary>
    public uint LastOffsetDelta => GetU32(_bytes.Storage, _bytes.Offset + 27);

    /// <summary>Timestamp of the first record, milliseconds since the epoch.</summary>
    public long BaseTimestamp => (long)GetU64(31);

    /// <summary>The newest timestamp in the batch.</summary>
    public long MaxTimestamp => (long)GetU64(39);

    /// <summary>The producer's id; 0 for a batch that is not idempotent.</summary>
    public ulong ProducerId => GetU64(47);

    /// <summary>The producer's epoch.</summary>
    public ushort ProducerEpoch => GetU16(_bytes.Storage, _bytes.Offset + 55);

    /// <summary>The sequence of the first record, or -1.</summary>
    public int BaseSequence => (int)GetU32(_bytes.Storage, _bytes.Offset + 57);

    /// <summary>How many records the header says the batch holds.</summary>
    public uint RecordCount => GetU32(_bytes.Storage, _bytes.Offset + 61);

    /// <summary>
    /// Decodes the records. A compressed block is decompressed first, each time this is called; its codec comes from
    /// the package <c>Lotse.Client.Codecs</c> or <see cref="CompressionCodecs.Register"/> (ADR 0084).
    /// </summary>
    /// <exception cref="RecordFormatException">
    /// The records are damaged, or the block is compressed with a codec that is not available
    /// (<see cref="RecordFormatErrorKind.UnsupportedCompression"/>, naming the package to add).
    /// </exception>
    public List<BatchRecord> Records()
    {
        bool compacted = IsCompacted;
        if (compacted && RecordCount == 0)
        {
            // Compaction writes an emptied batch's block plain, as nothing.
            return new List<BatchRecord>();
        }
        byte[] a;
        int pos;
        int end;
        if (_attributes.Compression == Compression.None)
        {
            a = _bytes.Storage;
            pos = _bytes.Offset + HeaderLen;
            end = _bytes.Offset + _bytes.Size;
        }
        else
        {
            a = BlockCodecs.Decompress(_attributes.Compression, _bytes.Storage, _bytes.Offset + HeaderLen, _bytes.Size - HeaderLen);
            pos = 0;
            end = a.Length;
        }

        ulong baseOffset = BaseOffset;
        long baseTs = BaseTimestamp;
        uint count = RecordCount;
        var output = new List<BatchRecord>((int)Math.Min(count, 1024u));
        long lastDelta = -1;
        for (long expectedDelta = 0; expectedDelta < count; expectedDelta++)
        {
            var len = Varint.GetI64(a, pos, end);
            pos += len.Length;
            if (len.Value < 0)
            {
                throw RecordFormatException.MalformedBatch("negative record length");
            }
            if (end - pos < len.Value)
            {
                throw RecordFormatException.TruncatedBuffer();
            }
            int recEnd = pos + (int)len.Value;
            var c = new Cursor(a, pos, recEnd);
            c.ByteValue(); // attributes, reserved
            long tsDelta = c.I64();
            int offsetDelta = c.I32();
            if (compacted)
            {
                // Gaps where records went, but in order and inside the batch.
                if (offsetDelta <= lastDelta || offsetDelta > LastOffsetDelta)
                {
                    throw RecordFormatException.MalformedBatch("record offset deltas out of order");
                }
                lastDelta = offsetDelta;
            }
            else if (offsetDelta != (int)expectedDelta)
            {
                throw RecordFormatException.MalformedBatch("record offset deltas are not consecutive");
            }
            ByteString? key = c.Nullable();
            ByteString? value = c.Nullable();
            long headerCount = c.I64();
            if (headerCount < 0)
            {
                throw RecordFormatException.MalformedBatch("negative header count");
            }
            var headers = new List<Header>((int)Math.Min(headerCount, 64));
            for (long i = 0; i < headerCount; i++)
            {
                ByteString? hk = c.Nullable();
                if (hk is null)
                {
                    throw RecordFormatException.MalformedBatch("null header key");
                }
                headers.Add(new Header(hk, c.Nullable()));
            }
            if (c.Pos != recEnd)
            {
                throw RecordFormatException.MalformedBatch("trailing bytes in record");
            }
            output.Add(new BatchRecord(unchecked(baseOffset + (ulong)offsetDelta), unchecked(baseTs + tsDelta), key, value, headers));
            pos = recEnd;
        }
        if (pos != end)
        {
            throw RecordFormatException.MalformedBatch("trailing bytes after records");
        }
        return output;
    }

    /// <inheritdoc/>
    public bool Equals(Batch? other) => other is not null && _bytes.Equals(other._bytes);

    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is Batch other && Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode() => _bytes.GetHashCode();

    /// <inheritdoc/>
    public override string ToString() => $"Batch[baseOffset={BaseOffset}, records={RecordCount}, {_attributes}]";

    // ---- editing encoded batches in place ----

    /// <summary>
    /// The length of the batch that starts <paramref name="buf"/> (<c>batch_len + 12</c>), or -1 if there are fewer
    /// than 12 bytes. The length is a <c>u32</c>, so the result can exceed what a batch may be.
    /// </summary>
    public static long PeekLen(byte[] buf) => PeekLen(buf, 0, buf.Length);

    private static long PeekLen(byte[] buf, int off, int available)
    {
        if (available < LenPrefix)
        {
            return -1;
        }
        return LenPrefix + (long)GetU32(buf, off + 8);
    }

    /// <summary>
    /// Sets <c>base_offset</c> and <c>term</c> in an encoded batch. They lie before the checksummed range, so the
    /// checksum stays valid. This is the leader's job; a client sends both as zero.
    /// </summary>
    public static void Stamp(byte[] batch, ulong baseOffset, ulong term)
    {
        if (batch.Length < HeaderLen)
        {
            throw RecordFormatException.TruncatedBuffer();
        }
        PutU64(batch, 0, baseOffset);
        PutU64(batch, 12, term);
    }

    /// <summary>Moves the batch's timestamps so <c>base_ts</c> is <paramref name="baseTs"/>, keeping their spread, and recomputes the checksum.</summary>
    public static void SetBaseTimestamp(byte[] batch, long baseTs)
    {
        int len = WholeLength(batch);
        long oldBase = (long)GetU64(batch, 31);
        long oldMax = (long)GetU64(batch, 39);
        PutU64(batch, 31, (ulong)baseTs);
        PutU64(batch, 39, unchecked((ulong)(baseTs + (oldMax - oldBase))));
        Resum(batch, len);
    }

    /// <summary>Sets the producer id, epoch and base sequence of an encoded batch and recomputes the checksum.</summary>
    public static void SetProducer(byte[] batch, ulong producerId, ushort producerEpoch, int baseSequence)
    {
        int len = WholeLength(batch);
        PutU64(batch, 47, producerId);
        PutU16(batch, 55, producerEpoch);
        PutU32(batch, 57, unchecked((uint)baseSequence));
        Resum(batch, len);
    }

    /// <summary>Sets or clears the transactional bit of an encoded batch and recomputes the checksum.</summary>
    public static void SetTransactional(byte[] batch, bool transactional)
    {
        int len = WholeLength(batch);
        int bits = GetU16(batch, 25);
        bits = transactional ? bits | 1 << 3 : bits & ~(1 << 3);
        PutU16(batch, 25, (ushort)bits);
        Resum(batch, len);
    }

    private static int WholeLength(byte[] batch)
    {
        long len = PeekLen(batch);
        if (len < 0 || len < HeaderLen || batch.Length < len)
        {
            throw RecordFormatException.TruncatedBuffer();
        }
        return (int)len;
    }

    private static void Resum(byte[] batch, int len) => PutU32(batch, 21, Crc(batch, CrcStart, len));

    // ---- fixed-width access, little-endian ----

    internal static uint Crc(byte[] a, int from, int to) => Crc32c.Compute(a, from, to);

    private ulong GetU64(int at) => GetU64(_bytes.Storage, _bytes.Offset + at);

    internal static ushort GetU16(byte[] b, int at) => (ushort)(b[at] | b[at + 1] << 8);

    internal static uint GetU32(byte[] b, int at) =>
        (uint)(b[at] | b[at + 1] << 8 | b[at + 2] << 16 | b[at + 3] << 24);

    internal static ulong GetU64(byte[] b, int at) => GetU32(b, at) | (ulong)GetU32(b, at + 4) << 32;

    internal static void PutU16(byte[] b, int at, ushort v)
    {
        b[at] = (byte)v;
        b[at + 1] = (byte)(v >> 8);
    }

    internal static void PutU32(byte[] b, int at, uint v)
    {
        b[at] = (byte)v;
        b[at + 1] = (byte)(v >> 8);
        b[at + 2] = (byte)(v >> 16);
        b[at + 3] = (byte)(v >> 24);
    }

    internal static void PutU64(byte[] b, int at, ulong v)
    {
        PutU32(b, at, (uint)v);
        PutU32(b, at + 4, (uint)(v >> 32));
    }

    private sealed class Cursor
    {
        private readonly byte[] _a;
        private readonly int _end;

        internal Cursor(byte[] a, int pos, int end)
        {
            _a = a;
            Pos = pos;
            _end = end;
        }

        internal int Pos { get; private set; }

        internal int ByteValue()
        {
            if (Pos >= _end)
            {
                throw RecordFormatException.TruncatedBuffer();
            }
            return _a[Pos++];
        }

        internal long I64()
        {
            var d = Varint.GetI64(_a, Pos, _end);
            Pos += d.Length;
            return d.Value;
        }

        internal int I32()
        {
            var d = Varint.GetI32(_a, Pos, _end);
            Pos += d.Length;
            return d.Value;
        }

        // A length of -1 is null; any other negative length is damage.
        internal ByteString? Nullable()
        {
            long len = I64();
            if (len == -1)
            {
                return null;
            }
            if (len < 0)
            {
                throw RecordFormatException.MalformedBatch("negative length");
            }
            if (_end - Pos < len)
            {
                throw RecordFormatException.TruncatedBuffer();
            }
            ByteString output = ByteString.View(_a, Pos, (int)len);
            Pos += (int)len;
            return output;
        }
    }
}
