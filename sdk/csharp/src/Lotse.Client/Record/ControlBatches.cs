using System.Collections.Generic;

namespace Lotse.Record;

/// <summary>
/// Control batches hold one record whose key is <c>version u16 (0) | type u16</c>. Types 0 and 1 are transaction
/// markers with the coordinator epoch as a <c>u32</c> value; the rest are the broker's own entries.
/// </summary>
public static class ControlBatches
{
    /// <summary>An encoded transaction marker, as the coordinator's leader writes it.</summary>
    public static ByteString Marker(
        ulong producerId, ushort producerEpoch, ControlType type, uint coordinatorEpoch, long timestamp)
    {
        var b = new BatchBuilder(
            new BatchOptions(new Attributes(Compression.None, true, true, BatchKind.Data), producerId, producerEpoch, -1));
        byte[] key = new byte[4];
        Batch.PutU16(key, 2, (ushort)type);
        byte[] value = new byte[4];
        Batch.PutU32(value, 0, coordinatorEpoch);
        b.Push(timestamp, key, value);
        return b.Finish();
    }

    /// <summary>
    /// The type of a control batch's record, or <c>null</c> if <paramref name="batch"/> is not a control batch.
    /// </summary>
    public static ControlKind? Kind(Batch batch)
    {
        if (!batch.Attributes.Control)
        {
            return null;
        }
        BatchRecord rec = FirstRecord(batch);
        ByteString? key = rec.Key;
        if (key is null)
        {
            throw RecordFormatException.MalformedBatch("control record without key");
        }
        if (key.Size != 4)
        {
            throw RecordFormatException.MalformedBatch("control record has the wrong size");
        }
        int code = key.At(2) | key.At(3) << 8;
        if (code > (int)ControlKind.ExpireProducers)
        {
            throw RecordFormatException.MalformedBatch("unknown control type");
        }
        var kind = (ControlKind)code;
        if (kind is ControlKind.AbortMarker or ControlKind.CommitMarker)
        {
            ByteString value = rec.Value ?? ByteString.Empty;
            if (value.Size != 4)
            {
                throw RecordFormatException.MalformedBatch("marker value has the wrong size");
            }
        }
        return kind;
    }

    /// <summary>Reads a transaction marker.</summary>
    /// <exception cref="RecordFormatException">The batch is not a transaction marker.</exception>
    public static ControlMarker ReadMarker(Batch batch)
    {
        if (!batch.Attributes.Control)
        {
            throw RecordFormatException.MalformedBatch("not a control batch");
        }
        BatchRecord rec = FirstRecord(batch);
        ByteString? key = rec.Key;
        ByteString? value = rec.Value;
        if (key is null)
        {
            throw RecordFormatException.MalformedBatch("control record without key");
        }
        if (value is null)
        {
            throw RecordFormatException.MalformedBatch("control record without value");
        }
        if (key.Size != 4 || value.Size != 4)
        {
            throw RecordFormatException.MalformedBatch("control record has the wrong size");
        }
        ControlType type = (key.At(2) | key.At(3) << 8) switch
        {
            0 => ControlType.Abort,
            1 => ControlType.Commit,
            _ => throw RecordFormatException.MalformedBatch("unknown control type"),
        };
        return new ControlMarker(type, Batch.GetU32(value.ToByteArray(), 0));
    }

    private static BatchRecord FirstRecord(Batch batch)
    {
        List<BatchRecord> records = batch.Records();
        if (records.Count == 0)
        {
            throw RecordFormatException.MalformedBatch("empty control batch");
        }
        return records[0];
    }
}
