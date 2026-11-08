using System;
using System.Collections.Generic;
using Lotse.Proto;
using Lotse.Record;

namespace Lotse.Client;

/// <summary>
/// Records read from one partition.
/// <para>
/// <see cref="Records"/> holds whole batches exactly as the broker sent them, control batches and aborted ones
/// included. A reader that wants read_committed semantics uses <see cref="CommittedBatches"/> or
/// <see cref="CommittedMessages"/>, which leave out what it must not see.
/// </para>
/// </summary>
public sealed class Fetched
{
    private readonly IReadOnlyList<Messages.AbortedTxn> _aborted;

    /// <summary>Creates a result.</summary>
    /// <param name="records">Whole record batches.</param>
    /// <param name="highWatermark">Offsets below this are committed.</param>
    /// <param name="lastStableOffset">Offsets below this are committed and in no open transaction.</param>
    /// <param name="logStartOffset">The first offset still in the partition.</param>
    /// <param name="aborted">Aborted transactions with data in <paramref name="records"/>.</param>
    public Fetched(
        ByteString records,
        ulong highWatermark,
        ulong lastStableOffset,
        ulong logStartOffset,
        IReadOnlyList<Messages.AbortedTxn> aborted)
    {
        Records = records;
        HighWatermark = highWatermark;
        LastStableOffset = lastStableOffset;
        LogStartOffset = logStartOffset;
        _aborted = aborted;
    }

    /// <summary>Whole record batches, control batches and aborted ones included; may be empty.</summary>
    public ByteString Records { get; }

    /// <summary>Offsets below this are committed.</summary>
    public ulong HighWatermark { get; }

    /// <summary>Offsets below this are committed and in no open transaction; reads stop here.</summary>
    public ulong LastStableOffset { get; }

    /// <summary>The first offset still in the partition.</summary>
    public ulong LogStartOffset { get; }

    /// <summary>Aborted transactions with data in <see cref="Records"/>: producer id and first offset.</summary>
    public IReadOnlyList<Messages.AbortedTxn> Aborted => _aborted;

    /// <summary>
    /// The offset to fetch next: after the last batch, or <paramref name="from"/> if there is none. Reading stops at
    /// the first batch that does not parse.
    /// </summary>
    public ulong NextOffset(ulong from)
    {
        ulong next = from;
        ByteString rest = Records;
        while (!rest.IsEmpty)
        {
            Batch.Split split;
            try
            {
                split = Batch.Parse(rest);
            }
            catch (RecordFormatException)
            {
                break;
            }
            next = split.Batch.NextOffset;
            rest = split.Rest;
        }
        return next;
    }

    /// <summary>
    /// The data batches a read_committed consumer sees: control batches and batches of aborted transactions left out.
    /// An aborted transaction's batches are those of its producer from its first offset up to the producer's ABORT
    /// marker.
    /// </summary>
    /// <exception cref="BrokerErrorException">With <c>CORRUPT_BATCH</c> if the records do not parse.</exception>
    public List<Batch> CommittedBatches()
    {
        var pending = new List<Messages.AbortedTxn>(_aborted);
        pending.Sort((x, y) => x.FirstOffset.CompareTo(y.FirstOffset));
        int next = 0;
        var skipping = new HashSet<ulong>();
        var output = new List<Batch>();
        ByteString rest = Records;
        while (!rest.IsEmpty)
        {
            Batch.Split split;
            try
            {
                split = Batch.Parse(rest);
            }
            catch (RecordFormatException e)
            {
                throw new BrokerErrorException(ErrorCode.CorruptBatch, e.Message);
            }
            Batch b = split.Batch;
            rest = split.Rest;
            while (next < pending.Count && pending[next].FirstOffset <= b.BaseOffset)
            {
                skipping.Add(pending[next].ProducerId);
                next++;
            }
            if (b.Attributes.Control)
            {
                if (IsAbortMarker(b))
                {
                    skipping.Remove(b.ProducerId);
                }
                continue;
            }
            if (b.Attributes.Transactional && skipping.Contains(b.ProducerId))
            {
                continue;
            }
            output.Add(b);
        }
        return output;
    }

    /// <summary>The records of <see cref="CommittedBatches"/>, in offset order.</summary>
    /// <exception cref="RecordFormatException">
    /// Of kind <see cref="RecordFormatErrorKind.UnsupportedCompression"/> if a batch is compressed with a codec that is
    /// not available (ADR 0084).
    /// </exception>
    public List<BatchRecord> CommittedMessages()
    {
        var output = new List<BatchRecord>();
        foreach (Batch b in CommittedBatches())
        {
            try
            {
                output.AddRange(b.Records());
            }
            catch (RecordFormatException e)
            {
                // A codec library missing here is no damage: say which one to add (ADR 0084).
                if (e.Kind == RecordFormatErrorKind.UnsupportedCompression)
                {
                    throw;
                }
                throw new BrokerErrorException(ErrorCode.CorruptBatch, e.Message);
            }
        }
        return output;
    }

    private static bool IsAbortMarker(Batch b)
    {
        try
        {
            return ControlBatches.ReadMarker(b).Type == ControlType.Abort;
        }
        catch (RecordFormatException)
        {
            return false; // not a transaction marker: Raft's or a group's own entry
        }
    }
}
