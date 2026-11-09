using System;
using System.Collections.Generic;
using Lotse.Proto;

namespace Lotse.Client;

/// <summary>Offsets as the half-open ranges <c>[from, to)</c> that <c>Ack</c> and <c>Nack</c> carry.</summary>
internal static class OffsetRanges
{
    /// <summary>
    /// Sorts the offsets (as <c>u64</c>), drops duplicates and joins neighbours: <c>5, 6, 7, 9</c> is
    /// <c>[5, 8), [9, 10)</c>.
    /// </summary>
    internal static List<Messages.OffsetRange> Of(IReadOnlyCollection<ulong> offsets)
    {
        ulong[] sorted = new ulong[offsets.Count];
        int i = 0;
        foreach (ulong o in offsets)
        {
            sorted[i++] = o;
        }
        Array.Sort(sorted);
        var output = new List<Messages.OffsetRange>();
        ulong from = 0;
        ulong to = 0;
        bool open = false;
        foreach (ulong o in sorted)
        {
            if (open && o == unchecked(to - 1))
            {
                continue; // a duplicate
            }
            if (open && o == to)
            {
                to = unchecked(o + 1);
                continue;
            }
            if (open)
            {
                output.Add(new Messages.OffsetRange(from, to));
            }
            from = o;
            to = unchecked(o + 1);
            open = true;
        }
        if (open)
        {
            output.Add(new Messages.OffsetRange(from, to));
        }
        return output;
    }
}
