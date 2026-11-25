using System.Collections.Generic;
using System.Linq;
using Lotse.Proto;

namespace Lotse.Client;

/// <summary>
/// Reads in this client's zone (the zone reads design): the indexes of the metadata and coordinator groups this client
/// has seen, which a node in the zone must have applied before it answers a read, and the choice of that node.
/// </summary>
internal static class Zone
{
    /// <summary>
    /// The highest index of each group named in an answer's <c>applied</c>, shared by a client and its connections. An
    /// index only rises: a follower further behind than an answer seen before must not make a later read accept older
    /// state. Thread-safe.
    /// </summary>
    internal sealed class Indexes
    {
        private readonly object _lock = new();

        // Both groups at 0: a read names them even before any answer did, so a node can tell a client that saw nothing
        // from one that sends no tag.
        private readonly SortedDictionary<ulong, ulong> _known = new()
        {
            [GroupIndex.Metadata] = 0,
            [GroupIndex.Coordinator] = 0,
        };

        /// <summary>Takes in what an answer says its node had applied.</summary>
        internal void Raise(IReadOnlyList<GroupIndex> applied)
        {
            lock (_lock)
            {
                foreach (GroupIndex e in applied)
                {
                    if (!_known.TryGetValue(e.Group, out ulong index) || index < e.Index)
                    {
                        _known[e.Group] = e.Index;
                    }
                }
            }
        }

        /// <summary>What a zone read asks for: every group's index, in group order.</summary>
        internal IReadOnlyList<GroupIndex> Snapshot()
        {
            lock (_lock)
            {
                return _known.Select(e => new GroupIndex(e.Key, e.Value)).ToList();
            }
        }
    }

    /// <summary>
    /// The lowest-id node of <paramref name="candidates"/> in <paramref name="zone"/>, as <paramref name="zones"/> has
    /// it, or <c>null</c>: one fixed choice, so a client's reads meet the same node's connection.
    /// </summary>
    internal static uint? ZoneNode(string zone, IEnumerable<uint> candidates, IReadOnlyDictionary<uint, string> zones)
    {
        uint? best = null;
        foreach (uint n in candidates)
        {
            if (zones.TryGetValue(n, out string? z) && z == zone && (best is null || n < best))
            {
                best = n;
            }
        }
        return best;
    }

    /// <summary>
    /// Whether a zone node's answer to a zone read is no answer: an error, of the whole read or of a part. The leader is
    /// asked instead, since a zone node refuses what it has not caught up on, and an older node refuses leader-only
    /// reads, with the same <c>NOT_LEADER</c> the leader never gives.
    /// </summary>
    internal static bool Refused(Response resp) => resp switch
    {
        Response.DescribeConfigs d => !d.Error.IsOk || d.Resources.Any(r => !r.Error.IsOk),
        Response.OffsetFetch o => o.Resources.SelectMany(r => r.Partitions).Any(p => !p.Error.IsOk),
        Response.DescribeNodes d => !d.Error.IsOk,
        Response.DescribeUsers d => !d.Error.IsOk,
        Response.DescribeTokens d => !d.Error.IsOk,
        Response.DescribeCertificates d => !d.Error.IsOk,
        Response.DescribeCertificateRequests d => !d.Error.IsOk,
        Response.DescribeUpgrade d => !d.Error.IsOk,
        Response.DescribeTenants d => !d.Error.IsOk,
        Response.DescribeRoleBindings d => !d.Error.IsOk,
        _ => true,
    };

    /// <summary>The metadata leader a refused admin read names, or <c>null</c>.</summary>
    internal static LeaderHint? LeaderHint(Response resp) => resp switch
    {
        Response.DescribeConfigs d => d.LeaderHint,
        Response.DescribeNodes d => d.LeaderHint,
        Response.DescribeUsers d => d.LeaderHint,
        Response.DescribeTokens d => d.LeaderHint,
        Response.DescribeCertificates d => d.LeaderHint,
        Response.DescribeCertificateRequests d => d.LeaderHint,
        Response.DescribeUpgrade d => d.LeaderHint,
        Response.DescribeTenants d => d.LeaderHint,
        Response.DescribeRoleBindings d => d.LeaderHint,
        _ => null,
    };
}
