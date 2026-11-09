using System;
using System.Collections.Generic;
using System.Linq;
using Lotse.Proto;
using Lotse.Record;

namespace Lotse.Client;

/// <summary>
/// An exchange and its bindings, as metadata gives them, and which queues a message goes to. The rules are those of
/// <c>lotse-route</c>, which the broker and the Rust client use, checked against <c>routing.txt</c>:
/// <list type="bullet">
/// <item><b>direct</b> (0): the binding key equals the routing key;</item>
/// <item><b>topic</b> (1): keys are words split at <c>.</c>; in a binding key <c>*</c> matches exactly one word and
/// <c>#</c> zero or more; an empty key has no words;</item>
/// <item><b>fanout</b> (2): every binding;</item>
/// <item><b>headers</b> (3): the binding's headers must be present with equal values (a null value equals only null),
/// all of them or any one; a binding without headers matches everything when it asks for all and nothing when it
/// asks for any.</item>
/// </list>
/// A queue bound twice gets a message once. An exchange of a type this library does not know routes nowhere.
/// </summary>
internal sealed class Exchange
{
    internal Exchange(string name, ulong resourceId, byte type, IReadOnlyList<Messages.BindingMetadata> bindings)
    {
        Name = name;
        ResourceId = resourceId;
        Type = type;
        Bindings = bindings.ToArray();
    }

    internal string Name { get; }

    internal ulong ResourceId { get; }

    internal byte Type { get; }

    internal IReadOnlyList<Messages.BindingMetadata> Bindings { get; }

    internal static Exchange Of(string name, ulong resourceId, Messages.ExchangeMetadata x) =>
        new(name, resourceId, x.ExchangeType, x.Bindings);

    /// <summary>The ids of the queues a message with <paramref name="routingKey"/> and <paramref name="headers"/> goes to, ascending.</summary>
    internal SortedSet<ulong> Route(string routingKey, IReadOnlyList<Header> headers)
    {
        var output = new SortedSet<ulong>();
        foreach (Messages.BindingMetadata b in Bindings)
        {
            bool hit = Type switch
            {
                Messages.ExchangeType.Direct => b.BindingKey == routingKey,
                Messages.ExchangeType.Topic => TopicMatches(b.BindingKey, routingKey),
                Messages.ExchangeType.Fanout => true,
                Messages.ExchangeType.Headers => HeadersMatch(b, headers),
                _ => false,
            };
            if (hit)
            {
                output.Add(b.QueueId);
            }
        }
        return output;
    }

    /// <summary>Whether a topic binding key matches a routing key.</summary>
    internal static bool TopicMatches(string pattern, string key) => Matches(Words(pattern), 0, Words(key), 0);

    /// <summary>An empty key has no words, not one empty word.</summary>
    private static string[] Words(string s) => s.Length == 0 ? Array.Empty<string>() : s.Split('.');

    private static bool Matches(string[] pattern, int p, string[] key, int k)
    {
        if (p == pattern.Length)
        {
            return k == key.Length;
        }
        string w = pattern[p];
        if (w == "#")
        {
            // # takes zero words, or one and stays.
            return Matches(pattern, p + 1, key, k) || (k < key.Length && Matches(pattern, p, key, k + 1));
        }
        if (k == key.Length)
        {
            return false;
        }
        return (w == "*" || w == key[k]) && Matches(pattern, p + 1, key, k + 1);
    }

    private static bool HeadersMatch(Messages.BindingMetadata b, IReadOnlyList<Header> present)
    {
        if (b.HeaderMatch.Count == 0)
        {
            return b.MatchAll;
        }
        IEnumerable<bool> hits = b.HeaderMatch.Select(m => present.Any(h => h.Key.Equals(m.Key) && Equals(h.Value, m.Value)));
        return b.MatchAll ? hits.All(x => x) : hits.Any(x => x);
    }
}
