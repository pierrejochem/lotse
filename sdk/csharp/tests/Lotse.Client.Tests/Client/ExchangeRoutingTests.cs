using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Lotse.Proto;
using Lotse.Record;
using Xunit;

namespace Lotse.Client.Tests.Client;

/// <summary>Exchange routing against <c>routing.txt</c>, which <c>lotse-route</c> writes.</summary>
public sealed class ExchangeRoutingTests
{
    // "-" for null, "=hex" for bytes.
    private static ByteString? Nullable(string s) => s == "-" ? null : ByteString.CopyOf(Vectors.Hex(s[1..]));

    private static string Text(string s) => Encoding.UTF8.GetString(Nullable(s)!.ToByteArray());

    private static List<Header> Headers(string s) =>
        s == "-"
            ? new List<Header>()
            : s.Split('+').Select(h =>
            {
                string[] kv = h.Split(':', 2);
                return new Header(ByteString.CopyOf(Vectors.Hex(kv[0])), Nullable(kv[1]));
            }).ToList();

    [Fact]
    public void RoutesAsTheBrokerDoes()
    {
        int n = 0;
        foreach (string[] v in Vectors.Lines("routing.txt"))
        {
            var bindings = new List<Messages.BindingMetadata>();
            if (v[2] != "-")
            {
                foreach (string b in v[2].Split(';'))
                {
                    string[] f = b.Split(',', 4);
                    List<Messages.HeaderMatch> matches = Headers(f[3]).Select(h => new Messages.HeaderMatch(h.Key, h.Value)).ToList();
                    bindings.Add(new Messages.BindingMetadata(ulong.Parse(f[0]), Text(f[1]), matches, f[2] == "all"));
                }
            }
            var x = new Exchange("x", 9, byte.Parse(v[1]), bindings);
            ulong[] want = v[5] == "-" ? Array.Empty<ulong>() : v[5].Split(',').Select(ulong.Parse).ToArray();
            Assert.True(want.SequenceEqual(x.Route(Text(v[3]), Headers(v[4]))), v[0]);
            n++;
        }
        Assert.Equal(41, n); // every vector read
    }

    [Fact]
    public void AnUnknownTypeRoutesNowhere()
    {
        var x = new Exchange("x", 9, 7, new[] { new Messages.BindingMetadata(1, "k", Array.Empty<Messages.HeaderMatch>(), true) });
        Assert.Empty(x.Route("k", Array.Empty<Header>()));
    }

    [Fact]
    public void AQueueBoundTwiceGetsAMessageOnce()
    {
        var x = new Exchange(
            "x",
            9,
            Messages.ExchangeType.Fanout,
            new[]
            {
                new Messages.BindingMetadata(1, "a", Array.Empty<Messages.HeaderMatch>(), true),
                new Messages.BindingMetadata(1, "b", Array.Empty<Messages.HeaderMatch>(), true),
                new Messages.BindingMetadata(2, "c", Array.Empty<Messages.HeaderMatch>(), true),
            });
        Assert.Equal(new ulong[] { 1, 2 }, x.Route("anything", Array.Empty<Header>()).ToArray());
    }

    [Fact]
    public void QueueIdsAreOrderedAsUnsignedNumbers()
    {
        var x = new Exchange(
            "x",
            9,
            Messages.ExchangeType.Fanout,
            new[]
            {
                new Messages.BindingMetadata(ulong.MaxValue, "a", Array.Empty<Messages.HeaderMatch>(), true),
                new Messages.BindingMetadata(3, "b", Array.Empty<Messages.HeaderMatch>(), true),
            });
        Assert.Equal(new[] { 3UL, ulong.MaxValue }, x.Route(string.Empty, Array.Empty<Header>()).ToArray());
    }
}
