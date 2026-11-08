using System.Collections.Generic;
using System.Linq;
using Lotse.Record;
using Xunit;

namespace Lotse.Client.Tests.Client;

public sealed class DeliveryTests
{
    private static Header H(string k, string? v) => new(ByteString.Utf8(k), v is null ? null : ByteString.Utf8(v));

    private static Delivery With(params Header[] headers) =>
        new(new BatchRecord(5, 1_000, null, ByteString.Utf8("body"), headers), 3, new object());

    [Fact]
    public void AMessageWithoutDeadLetterHeadersIsNotDeadLettered()
    {
        Assert.Null(With(H("x-other", "1")).DeadLetter);
        Assert.Null(With().DeadLetter);
    }

    [Fact]
    public void TheDeadLetterHeadersAreReadAsTyped()
    {
        Delivery d = With(
            H("app", "yes"),
            H("x-lotse-dead-letter-reason", "max-deliveries"),
            H("x-lotse-dead-letter-queue", "jobs"),
            H("x-lotse-dead-letter-queue-id", "18446744073709551615"),
            H("x-lotse-dead-letter-offset", "42"),
            H("x-lotse-dead-letter-deliveries", "3"),
            H("x-lotse-dead-letter-floor", "40"));
        DeadLetter dl = d.DeadLetter!;
        Assert.Equal("max-deliveries", dl.Reason);
        Assert.Equal("jobs", dl.Queue);
        Assert.Equal(ulong.MaxValue, dl.QueueId); // a u64 above i64::MAX
        Assert.Equal(42UL, dl.Offset);
        Assert.Equal(3, dl.Deliveries);
        Assert.Equal(40UL, dl.Floor);
        Assert.Equal(5UL, d.Offset);
        Assert.Equal(3, d.DeliveryCount);
    }

    [Fact]
    public void AnUnreadableNumberReadsAsZero()
    {
        DeadLetter dl = With(H("x-lotse-dead-letter-reason", "expired"), H("x-lotse-dead-letter-offset", "nope"), H("x-lotse-dead-letter-floor", "-1")).DeadLetter!;
        Assert.Equal("expired", dl.Reason);
        Assert.Equal(0UL, dl.Offset);
        Assert.Equal(0UL, dl.Floor);
        Assert.Equal(string.Empty, dl.Queue);
    }

    [Fact]
    public void WhenAHeaderRepeatsTheLastWins()
    {
        DeadLetter dl = With(H("x-lotse-dead-letter-reason", "expired"), H("x-lotse-dead-letter-reason", "max-deliveries")).DeadLetter!;
        Assert.Equal("max-deliveries", dl.Reason);
    }

    [Fact]
    public void HeadersKeepTheirOrder()
    {
        List<string> keys = With(H("a", "1"), H("b", "2")).Headers.Select(h => h.Key.ToUtf8()).ToList();
        Assert.Equal(new[] { "a", "b" }, keys);
    }

    [Fact]
    public void OffsetsBecomeSortedJoinedRanges()
    {
        static string R(params ulong[] o) => string.Join(",", OffsetRanges.Of(o).Select(r => $"[{r.From},{r.To})"));
        Assert.Equal("[5,8),[9,10)", R(5, 6, 7, 9));
        Assert.Equal("[5,8),[9,10)", R(9, 7, 5, 6, 7, 5));
        Assert.Equal(string.Empty, R());
        Assert.Equal("[18446744073709551614,18446744073709551615)", R(ulong.MaxValue - 1));
        Assert.Equal("[1,2),[ 9223372036854775808,9223372036854775809)".Replace(" ", string.Empty), R(1UL << 63, 1));
    }
}
