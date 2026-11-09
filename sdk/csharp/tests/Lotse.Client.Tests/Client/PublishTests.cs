using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Lotse.Net;
using Lotse.Proto;
using Xunit;

namespace Lotse.Client.Tests.Client;

/// <summary>Publishing through an exchange that a node routes (ADR 0079), against a scripted broker.</summary>
public sealed class PublishTests : IDisposable
{
    private readonly FakeNode _a = new(1);
    private LotseClient? _client;

    public void Dispose()
    {
        _client?.Dispose();
        _a.Dispose();
    }

    private static Messages.ResourceMetadata Resource(string name, ulong id, byte kind) =>
        new(ErrorCode.None, name, "default", id, kind, Array.Empty<Messages.PartitionMetadata>(), null);

    /// <summary>The exchange <c>x</c> (20) with the queues <c>a</c> (7) and <c>b</c> (8) bound to it.</summary>
    private void Answer(Func<Request.Publish, Response?> publish)
    {
        _a.OnConn((n, conn, request) => request switch
        {
            Request.Metadata => Fx.Metadata(
                new[] { _a },
                Resource("x", 20, Messages.ResourceKind.Exchange),
                Resource("a", 7, Messages.ResourceKind.Queue),
                Resource("b", 8, Messages.ResourceKind.Queue)),
            Request.Publish p => publish(p),
            _ => throw new InvalidOperationException("unexpected " + request),
        });
        _client?.Dispose();
        _client = new LotseClient(Fx.Config(_a).WithRequestTimeout(TimeSpan.FromMilliseconds(500)).WithRetryFor(TimeSpan.FromSeconds(1)).Build());
    }

    private static Response Routed(params Messages.RoutedMessage[] queues) =>
        new Response.Publish(0, new[] { new Messages.PublishResult(ErrorCode.None, queues) });

    [Fact]
    public async Task ARoutedMessageNamesItsQueuesAndOffsets()
    {
        Request.Publish? sent = null;
        Answer(p =>
        {
            sent = p;
            // 9 was bound and deleted between this client's lookups: only the node can name it. An empty name falls
            // back to the name this client knows.
            return Routed(
                new Messages.RoutedMessage(8, "b", ErrorCode.None, 3),
                new Messages.RoutedMessage(9, "gone", ErrorCode.None, 5),
                new Messages.RoutedMessage(7, string.Empty, ErrorCode.None, 41));
        });
        Published got = await _client!.PublishAsync("x", "k", OutMessage.FromValue("hello"), mandatory: true);
        Assert.Equal(
            new[] { new RoutedQueue("a", 41), new RoutedQueue("b", 3), new RoutedQueue("gone", 5) },
            got.Routed.ToArray());
        Messages.PublishEntry e = sent!.Entries[0];
        Assert.Equal(20UL, e.ExchangeId);
        Assert.Equal("k", e.RoutingKey);
        Assert.True(e.Mandatory);
        Assert.Null(sent.TxnId);
    }

    [Fact]
    public async Task SomeQueuesOnlyAreReportedAsSuch()
    {
        Answer(_ => Routed(
            new Messages.RoutedMessage(7, "a", ErrorCode.None, 41),
            new Messages.RoutedMessage(8, "b", ErrorCode.RequestTimedOut, 0)));
        var e = await Assert.ThrowsAsync<NotAllWrittenException>(() => _client!.PublishAsync("x", "k", OutMessage.FromValue("m")));
        Assert.Equal(new[] { "a" }, e.Written);
        Assert.IsType<OutcomeUnknownException>(e.Failures[0]);
    }

    [Fact]
    public async Task ARefusedQueueIsAFailureThatDidNotHappen()
    {
        Answer(_ => Routed(
            new Messages.RoutedMessage(7, "a", ErrorCode.None, 41),
            new Messages.RoutedMessage(8, "b", ErrorCode.Unauthorized, 0)));
        var e = await Assert.ThrowsAsync<NotAllWrittenException>(() => _client!.PublishAsync("x", "k", OutMessage.FromValue("m")));
        Assert.Equal(ErrorCode.Unauthorized, Assert.IsType<BrokerErrorException>(e.Failures[0]).Code);
    }

    [Fact]
    public async Task AMandatoryMessageNoQueueTakesIsRefused()
    {
        Answer(_ => new Response.Publish(0, new[] { new Messages.PublishResult(ErrorCode.Unroutable, Array.Empty<Messages.RoutedMessage>()) }));
        await Assert.ThrowsAsync<UnroutableException>(() => _client!.PublishAsync("x", "nobody", OutMessage.FromValue("m"), mandatory: true));
    }

    [Fact]
    public async Task AMessageNoQueueTakesIsDroppedUnlessMandatory()
    {
        Answer(_ => Routed());
        Published p = await _client!.PublishAsync("x", "nobody", OutMessage.FromValue("m"));
        Assert.Empty(p.Routed);
    }

    [Fact]
    public async Task AnExchangeThatWasDeletedAndMadeAgainIsLookedUpOnce()
    {
        int publishes = 0;
        Answer(p => Interlocked.Increment(ref publishes) == 1
            ? new Response.Publish(0, new[] { new Messages.PublishResult(ErrorCode.UnknownResource, Array.Empty<Messages.RoutedMessage>()) })
            : Routed(new Messages.RoutedMessage(7, "a", ErrorCode.None, 1)));
        Published got = await _client!.PublishAsync("x", "k", OutMessage.FromValue("m"));
        Assert.Equal(2, publishes);
        Assert.Single(got.Routed);
        Assert.Equal(2, _a.Count<Request.Metadata>()); // asked again for the exchange's id
    }

    [Fact]
    public async Task AnUnknownExchangeIsReported()
    {
        Answer(_ => new Response.Publish(0, new[] { new Messages.PublishResult(ErrorCode.UnknownResource, Array.Empty<Messages.RoutedMessage>()) }));
        await Assert.ThrowsAsync<UnknownStreamException>(() => _client!.PublishAsync("nobody", "k", OutMessage.FromValue("m")));
        Assert.Equal(0, _a.Count<Request.Publish>());
    }

    [Fact]
    public async Task ALostAnswerIsNotSentAgain()
    {
        Answer(_ => throw new FakeNode.Drop());
        await Assert.ThrowsAsync<OutcomeUnknownException>(() => _client!.PublishAsync("x", "k", OutMessage.FromValue("m")));
        Assert.Equal(1, _a.Count<Request.Publish>()); // the node may have written it
    }

    [Fact]
    public async Task ANodeThatIsDownIsSkippedBeforeTheMessageWasSent()
    {
        using var down = new FakeNode(2);
        Address gone = down.Address;
        down.Kill();
        _a.OnConn((n, conn, request) => request switch
        {
            Request.Metadata => Fx.Metadata(new[] { _a }, Resource("x", 20, Messages.ResourceKind.Exchange)),
            Request.Publish => Routed(new Messages.RoutedMessage(7, "a", ErrorCode.None, 1)),
            _ => throw new InvalidOperationException("unexpected " + request),
        });
        _client = new LotseClient(ClientConfig.CreateBuilder(gone.ToString(), _a.HostPort).PlaintextOnLoopback()
            .WithRequestTimeout(TimeSpan.FromMilliseconds(500)).WithRetryFor(TimeSpan.FromSeconds(2)).WithBackoff(TimeSpan.FromMilliseconds(5)).Build());
        Published p = await _client.PublishAsync("x", "k", OutMessage.FromValue("m"));
        Assert.Single(p.Routed);
    }

    [Fact]
    public async Task APublishOutsideATransactionUsesTheNodeAndNotTheClientsRouting()
    {
        Answer(_ => Routed(new Messages.RoutedMessage(7, "a", ErrorCode.None, 9)));
        Published p = await _client!.PublishAsync("x", "anything", OutMessage.FromValue("m").WithHeader("h", "v"));
        Assert.Equal(9UL, p.Routed[0].Offset);
        Assert.Equal(0, _a.Count<Request.Produce>());
    }
}
