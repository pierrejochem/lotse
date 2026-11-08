using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Lotse.Net;
using Lotse.Proto;
using Lotse.Record;
using Xunit;

namespace Lotse.Client.Tests.Net;

public class ConnectionTests
{
    private static ConnectionOptions Plain() =>
        ConnectionOptions.CreateBuilder().PlaintextOnLoopback().WithRequestTimeout(TimeSpan.FromSeconds(5)).Build();

    /// <summary>A request whose answer tells which one it answers.</summary>
    private static Request.Unsubscribe Ask(uint n) => new(n);

    private static Response Answer(uint n) => new Response.Outcome(new ErrorCode((ushort)n), null);

    private static string Text(Response r) => "answer " + ((Response.Outcome)r).Error.Code;

    private static Frames.DeliverPush Push(uint subscription, ulong offset) =>
        new(subscription, ErrorCode.None, ByteString.Empty, new[] { new Messages.DeliveredOffset(offset, 1) }, null);

    /// <summary>Collects what a subscription is told.</summary>
    private sealed class Inbox : IPushListener
    {
        internal Channel<Frames.DeliverPush> Pushes { get; } = Channel.CreateUnbounded<Frames.DeliverPush>();

        internal Channel<ConnectionException> Closed { get; } = Channel.CreateUnbounded<ConnectionException>();

        public void OnPush(Frames.DeliverPush push) => Pushes.Writer.TryWrite(push);

        public void OnClose(ConnectionException reason) => Closed.Writer.TryWrite(reason);

        internal async Task<ulong> NextOffset()
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            Frames.DeliverPush p = await Pushes.Reader.ReadAsync(timeout.Token);
            return p.Deliveries[0].Offset;
        }

        internal async Task<bool> NothingWithin(int ms)
        {
            await Task.Delay(ms);
            return !Pushes.Reader.TryPeek(out _);
        }
    }

    private static async Task<T> Failure<T>(Task task)
        where T : Exception
    {
        return await Assert.ThrowsAsync<T>(async () => await task.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task RequestsArePipelinedAndAnsweredInAnyOrder()
    {
        var held = new ConcurrentQueue<Frames.DecodedRequest>();
        using var broker = new FakeBroker((peer, req) =>
        {
            held.Enqueue(req);
            if (held.Count == 3)
            {
                // All three are on the wire before any answer: pipelined. Answer the last first.
                foreach (Frames.DecodedRequest r in held.Reverse())
                {
                    peer.Reply(r.Header.CorrelationId, Answer(((Request.Unsubscribe)r.Request).SubscriptionId));
                }
            }
        });
        await using Connection c = await Connection.OpenAsync(broker.Address, Plain());
        Task<Response> a = c.SendAsync(Ask(10));
        Task<Response> b = c.SendAsync(Ask(20));
        Task<Response> d = c.SendAsync(Ask(30));
        Assert.Equal("answer 10", Text(await a.WaitAsync(TimeSpan.FromSeconds(5))));
        Assert.Equal("answer 20", Text(await b.WaitAsync(TimeSpan.FromSeconds(5))));
        Assert.Equal("answer 30", Text(await d.WaitAsync(TimeSpan.FromSeconds(5))));
        Assert.Equal(new uint[] { 1, 2, 3 }, held.Select(r => r.Header.CorrelationId).ToArray());
    }

    [Fact]
    public async Task ManyTasksMayShareOneConnection()
    {
        using var broker = new FakeBroker((peer, req) =>
            peer.Reply(req.Header.CorrelationId, Answer(((Request.Unsubscribe)req.Request).SubscriptionId)));
        await using Connection c = await Connection.OpenAsync(broker.Address, Plain());
        int wrong = 0;
        var tasks = new List<Task>();
        for (uint t = 0; t < 8; t++)
        {
            uint baseValue = t * 1000;
            tasks.Add(Task.Run(async () =>
            {
                for (uint i = 0; i < 100; i++)
                {
                    if (Text(await c.CallAsync(Ask(baseValue + i))) != "answer " + (baseValue + i))
                    {
                        Interlocked.Increment(ref wrong);
                    }
                }
            }));
        }
        await Task.WhenAll(tasks);
        Assert.Equal(0, wrong);
    }

    [Fact]
    public async Task CreditIsNeverAnsweredAndHasNoResponseSlot()
    {
        var seen = new ConcurrentQueue<Frames.DecodedRequest>();
        using var broker = new FakeBroker((peer, req) =>
        {
            seen.Enqueue(req);
            if (req.Request is not Request.Credit)
            {
                peer.Reply(req.Header.CorrelationId, Answer(1));
            }
        });
        await using Connection c = await Connection.OpenAsync(broker.Address, Plain());
        Assert.Throws<ArgumentException>(() => { _ = c.SendAsync(new Request.Credit(5, 1, 1)); });
        await c.SendOnlyAsync(new Request.Credit(5, 10, 1000));
        Assert.Equal("answer 1", Text(await c.CallAsync(Ask(1))));
        Frames.DecodedRequest[] all = seen.ToArray();
        Assert.Equal(2, all.Length);
        Assert.IsType<Request.Credit>(all[0].Request);
        Assert.Equal(1u, all[0].Header.CorrelationId);
        Assert.Equal(2u, all[1].Header.CorrelationId);
    }

    [Fact]
    public async Task AResponseNobodyWaitsForIsDropped()
    {
        using var broker = new FakeBroker((peer, req) =>
        {
            peer.Reply(777, Answer(0)); // no such request
            peer.Reply(req.Header.CorrelationId, Answer(1));
        });
        await using Connection c = await Connection.OpenAsync(broker.Address, Plain());
        Assert.Equal("answer 1", Text(await c.CallAsync(Ask(1))));
        Assert.Equal("answer 1", Text(await c.CallAsync(Ask(1))));
        Assert.True(c.IsOpen);
    }

    [Fact]
    public async Task PushesBeforeTheSubscriptionIsRegisteredAreKeptAndDeliveredInOrder()
    {
        using var broker = new FakeBroker((peer, req) =>
        {
            uint n = ((Request.Unsubscribe)req.Request).SubscriptionId;
            if (n == 1)
            {
                // The subscription's pushes come before anyone registered for it.
                peer.Push(Push(7, 100));
                peer.Push(Push(7, 101));
                peer.Push(Push(8, 200));
                peer.Push(Push(7, 102));
            }
            if (n == 2)
            {
                peer.Push(Push(7, 103));
            }
            peer.Reply(req.Header.CorrelationId, Answer(n));
        });
        await using Connection c = await Connection.OpenAsync(broker.Address, Plain());
        await c.CallAsync(Ask(1)); // when this returns, the read loop has routed the four pushes before it
        var seven = new Inbox();
        var eight = new Inbox();
        c.Register(7, seven);
        Assert.Equal(100ul, await seven.NextOffset());
        Assert.Equal(101ul, await seven.NextOffset());
        Assert.Equal(102ul, await seven.NextOffset());
        c.Register(8, eight);
        Assert.Equal(200ul, await eight.NextOffset());
        await c.CallAsync(Ask(2));
        Assert.Equal(103ul, await seven.NextOffset());
        Assert.True(await seven.NothingWithin(50));
        c.Unregister(7);
        c.Register(7, new Inbox());
    }

    [Fact]
    public async Task OnlySixtyFourSubscriptionsAreKeptWaiting()
    {
        using var broker = new FakeBroker((peer, req) =>
        {
            for (uint sub = 1; sub <= 70; sub++)
            {
                peer.Push(Push(sub, sub));
            }
            peer.Reply(req.Header.CorrelationId, Answer(1));
        });
        await using Connection c = await Connection.OpenAsync(broker.Address, Plain());
        await c.CallAsync(Ask(1));
        var evicted = new Inbox();
        var oldestKept = new Inbox();
        var newest = new Inbox();
        c.Register(6, evicted);
        c.Register(7, oldestKept);
        c.Register(70, newest);
        Assert.True(await evicted.NothingWithin(50), "the lowest ids made room");
        Assert.Equal(7ul, await oldestKept.NextOffset());
        Assert.Equal(70ul, await newest.NextOffset());
    }

    [Fact]
    public async Task UnregisteringForgetsTheKeptPushes()
    {
        using var broker = new FakeBroker((peer, req) =>
        {
            peer.Push(Push(5, 1));
            peer.Reply(req.Header.CorrelationId, Answer(1));
        });
        await using Connection c = await Connection.OpenAsync(broker.Address, Plain());
        await c.CallAsync(Ask(1));
        c.Unregister(5);
        var late = new Inbox();
        c.Register(5, late);
        Assert.True(await late.NothingWithin(50));
    }

    [Fact]
    public async Task AListenerThatThrowsDoesNotEndTheConnection()
    {
        using var broker = new FakeBroker((peer, req) =>
        {
            peer.Push(Push(5, 1));
            peer.Reply(req.Header.CorrelationId, Answer(1));
        });
        await using Connection c = await Connection.OpenAsync(broker.Address, Plain());
        c.Register(5, new Throwing());
        Assert.Equal("answer 1", Text(await c.CallAsync(Ask(1))));
        Assert.True(c.IsOpen);
    }

    private sealed class Throwing : IPushListener
    {
        public void OnPush(Frames.DeliverPush push) => throw new InvalidOperationException("listener failed");

        public void OnClose(ConnectionException reason)
        {
        }
    }

    [Fact]
    public async Task AResponseWithTheErrorFlagBecomesABrokerExceptionBeforeAnyTypedDecoding()
    {
        using var broker = new FakeBroker((peer, req) =>
        {
            if (req.Request is Request.Metadata)
            {
                // A body that is only an error code, for a request whose answer has a long typed body.
                peer.Reply(req.Header.CorrelationId, new Response.Failed(ErrorCode.Unauthorized));
            }
            else
            {
                peer.Reply(req.Header.CorrelationId, new Response.Refused(ErrorCode.InvalidRequest, "no such thing"));
            }
        });
        await using Connection c = await Connection.OpenAsync(broker.Address, Plain());
        BrokerException plain = await Failure<BrokerException>(c.SendAsync(new Request.Metadata(new[] { "x" })));
        Assert.Equal(ErrorCode.Unauthorized, plain.Code);
        Assert.Null(plain.Reason);
        BrokerException withReason = await Failure<BrokerException>(c.SendAsync(Ask(1)));
        Assert.Equal(ErrorCode.InvalidRequest, withReason.Code);
        Assert.Equal("no such thing", withReason.Reason);
        Assert.Contains("no such thing", withReason.Message);
        Assert.True(c.IsOpen, "a refused request does not end the connection");
    }

    [Fact]
    public async Task ATimedOutRequestFailsAndTheLateAnswerChangesNothing()
    {
        FakeBroker.Peer? peerRef = null;
        var ids = new ConcurrentQueue<uint>();
        using var broker = new FakeBroker((peer, req) =>
        {
            peerRef = peer;
            ids.Enqueue(req.Header.CorrelationId);
            if (ids.Count > 1)
            {
                peer.Reply(req.Header.CorrelationId, Answer(2));
            }
        });
        ConnectionOptions options =
            ConnectionOptions.CreateBuilder().PlaintextOnLoopback().WithRequestTimeout(TimeSpan.FromMilliseconds(150)).Build();
        await using Connection c = await Connection.OpenAsync(broker.Address, options);
        long start = Environment.TickCount64;
        RequestTimeoutException timeout = await Failure<RequestTimeoutException>(c.SendAsync(Ask(1)));
        Assert.True(timeout.RequestSent, "the outcome of a timed out request is unknown");
        Assert.True(Environment.TickCount64 - start >= 100);
        peerRef!.Reply(ids.First(), Answer(1)); // too late
        Assert.Equal("answer 2", Text(await c.CallAsync(Ask(2))));
        Assert.True(c.IsOpen);
    }

    [Fact]
    public async Task CancellingStopsTheWaitAndKeepsTheConnection()
    {
        using var broker = new FakeBroker((peer, req) =>
        {
            if (((Request.Unsubscribe)req.Request).SubscriptionId == 2)
            {
                peer.Reply(req.Header.CorrelationId, Answer(2));
            }
        });
        await using Connection c = await Connection.OpenAsync(broker.Address, Plain());
        using var cts = new CancellationTokenSource();
        Task<Response> pending = c.SendAsync(Ask(1), cts.Token);
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await pending);
        Assert.Equal("answer 2", Text(await c.CallAsync(Ask(2))));
    }

    [Fact]
    public async Task ClosingFailsEveryPendingRequestAndNotifiesSubscriptions()
    {
        using var broker = new FakeBroker((peer, req) => { });
        Connection c = await Connection.OpenAsync(broker.Address, Plain());
        var inbox = new Inbox();
        c.Register(3, inbox);
        Task<Response> a = c.SendAsync(Ask(1));
        Task<Response> b = c.SendAsync(Ask(2));
        c.Dispose();
        foreach (Task<Response> f in new[] { a, b })
        {
            ConnectionException e = await Failure<ConnectionException>(f);
            Assert.True(e.RequestSent);
        }
        await inbox.Closed.Reader.ReadAsync(new CancellationTokenSource(TimeSpan.FromSeconds(5)).Token);
        Assert.False(c.IsOpen);
        ConnectionException later = await Failure<ConnectionException>(c.SendAsync(Ask(3)));
        Assert.False(later.RequestSent, "nothing was sent on a closed connection");
        Assert.Throws<ConnectionException>(() => c.Register(4, new Inbox()));
        await Assert.ThrowsAsync<ConnectionException>(() => c.SendOnlyAsync(new Request.Credit(1, 1, 1)));
        c.Dispose(); // closing twice is fine
    }

    [Fact]
    public async Task TheBrokerClosingTheConnectionFailsPendingRequests()
    {
        using var broker = new FakeBroker((peer, req) => peer.CloseSocket());
        await using Connection c = await Connection.OpenAsync(broker.Address, Plain());
        var inbox = new Inbox();
        c.Register(3, inbox);
        ConnectionException e = await Failure<ConnectionException>(c.SendAsync(Ask(1)));
        Assert.Contains("clos", e.Message);
        await inbox.Closed.Reader.ReadAsync(new CancellationTokenSource(TimeSpan.FromSeconds(5)).Token);
        await Eventually.AwaitAsync("the connection to end", () => !c.IsOpen);
    }

    [Fact]
    public async Task AFrameOverTheLimitEndsTheConnection()
    {
        using var broker = new FakeBroker((peer, req) => peer.RawBytes(new byte[] { 1, 0, 0, 1 }));
        await using Connection c = await Connection.OpenAsync(broker.Address, Plain());
        ConnectionException e = await Failure<ConnectionException>(c.SendAsync(Ask(1)));
        Assert.Contains("limit", e.Message);
        Assert.False(c.IsOpen);
    }

    [Fact]
    public async Task ARequestOverTheLimitIsNotSentAndTheConnectionStaysUsable()
    {
        using var broker = new FakeBroker((peer, req) => peer.Reply(req.Header.CorrelationId, Answer(1)));
        await using Connection c = await Connection.OpenAsync(broker.Address, Plain());
        byte[] big = new byte[Frames.MaxFrame + 1];
        ConnectionException e = await Failure<ConnectionException>(c.SendAsync(new Request.SaslAuthenticate(ByteString.CopyOf(big))));
        Assert.False(e.RequestSent);
        Assert.Equal("answer 1", Text(await c.CallAsync(Ask(1))));
    }

    [Fact]
    public async Task AnUndecodableResponseEndsTheConnection()
    {
        using var broker = new FakeBroker((peer, req) =>
        {
            var w = new Writer();
            w.U32(req.Header.CorrelationId).U16(0).NoTags();
            w.U8(9); // far too short for a Metadata response
            peer.Raw(w.ToByteArray());
        });
        await using Connection c = await Connection.OpenAsync(broker.Address, Plain());
        ConnectionException e = await Failure<ConnectionException>(c.SendAsync(new Request.Metadata(Array.Empty<string>())));
        Assert.Contains("undecodable", e.Message);
        await Eventually.AwaitAsync("the connection to end", () => !c.IsOpen);
    }

    [Fact]
    public async Task ApiVersionsIsOptionalButAvailable()
    {
        using var broker = new FakeBroker((peer, req) =>
        {
            Assert.IsType<Request.ApiVersions>(req.Request);
            peer.Reply(req.Header.CorrelationId, new Response.ApiVersions(new[] { new ApiVersionRange(ApiKey.Metadata, 0, 0) }, new[] { "streams" }));
        });
        await using Connection c = await Connection.OpenAsync(broker.Address, Plain());
        Response.ApiVersions v = await c.ApiVersionsAsync();
        Assert.Equal(new[] { "streams" }, v.Features);
        Assert.Single(v.Versions);
    }

    [Fact]
    public async Task AWrongTypeOfResponseIsReported()
    {
        using var broker = new FakeBroker((peer, req) => peer.Reply(req.Header.CorrelationId, Answer(1)));
        await using Connection c = await Connection.OpenAsync(broker.Address, Plain());
        await Assert.ThrowsAsync<ConnectionException>(() => c.CallAsync<Response.Metadata>(Ask(1)));
    }

    [Fact]
    public async Task CorrelationIdsWrapPastZero()
    {
        var ids = new ConcurrentQueue<uint>();
        using var broker = new FakeBroker((peer, req) =>
        {
            ids.Enqueue(req.Header.CorrelationId);
            peer.Reply(req.Header.CorrelationId, Answer(1));
        });
        await using Connection c = await Connection.OpenAsync(broker.Address, Plain());
        typeof(Connection).GetField("_lastId", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
            .SetValue(c, uint.MaxValue - 1);
        for (int i = 0; i < 3; i++)
        {
            await c.CallAsync(Ask(1));
        }
        Assert.Equal(new uint[] { uint.MaxValue, 1, 2 }, ids.ToArray());
    }

    [Fact]
    public async Task ConnectingToNothingFailsWithoutHanging()
    {
        ConnectionException e = await Assert.ThrowsAsync<ConnectionException>(
            () => Connection.OpenAsync(new Address("127.0.0.1", 1), Plain()));
        Assert.False(e.RequestSent);
    }
}
