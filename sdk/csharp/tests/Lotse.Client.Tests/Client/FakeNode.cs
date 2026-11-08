using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using Lotse.Net;
using Lotse.Proto;

namespace Lotse.Client.Tests.Client;

/// <summary>
/// A broker in this process, on loopback, that answers as a test tells it to. Plain TCP. Every request is handled on a
/// task of its own, so a handler that waits does not hold up the requests behind it on the same connection.
/// </summary>
internal sealed class FakeNode : IDisposable
{
    /// <summary>What a test does with a request. Return <c>null</c> to stay silent; throw <see cref="Drop"/> to close the connection.</summary>
    internal delegate Task<Response?> Handler(FakeNode node, Request request);

    /// <summary>Close the connection without answering.</summary>
    internal sealed class Drop : Exception
    {
        internal Drop()
            : base("drop the connection")
        {
        }
    }

    private readonly TcpListener _server;
    private readonly ConcurrentQueue<Request> _received = new();
    private readonly ConcurrentBag<Socket> _sockets = new();
    private volatile Handler _handler;

    internal FakeNode(uint id)
    {
        Id = id;
        _handler = (node, request) => throw new InvalidOperationException($"node {node.Id} was not expected to get {request}");
        _server = new TcpListener(IPAddress.Loopback, 0);
        _server.Start();
        // Read once: a stopped listener no longer knows its port.
        Address = new Address("127.0.0.1", ((IPEndPoint)_server.LocalEndpoint).Port);
        _ = Task.Run(AcceptLoop);
    }

    internal uint Id { get; }

    internal Address Address { get; }

    internal string HostPort => Address.ToString();

    internal FakeNode On(Func<FakeNode, Request, Response?> handler)
    {
        _handler = (n, r) => Task.FromResult(handler(n, r));
        return this;
    }

    internal FakeNode OnAsync(Handler handler)
    {
        _handler = handler;
        return this;
    }

    /// <summary>Every request this node received, in arrival order.</summary>
    internal List<Request> Requests() => _received.ToList();

    internal List<T> Requests<T>()
        where T : Request => _received.OfType<T>().ToList();

    internal int Count<T>()
        where T : Request => _received.OfType<T>().Count();

    private async Task AcceptLoop()
    {
        while (true)
        {
            Socket s;
            try
            {
                s = await _server.AcceptSocketAsync();
            }
            catch (Exception)
            {
                return;
            }
            s.NoDelay = true;
            _sockets.Add(s);
            _ = Task.Run(() => Serve(s));
        }
    }

    private async Task Serve(Socket socket)
    {
        var write = new object();
        try
        {
            await using var stream = new NetworkStream(socket, ownsSocket: false);
            byte[] prefix = new byte[4];
            while (true)
            {
                await stream.ReadExactlyAsync(prefix);
                byte[] frame = new byte[Frames.ReadLength(prefix)];
                await stream.ReadExactlyAsync(frame);
                Frames.DecodedRequest decoded = Frames.DecodeRequest(frame);
                _received.Enqueue(decoded.Request);
                _ = Task.Run(() => Answer(socket, stream, write, decoded));
            }
        }
        catch (Exception)
        {
            // the peer or the test closed the socket
        }
    }

    private async Task Answer(Socket socket, NetworkStream stream, object write, Frames.DecodedRequest decoded)
    {
        try
        {
            Response? response = await _handler(this, decoded.Request);
            if (response is null)
            {
                return;
            }
            byte[] wire = Frames.WithLengthPrefix(Frames.EncodeResponse(decoded.Header.CorrelationId, response));
            lock (write)
            {
                stream.Write(wire);
                stream.Flush();
            }
        }
        catch (Exception)
        {
            try
            {
                socket.Close();
            }
            catch (Exception)
            {
                // closing
            }
        }
    }

    /// <summary>Stops the node: it no longer accepts, and its connections are closed.</summary>
    internal void Kill()
    {
        _server.Stop();
        foreach (Socket s in _sockets)
        {
            try
            {
                s.Close();
            }
            catch (Exception)
            {
                // closing
            }
        }
    }

    public void Dispose() => Kill();
}
