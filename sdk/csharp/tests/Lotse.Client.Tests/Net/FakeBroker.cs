using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Lotse.Net;
using Lotse.Proto;

namespace Lotse.Client.Tests.Net;

/// <summary>A broker in this process, on loopback, that answers as a test tells it to. Plain TCP.</summary>
internal sealed class FakeBroker : IDisposable
{
    /// <summary>What a test does with each request.</summary>
    internal delegate Task Handler(Peer peer, Frames.DecodedRequest request);

    /// <summary>One accepted connection.</summary>
    internal sealed class Peer
    {
        private readonly object _write = new();
        private readonly Socket _socket;
        private readonly Stream _stream;

        internal Peer(Socket socket)
        {
            _socket = socket;
            _stream = new NetworkStream(socket, ownsSocket: false);
        }

        internal Stream Stream => _stream;

        internal ConcurrentQueue<Frames.DecodedRequest> Received { get; } = new();

        internal void Reply(uint correlationId, Response response) => Raw(Frames.EncodeResponse(correlationId, response));

        internal void Push(Frames.DeliverPush push) => Raw(Frames.EncodePush(push));

        internal void Raw(byte[] frame) => RawBytes(Frames.WithLengthPrefix(frame));

        internal void RawBytes(byte[] bytes)
        {
            lock (_write)
            {
                _stream.Write(bytes);
                _stream.Flush();
            }
        }

        internal void CloseSocket()
        {
            try
            {
                _socket.Shutdown(SocketShutdown.Both);
            }
            catch (Exception e) when (e is SocketException or ObjectDisposedException)
            {
            }
            _socket.Close();
        }
    }

    private readonly TcpListener _server;
    private readonly Handler _handler;
    private int _accepted;

    internal FakeBroker(Handler handler)
    {
        _server = new TcpListener(IPAddress.Loopback, 0);
        _server.Start();
        _handler = handler;
        _ = Task.Run(AcceptLoop);
    }

    internal FakeBroker(Action<Peer, Frames.DecodedRequest> handler)
        : this((p, r) =>
        {
            handler(p, r);
            return Task.CompletedTask;
        })
    {
    }

    internal int Accepted => Volatile.Read(ref _accepted);

    internal ConcurrentBag<Peer> Peers { get; } = new();

    internal Address Address => new("127.0.0.1", ((IPEndPoint)_server.LocalEndpoint).Port);

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
            Interlocked.Increment(ref _accepted);
            var peer = new Peer(s);
            Peers.Add(peer);
            _ = Task.Run(() => Serve(peer));
        }
    }

    private async Task Serve(Peer peer)
    {
        try
        {
            byte[] prefix = new byte[4];
            while (true)
            {
                await peer.Stream.ReadExactlyAsync(prefix);
                byte[] frame = new byte[Frames.ReadLength(prefix)];
                await peer.Stream.ReadExactlyAsync(frame);
                Frames.DecodedRequest request = Frames.DecodeRequest(frame);
                peer.Received.Enqueue(request);
                await _handler(peer, request);
            }
        }
        catch (Exception)
        {
            // the peer or the test closed the socket
        }
    }

    public void Dispose()
    {
        _server.Stop();
        foreach (Peer p in Peers)
        {
            p.CloseSocket();
        }
    }
}
