using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Lotse.Auth;
using Lotse.Proto;

namespace Lotse.Net;

/// <summary>
/// One persistent connection to one broker address.
/// <para>
/// A read loop decodes the frames the broker sends. Requests are pipelined: <see cref="SendAsync"/> queues the request
/// and returns a task, and the response completes it whenever it arrives, in any order, matched by correlation id.
/// Requests go out in the order they were sent, one frame at a time. Correlation ids start at 1 and wrap past 0 (0
/// marks pushes). Pushes go to the <see cref="IPushListener"/> registered for their subscription; a push that arrives
/// before its subscription is registered is kept, for at most <see cref="OrphanSubscriptions"/> subscriptions, and
/// delivered on registration.
/// </para>
/// <para>
/// A response with the <c>ERROR</c> flag fails the task with <see cref="BrokerException"/> before any typed decoding. A
/// connection that breaks, is closed, or receives something it cannot decode fails every pending task with
/// <see cref="ConnectionException"/>, and cannot be used again. Frames larger than <see cref="Frames.MaxFrame"/> end
/// the connection. So does the expiry of the certificate the node showed (ADR 0068).
/// </para>
/// <para>Thread-safe.</para>
/// </summary>
public sealed class Connection : IAsyncDisposable, IDisposable
{
    /// <summary>Subscriptions with pushes waiting for <see cref="Register"/> that are kept; the one with the lowest id is dropped for more.</summary>
    public const int OrphanSubscriptions = 64;

    private static readonly TimeSpan HandshakeTimeout = TimeSpan.FromSeconds(10);

    /// <summary>Longest lifetime kept, so that arithmetic on it in nanoseconds cannot overflow: about 36 years.</summary>
    private const long MaxLifetimeMs = 1_000_000_000_000L;

    private sealed record Slot(ApiKey Key, TaskCompletionSource<Response> Source);

    private sealed record SessionTimes(long RenewAt, long EndsAt);

    private sealed record Outgoing(byte[] Wire, TaskCompletionSource? Written);

    private readonly Stream _stream;
    private readonly ConnectionOptions _options;
    private readonly Ticker _ticker;
    private readonly object _lock = new();
    private readonly Dictionary<uint, Slot> _slots = new();
    private readonly Dictionary<uint, IPushListener> _subscriptions = new();
    private readonly SortedDictionary<uint, List<Frames.DeliverPush>> _orphans = new();
    private readonly Channel<Outgoing> _outgoing = Channel.CreateUnbounded<Outgoing>(
        new UnboundedChannelOptions { SingleReader = true, AllowSynchronousContinuations = false });
    private readonly CancellationTokenSource _ended = new();
    private uint _lastId;
    private ConnectionException? _broken;
    private volatile SessionTimes? _session;

    private Connection(Address address, Stream stream, bool secure, ConnectionOptions options)
    {
        Address = address;
        _stream = stream;
        IsSecure = secure;
        _options = options;
        _ticker = options.Ticker;
    }

    // ---- opening ----

    /// <summary>
    /// Connects to <paramref name="address"/> and, if the options carry credentials, authenticates.
    /// </summary>
    /// <exception cref="InsecureTransportException">
    /// The options name no transport, or plain TCP is asked for an address that is not loopback; nothing is sent.
    /// </exception>
    /// <exception cref="ConnectionException">
    /// The broker cannot be reached or the TLS handshake fails (the certificate is not signed by the given CA, or is not
    /// valid for the host). The inner exception says why.
    /// </exception>
    /// <exception cref="AuthenticationException">The broker does not accept the credentials.</exception>
    public static async Task<Connection> OpenAsync(Address address, ConnectionOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(address);
        ArgumentNullException.ThrowIfNull(options);
        bool tls = options.Tls is not null;
        if (!tls && !options.AllowsPlaintextOnLoopback)
        {
            throw new InsecureTransportException(
                "the transport is not configured: give the options TLS (Tls), or allow plain TCP on loopback "
                + "(PlaintextOnLoopback)");
        }
        IPAddress[] resolved;
        try
        {
            resolved = IPAddress.TryParse(address.Host, out IPAddress? literal)
                ? new[] { literal }
                : await Dns.GetHostAddressesAsync(address.Host, cancellationToken).ConfigureAwait(false);
        }
        catch (SocketException e)
        {
            throw new ConnectionException("cannot resolve " + address.Host, false, e);
        }
        if (resolved.Length == 0)
        {
            throw new ConnectionException("cannot resolve " + address.Host, false);
        }
        if (!tls)
        {
            foreach (IPAddress a in resolved)
            {
                if (!IPAddress.IsLoopback(a))
                {
                    throw new InsecureTransportException(
                        $"{address} is not a loopback address; plain TCP is only allowed on loopback, configure TLS");
                }
            }
        }
        Connection? connection = null;
        Exception? last = null;
        foreach (IPAddress target in resolved)
        {
            var socket = new Socket(target.AddressFamily, SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
            try
            {
                socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
                using (var connectTimeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken))
                {
                    connectTimeout.CancelAfter(options.ConnectTimeout);
                    await socket.ConnectAsync(new IPEndPoint(target, address.Port), connectTimeout.Token).ConfigureAwait(false);
                }
                Stream stream = new NetworkStream(socket, ownsSocket: true);
                DateTimeOffset? expiry = null;
                if (tls)
                {
                    (stream, expiry) = await HandshakeAsync(stream, address, options.Tls!, cancellationToken).ConfigureAwait(false);
                }
                connection = new Connection(address, stream, tls, options);
                connection.Start(expiry);
                break;
            }
            catch (Exception e) when (e is IOException or SocketException or System.Security.Authentication.AuthenticationException
                                          || (e is OperationCanceledException && !cancellationToken.IsCancellationRequested))
            {
                last = e;
                socket.Dispose();
            }
            catch
            {
                socket.Dispose();
                throw;
            }
        }
        if (connection is null)
        {
            throw new ConnectionException($"cannot connect to {address}: {Describe(last)}", false, last);
        }
        if (options.Credentials is { } credentials)
        {
            try
            {
                await connection.AuthenticateAsync(credentials, cancellationToken).ConfigureAwait(false);
            }
            catch (ConnectionException e)
            {
                connection.Dispose();
                // Nothing else went out on this connection.
                throw new ConnectionException(e.Message, false, e);
            }
            catch
            {
                connection.Dispose();
                throw;
            }
        }
        return connection;
    }

    private static string Describe(Exception? e) => e is null ? "no address" : e.Message;

    private static async Task<(Stream, DateTimeOffset?)> HandshakeAsync(
        Stream transport, Address address, TlsConfig tls, CancellationToken cancellationToken)
    {
        var ssl = new SslStream(transport, leaveInnerStreamOpen: false);
        try
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(HandshakeTimeout);
            // Checks the certificate against the host dialled: its DNS names, or, for an IP address, its IP entries.
            await ssl.AuthenticateAsClientAsync(tls.Options(address.Host), timeout.Token).ConfigureAwait(false);
        }
        catch
        {
            await ssl.DisposeAsync().ConfigureAwait(false);
            throw;
        }
        DateTimeOffset? notAfter = null;
        if (ssl.RemoteCertificate is { } remote)
        {
            notAfter = (remote as X509Certificate2 ?? new X509Certificate2(remote)).NotAfter.ToUniversalTime();
        }
        return (ssl, notAfter);
    }

    private void Start(DateTimeOffset? certificateExpiry)
    {
        _ = Task.Run(ReadLoopAsync);
        _ = Task.Run(WriteLoopAsync);
        if (certificateExpiry is { } notAfter)
        {
            _ = Task.Run(() => WatchCertificateAsync(notAfter));
        }
    }

    // ---- state ----

    /// <summary>The address this connection is to.</summary>
    public Address Address { get; }

    /// <summary>Whether the connection is encrypted with TLS.</summary>
    public bool IsSecure { get; }

    /// <summary>Whether the connection is usable: not closed and not broken.</summary>
    public bool IsOpen
    {
        get
        {
            lock (_lock)
            {
                return _broken is null;
            }
        }
    }

    /// <summary>Why the connection ended, or <c>null</c> while it is open.</summary>
    public ConnectionException? Failure
    {
        get
        {
            lock (_lock)
            {
                return _broken;
            }
        }
    }

    /// <summary>Where the session stands, by the options' <see cref="Net.Ticker"/>.</summary>
    public SessionState SessionState
    {
        get
        {
            SessionTimes? s = _session;
            if (s is null)
            {
                return SessionState.Unlimited;
            }
            long now = _ticker();
            if (now - s.EndsAt >= 0)
            {
                return SessionState.Ended;
            }
            return now - s.RenewAt >= 0 ? SessionState.RenewDue : SessionState.Active;
        }
    }

    // ---- authentication ----

    /// <summary>
    /// Authenticates this connection, or authenticates again before the session ends (the node insists on the same
    /// principal). The session lifetime is taken from the last response, and counted from before the first request, so
    /// the connection never believes in a session longer than the node does.
    /// </summary>
    /// <exception cref="AuthenticationException">The node refuses.</exception>
    /// <exception cref="ConnectionException">The connection fails meanwhile.</exception>
    public async Task AuthenticateAsync(Credentials credentials, CancellationToken cancellationToken = default)
    {
        long start = _ticker();
        ulong lifetimeMs = await Authenticator
            .AuthenticateAsync(request => CallAsync(request, cancellationToken), credentials, _options.Random, cancellationToken)
            .ConfigureAwait(false);
        // u64 on the wire: a value with the top bit set is far beyond any real lifetime.
        long clamped = lifetimeMs > MaxLifetimeMs ? MaxLifetimeMs : (long)lifetimeMs;
        if (lifetimeMs == 0)
        {
            _session = null;
        }
        else
        {
            long nanos = clamped * 1_000_000L;
            _session = new SessionTimes(start + nanos / 5 * 4, start + nanos);
        }
    }

    // ---- requests ----

    /// <summary>
    /// Sends a request and returns the task of its response. The task fails with <see cref="BrokerException"/> if the
    /// node refuses the request as a whole, with <see cref="RequestTimeoutException"/> if no response comes within the
    /// request timeout, and with <see cref="ConnectionException"/> if the connection fails first. Cancelling
    /// <paramref name="cancellationToken"/> stops the wait; the request may still have reached the node, and a late
    /// response is dropped.
    /// </summary>
    /// <exception cref="ArgumentException"><c>Credit</c>, which is never answered: use <see cref="SendOnlyAsync"/>.</exception>
    public Task<Response> SendAsync(Request request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (request.ApiKey.Equals(ApiKey.Credit))
        {
            throw new ArgumentException("Credit is never answered; use SendOnlyAsync", nameof(request));
        }
        uint id;
        lock (_lock)
        {
            if (_broken is not null)
            {
                return Task.FromException<Response>(NotSent(_broken));
            }
            id = NextId();
        }
        byte[] wire;
        try
        {
            wire = Frames.WithLengthPrefix(Frames.EncodeRequest(id, request));
        }
        catch (Exception e) when (e is ArgumentException or InvalidOperationException or OverflowException)
        {
            return Task.FromException<Response>(new ConnectionException("cannot encode the request: " + e.Message, false, e));
        }
        var source = new TaskCompletionSource<Response>(TaskCreationOptions.RunContinuationsAsynchronously);
        lock (_lock)
        {
            if (_broken is not null)
            {
                return Task.FromException<Response>(NotSent(_broken));
            }
            _slots[id] = new Slot(request.ApiKey, source);
            // Queued under the lock: requests leave in the order they were registered.
            _outgoing.Writer.TryWrite(new Outgoing(wire, null));
        }
        return WaitAsync(id, source, cancellationToken);
    }

    private async Task<Response> WaitAsync(uint id, TaskCompletionSource<Response> source, CancellationToken cancellationToken)
    {
        using var timeout = new CancellationTokenSource(_options.RequestTimeout);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeout.Token);
        try
        {
            return await source.Task.WaitAsync(linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested && timeout.IsCancellationRequested)
        {
            throw new RequestTimeoutException(_options.RequestTimeout);
        }
        finally
        {
            lock (_lock)
            {
                _slots.Remove(id);
            }
        }
    }

    /// <summary>
    /// Sends a request nobody waits for: <c>Credit</c>, which the broker never answers. An answer that comes anyway is
    /// dropped. The task completes when the request has been written.
    /// </summary>
    /// <exception cref="ConnectionException">The connection is broken or closed; the request was not sent, or not fully.</exception>
    public async Task SendOnlyAsync(Request request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        uint id;
        var written = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        byte[] wire;
        lock (_lock)
        {
            if (_broken is not null)
            {
                throw NotSent(_broken);
            }
            id = NextId();
            wire = Frames.WithLengthPrefix(Frames.EncodeRequest(id, request));
            _outgoing.Writer.TryWrite(new Outgoing(wire, written));
        }
        await written.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Sends a request and waits for the response; the awaiting form of <see cref="SendAsync"/>.</summary>
    public Task<Response> CallAsync(Request request, CancellationToken cancellationToken = default) =>
        SendAsync(request, cancellationToken);

    /// <summary>Sends a request and waits for a response of type <typeparamref name="T"/>.</summary>
    /// <exception cref="ConnectionException">The response is of another type.</exception>
    public async Task<T> CallAsync<T>(Request request, CancellationToken cancellationToken = default)
        where T : Response
    {
        Response r = await SendAsync(request, cancellationToken).ConfigureAwait(false);
        if (r is not T typed)
        {
            throw new ConnectionException($"expected {typeof(T).Name} but got {r.GetType().Name}", true);
        }
        return typed;
    }

    /// <summary>Asks the node for the API versions it supports. The client does not need the answer to work; it is there to look at.</summary>
    public Task<Response.ApiVersions> ApiVersionsAsync(CancellationToken cancellationToken = default) =>
        CallAsync<Response.ApiVersions>(new Request.ApiVersions(), cancellationToken);

    private uint NextId()
    {
        // u32, never 0.
        unchecked
        {
            _lastId++;
        }
        if (_lastId == 0)
        {
            _lastId = 1;
        }
        return _lastId;
    }

    private static ConnectionException NotSent(ConnectionException why) => new(why.Message, false, why);

    // ---- writing ----

    private async Task WriteLoopAsync()
    {
        try
        {
            await foreach (Outgoing item in _outgoing.Reader.ReadAllAsync(_ended.Token).ConfigureAwait(false))
            {
                try
                {
                    await _stream.WriteAsync(item.Wire, _ended.Token).ConfigureAwait(false);
                    await _stream.FlushAsync(_ended.Token).ConfigureAwait(false);
                }
                catch (Exception e) when (e is IOException or SocketException or ObjectDisposedException or OperationCanceledException)
                {
                    var failure = new ConnectionException("send failed: " + Describe(e), true, e);
                    Fail(failure);
                    item.Written?.TrySetException(failure);
                    return;
                }
                item.Written?.TrySetResult();
            }
        }
        catch (OperationCanceledException)
        {
            // the connection ended
        }
        finally
        {
            // Whatever was queued and never written fails with the reason the connection ended.
            ConnectionException reason = Failure ?? new ConnectionException("connection closed", true);
            while (_outgoing.Reader.TryRead(out Outgoing? left))
            {
                left.Written?.TrySetException(reason);
            }
        }
    }

    // ---- pushes ----

    /// <summary>
    /// Registers the listener of subscription <paramref name="subscriptionId"/>. Pushes that arrived earlier are
    /// delivered to it first, in order, before this method returns; later ones follow.
    /// </summary>
    /// <exception cref="ConnectionException">The connection is broken or closed.</exception>
    public void Register(uint subscriptionId, IPushListener listener)
    {
        ArgumentNullException.ThrowIfNull(listener);
        lock (_lock)
        {
            if (_broken is not null)
            {
                throw NotSent(_broken);
            }
            _subscriptions[subscriptionId] = listener;
            if (_orphans.Remove(subscriptionId, out List<Frames.DeliverPush>? early))
            {
                foreach (Frames.DeliverPush push in early)
                {
                    Deliver(listener, push);
                }
            }
        }
    }

    /// <summary>Stops delivering pushes of the subscription and forgets any that were kept for it.</summary>
    public void Unregister(uint subscriptionId)
    {
        lock (_lock)
        {
            _subscriptions.Remove(subscriptionId);
            _orphans.Remove(subscriptionId);
        }
    }

    private void Route(Frames.DeliverPush push)
    {
        lock (_lock)
        {
            if (_subscriptions.TryGetValue(push.SubscriptionId, out IPushListener? listener))
            {
                Deliver(listener, push);
                return;
            }
            if (!_orphans.ContainsKey(push.SubscriptionId) && _orphans.Count >= OrphanSubscriptions)
            {
                _orphans.Remove(_orphans.First().Key);
            }
            if (!_orphans.TryGetValue(push.SubscriptionId, out List<Frames.DeliverPush>? kept))
            {
                kept = new List<Frames.DeliverPush>();
                _orphans[push.SubscriptionId] = kept;
            }
            kept.Add(push);
        }
    }

    private static void Deliver(IPushListener listener, Frames.DeliverPush push)
    {
        try
        {
            listener.OnPush(push);
        }
        catch (Exception)
        {
            // A listener's failure is its own; it must not end the connection.
        }
    }

    // ---- reading ----

    private async Task ReadLoopAsync()
    {
        byte[] prefix = new byte[4];
        try
        {
            while (true)
            {
                if (!await ReadFullAsync(prefix, true).ConfigureAwait(false))
                {
                    Fail(new ConnectionException("the broker closed the connection", true));
                    return;
                }
                int length = Frames.ReadLength(prefix);
                byte[] frame = new byte[length];
                if (length > 0 && !await ReadFullAsync(frame, false).ConfigureAwait(false))
                {
                    throw new EndOfStreamException("the broker closed the connection inside a frame");
                }
                Handle(frame);
            }
        }
        catch (DecodeException e)
        {
            Fail(new ConnectionException("undecodable frame: " + e.Message, true, e));
        }
        catch (Exception e) when (e is IOException or SocketException or ObjectDisposedException or OperationCanceledException)
        {
            Fail(new ConnectionException("connection broke: " + Describe(e), true, e));
        }
        catch (Exception e)
        {
            Fail(new ConnectionException("reader failed: " + e, true, e));
        }
    }

    /// <summary>Fills <paramref name="buffer"/>; false if the stream ended before its first byte and that is allowed.</summary>
    private async Task<bool> ReadFullAsync(byte[] buffer, bool endAllowedAtStart)
    {
        int read = 0;
        while (read < buffer.Length)
        {
            int n = await _stream.ReadAsync(buffer.AsMemory(read), _ended.Token).ConfigureAwait(false);
            if (n == 0)
            {
                if (read == 0 && endAllowedAtStart)
                {
                    return false;
                }
                throw new EndOfStreamException("the broker closed the connection inside a frame");
            }
            read += n;
        }
        return true;
    }

    private void Handle(byte[] frame)
    {
        if (Frames.IsPush(frame))
        {
            Route(Frames.DecodePush(frame));
            return;
        }
        uint id = Frames.ResponseCorrelationId(frame);
        Slot? slot;
        lock (_lock)
        {
            _slots.Remove(id, out slot);
        }
        if (slot is null)
        {
            // Late: the request timed out, or it was sent with SendOnlyAsync.
            return;
        }
        Response response;
        try
        {
            response = Frames.DecodeResponse(frame, slot.Key).Response;
        }
        catch (DecodeException e)
        {
            slot.Source.TrySetException(new ConnectionException("undecodable response: " + e.Message, true, e));
            throw;
        }
        switch (response)
        {
            case Response.Failed failed:
                slot.Source.TrySetException(new BrokerException(failed.Code, null));
                break;
            case Response.Refused refused:
                slot.Source.TrySetException(new BrokerException(refused.Code, refused.Message));
                break;
            default:
                slot.Source.TrySetResult(response);
                break;
        }
    }

    // ---- certificate expiry (ADR 0068) ----

    private async Task WatchCertificateAsync(DateTimeOffset notAfter)
    {
        try
        {
            // A delay is bounded (about 24 days), a certificate may last longer.
            while (true)
            {
                TimeSpan remaining = notAfter - _options.Time.GetUtcNow();
                if (remaining <= TimeSpan.Zero)
                {
                    break;
                }
                TimeSpan step = remaining < TimeSpan.FromDays(7) ? remaining : TimeSpan.FromDays(7);
                await Task.Delay(step, _options.Time, _ended.Token).ConfigureAwait(false);
            }
            Fail(new ConnectionException("the node's certificate expired", true));
        }
        catch (OperationCanceledException)
        {
            // the connection ended first
        }
    }

    // ---- ending ----

    /// <summary>Closes the connection. Every pending request fails with <see cref="ConnectionException"/>.</summary>
    public void Dispose() => Fail(new ConnectionException("connection closed", true));

    /// <inheritdoc cref="Dispose"/>
    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }

    private void Fail(ConnectionException why)
    {
        List<Slot> pending;
        List<IPushListener> listeners;
        lock (_lock)
        {
            if (_broken is not null)
            {
                return;
            }
            _broken = why;
            pending = _slots.Values.ToList();
            _slots.Clear();
            listeners = _subscriptions.Values.ToList();
            _subscriptions.Clear();
            _orphans.Clear();
        }
        _outgoing.Writer.TryComplete();
        try
        {
            _ended.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // already torn down
        }
        _stream.Dispose();
        foreach (Slot slot in pending)
        {
            slot.Source.TrySetException(why);
        }
        foreach (IPushListener listener in listeners)
        {
            try
            {
                listener.OnClose(why);
            }
            catch (Exception)
            {
                // see Deliver
            }
        }
    }

    /// <inheritdoc/>
    public override string ToString() => $"Connection[{Address}{(IsSecure ? ", TLS" : ", plain")}{(IsOpen ? "" : ", closed")}]";
}
