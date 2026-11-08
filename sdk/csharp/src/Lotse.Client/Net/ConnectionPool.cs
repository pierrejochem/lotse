using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Lotse.Auth;

namespace Lotse.Net;

/// <summary>
/// One connection per broker address, authenticated, kept for reuse.
/// <para>
/// <see cref="GetAsync"/> returns the live connection to an address, opening it on first use. A connection whose
/// session is due for renewal (four fifths of its lifetime) authenticates again on the same connection, as the same
/// principal; one caller renews while the others go on with the session, which still holds. A connection whose session
/// has ended, or that broke, is replaced by a new one with a fresh login. A connection that failed once should be
/// handed to <see cref="Discard"/>: it is not trusted again.
/// </para>
/// <para>Thread-safe.</para>
/// </summary>
public sealed class ConnectionPool : IDisposable
{
    private readonly ConnectionOptions _options;
    private readonly ConcurrentDictionary<Address, Connection> _connections = new();
    private readonly ConcurrentDictionary<Connection, bool> _renewing = new();
    private volatile bool _closed;

    /// <summary>Creates a pool whose connections are made with <paramref name="options"/>.</summary>
    public ConnectionPool(ConnectionOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <summary>The connection to <paramref name="address"/>; see the class description for when it is opened, renewed or replaced.</summary>
    public async Task<Connection> GetAsync(Address address, CancellationToken cancellationToken = default)
    {
        if (_closed)
        {
            throw new ConnectionException("the pool is closed", false);
        }
        if (_connections.TryGetValue(address, out Connection? cached))
        {
            if (!cached.IsOpen)
            {
                Discard(cached);
            }
            else
            {
                switch (cached.SessionState)
                {
                    case SessionState.Unlimited:
                    case SessionState.Active:
                        return cached;
                    case SessionState.Ended:
                        Discard(cached); // requests would be refused: start over
                        break;
                    case SessionState.RenewDue:
                        if (!_renewing.TryAdd(cached, true))
                        {
                            return cached;
                        }
                        try
                        {
                            await cached.AuthenticateAsync(_options.Credentials!, cancellationToken).ConfigureAwait(false);
                            return cached;
                        }
                        catch (Exception e) when (e is not OperationCanceledException)
                        {
                            Discard(cached);
                            if (e is AuthenticationException)
                            {
                                throw;
                            }
                            // a failure of the connection itself: log in on a new one below
                        }
                        finally
                        {
                            _renewing.TryRemove(cached, out _);
                        }
                        break;
                }
            }
        }
        Connection fresh = await Connection.OpenAsync(address, _options, cancellationToken).ConfigureAwait(false);
        if (!_connections.TryAdd(address, fresh) && _connections.TryGetValue(address, out Connection? winner) && winner != fresh)
        {
            fresh.Dispose();
            return winner;
        }
        return fresh;
    }

    /// <summary>Forgets and closes <paramref name="connection"/>, if the pool still holds it.</summary>
    public void Discard(Connection connection)
    {
        _connections.TryRemove(new System.Collections.Generic.KeyValuePair<Address, Connection>(connection.Address, connection));
        _renewing.TryRemove(connection, out _);
        connection.Dispose();
    }

    /// <summary>Closes every connection.</summary>
    public void Dispose()
    {
        _closed = true;
        foreach (Connection c in _connections.Values)
        {
            c.Dispose();
        }
        _connections.Clear();
        _renewing.Clear();
    }
}
