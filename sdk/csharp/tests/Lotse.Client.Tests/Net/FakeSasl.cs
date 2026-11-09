using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Lotse.Auth;
using Lotse.Client.Tests.Auth;
using Lotse.Proto;
using Lotse.Record;

namespace Lotse.Client.Tests.Net;

/// <summary>The authentication half of a fake broker: SCRAM against a user table, or OAUTHBEARER accepting any token.</summary>
internal sealed class FakeSasl
{
    internal static readonly ScramCredential Alice = ScramCredential.Of("pw", Encoding.ASCII.GetBytes("0123456789abcdef"), 4096);

    private readonly Dictionary<string, ScramCredential> _users = new() { ["alice"] = Alice };
    private readonly ConcurrentDictionary<FakeBroker.Peer, TestScramServer> _servers = new();
    private readonly ConcurrentDictionary<FakeBroker.Peer, string> _chosen = new();
    private int _handshakes;
    private int _finals;

    internal FakeSasl(ulong lifetimeMs)
    {
        LifetimeMs = lifetimeMs;
    }

    internal ulong LifetimeMs;

    internal int Handshakes => Volatile.Read(ref _handshakes);

    internal int Finals => Volatile.Read(ref _finals);

    internal ConcurrentQueue<string> Tokens { get; } = new();

    /// <summary>Called with the number of handshakes so far, when a handshake arrives.</summary>
    internal volatile Action<int> OnHandshake = _ => { };

    /// <summary>Mechanisms the broker offers.</summary>
    internal volatile IReadOnlyList<string> Mechanisms = new[] { Credentials.ScramLogin.MechanismName, Credentials.OAuthBearerLogin.MechanismName };

    /// <summary>Handles SASL requests; returns false for any other.</summary>
    internal bool Handle(FakeBroker.Peer peer, Frames.DecodedRequest decoded)
    {
        uint id = decoded.Header.CorrelationId;
        if (decoded.Request is Request.SaslHandshake h)
        {
            OnHandshake(Interlocked.Increment(ref _handshakes));
            if (Mechanisms.Contains(h.Mechanism))
            {
                _chosen[peer] = h.Mechanism;
                peer.Reply(id, new Response.SaslHandshake(ErrorCode.None, Mechanisms));
            }
            else
            {
                peer.Reply(id, new Response.SaslHandshake(ErrorCode.UnsupportedSaslMechanism, Mechanisms));
            }
            return true;
        }
        if (decoded.Request is Request.SaslAuthenticate a)
        {
            byte[] bytes = a.AuthBytes.ToByteArray();
            if (_chosen.TryGetValue(peer, out string? mechanism) && mechanism == Credentials.ScramLogin.MechanismName)
            {
                Scram(peer, id, bytes);
            }
            else
            {
                string message = Encoding.UTF8.GetString(bytes);
                int start = "n,,\u0001auth=Bearer ".Length;
                Tokens.Enqueue(message.Substring(start, message.Length - 2 - start));
                Interlocked.Increment(ref _finals);
                peer.Reply(id, new Response.SaslAuthenticate(ErrorCode.None, null, ByteString.Empty, LifetimeMs));
            }
            return true;
        }
        return false;
    }

    private void Scram(FakeBroker.Peer peer, uint id, byte[] bytes)
    {
        try
        {
            if (!_servers.TryGetValue(peer, out TestScramServer? server))
            {
                server = new TestScramServer(u => _users.GetValueOrDefault(u), "servernonce");
                _servers[peer] = server;
                byte[] first = server.First(bytes);
                peer.Reply(id, new Response.SaslAuthenticate(ErrorCode.None, null, ByteString.CopyOf(first), 0));
            }
            else
            {
                _servers.TryRemove(peer, out _);
                TestScramServer.Result r = server.Last(bytes);
                Interlocked.Increment(ref _finals);
                peer.Reply(id, new Response.SaslAuthenticate(ErrorCode.None, null, ByteString.CopyOf(r.ServerFinal), LifetimeMs));
            }
        }
        catch (TestScramServer.Failed)
        {
            _servers.TryRemove(peer, out _);
            peer.Reply(id, new Response.SaslAuthenticate(ErrorCode.AuthenticationFailed, "authentication failed", ByteString.Empty, 0));
            peer.CloseSocket();
        }
    }
}
