using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Lotse.Proto;
using Lotse.Record;

namespace Lotse.Auth;

/// <summary>
/// Runs the SASL exchange of a connection: <c>SaslHandshake</c> with the mechanism, then <c>SaslAuthenticate</c>
/// rounds. Calling it again on a connection that is already authenticated renews the session; the node then insists
/// on the same principal.
/// </summary>
public static class Authenticator
{
    /// <summary>
    /// Authenticates over <paramref name="exchange"/>, which sends one request and returns its response, or throws if
    /// there is none.
    /// </summary>
    /// <param name="exchange">Sends a request and waits for the answer; may throw whatever the transport throws.</param>
    /// <param name="credentials">Who to log in as.</param>
    /// <param name="random">Fills a buffer with random bytes: the source of the SCRAM client nonce.</param>
    /// <param name="cancellationToken">Cancels the exchange between its steps.</param>
    /// <returns>
    /// The session's lifetime in milliseconds as the node granted it with its last answer; <c>0</c> if the session
    /// lasts as long as the connection. Take it from the last response: a renewal can grant another lifetime than the
    /// first login.
    /// </returns>
    /// <exception cref="AuthenticationException">
    /// The node refuses, does not offer the mechanism (the message names the mechanisms it offers), or a SCRAM server
    /// fails the checks of <see cref="ScramClient"/>.
    /// </exception>
    public static async Task<ulong> AuthenticateAsync(
        Func<Request, Task<Response>> exchange,
        Credentials credentials,
        Action<byte[]> random,
        CancellationToken cancellationToken = default)
    {
        string mechanism = credentials.Mechanism;
        Response handshake = await exchange(new Request.SaslHandshake(mechanism)).ConfigureAwait(false);
        if (handshake is not Response.SaslHandshake h)
        {
            throw Unexpected(handshake);
        }
        if (!h.Error.IsOk)
        {
            throw new AuthenticationException(
                mechanism + " is not enabled on this node; it offers "
                + (h.Mechanisms.Count == 0 ? "none" : string.Join(", ", h.Mechanisms)),
                h.Error);
        }
        cancellationToken.ThrowIfCancellationRequested();
        if (credentials is Credentials.ScramLogin scram)
        {
            var client = new ScramClient(scram.User, scram.Password, Nonce(random));
            Response.SaslAuthenticate first = await StepAsync(exchange, client.First()).ConfigureAwait(false);
            byte[] last = client.Respond(first.AuthBytes.ToByteArray());
            Response.SaslAuthenticate final = await StepAsync(exchange, last).ConfigureAwait(false);
            client.Verify(final.AuthBytes.ToByteArray());
            return final.SessionLifetimeMs;
        }
        var bearer = (Credentials.OAuthBearerLogin)credentials;
        string token = await bearer.Token(cancellationToken).ConfigureAwait(false);
        Response.SaslAuthenticate done =
            await StepAsync(exchange, Credentials.OAuthBearerLogin.InitialResponse(token)).ConfigureAwait(false);
        return done.SessionLifetimeMs;
    }

    private static async Task<Response.SaslAuthenticate> StepAsync(Func<Request, Task<Response>> exchange, byte[] bytes)
    {
        Response r = await exchange(new Request.SaslAuthenticate(ByteString.CopyOf(bytes))).ConfigureAwait(false);
        if (r is not Response.SaslAuthenticate a)
        {
            throw Unexpected(r);
        }
        if (!a.Error.IsOk)
        {
            throw new AuthenticationException(a.ErrorMsg ?? a.Error.ToString(), a.Error);
        }
        return a;
    }

    private static AuthenticationException Unexpected(Response r) =>
        new("unexpected response " + r.GetType().Name + " during authentication", null);

    /// <summary>A fresh client nonce: 24 random bytes in hex.</summary>
    internal static string Nonce(Action<byte[]> random)
    {
        byte[] b = new byte[24];
        random(b);
        var s = new StringBuilder(48);
        foreach (byte x in b)
        {
            s.Append("0123456789abcdef"[x >> 4]).Append("0123456789abcdef"[x & 0xF]);
        }
        return s.ToString();
    }
}
