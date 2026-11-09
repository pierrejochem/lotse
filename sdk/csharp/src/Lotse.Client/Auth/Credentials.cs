using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Lotse.Auth;

/// <summary>
/// How a client proves who it is to a node. A client certificate needs no entry here: it is part of the TLS
/// configuration (<see cref="Lotse.Net.TlsConfig"/>).
/// </summary>
public abstract record Credentials
{
    private protected Credentials()
    {
    }

    /// <summary>The mechanism's name in <c>SaslHandshake</c>.</summary>
    public abstract string Mechanism { get; }

    /// <summary>SASL SCRAM-SHA-512 with a user name and a password. The password never leaves the client.</summary>
    public static Credentials Scram(string user, string password) => new ScramLogin(user, password);

    /// <summary>
    /// SASL OAUTHBEARER. <paramref name="token"/> is called for <em>every</em> authentication, on connecting and
    /// before each session ends, and returns the current access token; keep it cheap and let it refresh the token when
    /// needed.
    /// </summary>
    public static Credentials OAuthBearer(Func<string> token)
    {
        ArgumentNullException.ThrowIfNull(token);
        return new OAuthBearerLogin(_ => Task.FromResult(token()));
    }

    /// <summary>Like <see cref="OAuthBearer(Func{string})"/>, for a token source that is asynchronous.</summary>
    public static Credentials OAuthBearerAsync(Func<CancellationToken, Task<string>> token) => new OAuthBearerLogin(token);

    /// <summary>See <see cref="Scram"/>.</summary>
    public sealed record ScramLogin : Credentials
    {
        /// <summary>The mechanism's name.</summary>
        public const string MechanismName = "SCRAM-SHA-512";

        /// <summary>Creates the credentials.</summary>
        public ScramLogin(string user, string password)
        {
            ArgumentNullException.ThrowIfNull(user);
            ArgumentNullException.ThrowIfNull(password);
            if (user.Length == 0)
            {
                throw new ArgumentException("the user name is empty", nameof(user));
            }
            User = user;
            Password = password;
        }

        /// <summary>The user.</summary>
        public string User { get; }

        /// <summary>The password; <see cref="ToString"/> does not show it.</summary>
        public string Password { get; }

        /// <inheritdoc/>
        public override string Mechanism => MechanismName;

        /// <inheritdoc/>
        public override string ToString() => $"Scram[user={User}]";
    }

    /// <summary>See <see cref="OAuthBearer(Func{string})"/>.</summary>
    public sealed record OAuthBearerLogin : Credentials
    {
        /// <summary>The mechanism's name.</summary>
        public const string MechanismName = "OAUTHBEARER";

        /// <summary>Creates the credentials.</summary>
        public OAuthBearerLogin(Func<CancellationToken, Task<string>> token)
        {
            ArgumentNullException.ThrowIfNull(token);
            Token = token;
        }

        /// <summary>Returns the current access token.</summary>
        public Func<CancellationToken, Task<string>> Token { get; }

        /// <inheritdoc/>
        public override string Mechanism => MechanismName;

        /// <summary>The initial client response for <paramref name="token"/>: <c>n,,\x01auth=Bearer &lt;token&gt;\x01\x01</c> (RFC 7628).</summary>
        public static byte[] InitialResponse(string token) =>
            Encoding.UTF8.GetBytes("n,,\u0001auth=Bearer " + token + "\u0001\u0001");

        /// <inheritdoc/>
        public override string ToString() => "OAuthBearer";
    }
}
