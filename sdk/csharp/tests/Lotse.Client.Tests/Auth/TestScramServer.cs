using System;
using System.Security.Cryptography;
using System.Text;
using Lotse.Auth;

namespace Lotse.Client.Tests.Auth;

/// <summary>
/// The broker's end of a SCRAM-SHA-512 exchange, written for tests from RFC 5802 and checked against the Rust
/// implementation through the vectors. It lets the fake broker of the connection tests accept or refuse a login.
/// </summary>
internal sealed class TestScramServer
{
    /// <summary>The login failed; the real broker answers <c>AUTHENTICATION_FAILED</c> and closes the connection.</summary>
    internal sealed class Failed : Exception
    {
        public Failed(string why)
            : base(why)
        {
        }
    }

    internal sealed record Result(string User, byte[] ServerFinal);

    private readonly Func<string, ScramCredential?> _lookup;
    private readonly string _serverNonce;
    private string _user = "";
    private ScramCredential _credential = null!;
    private string _nonce = "";
    private string _authPrefix = "";

    public TestScramServer(Func<string, ScramCredential?> lookup, string serverNonce)
    {
        _lookup = lookup;
        _serverNonce = serverNonce;
    }

    private static string Unescape(string name) => name.Replace("=2C", ",").Replace("=3D", "=");

    public byte[] First(byte[] message)
    {
        string msg = Encoding.UTF8.GetString(message);
        if (!msg.StartsWith("n,,", StringComparison.Ordinal))
        {
            throw new Failed("only n,, is supported");
        }
        string bare = msg.Substring(3);
        string[] parts = bare.Split(',');
        _user = Unescape(parts[0].Substring(2));
        string clientNonce = parts[1].Substring(2);
        _credential = _lookup(_user) ?? throw new Failed("unknown user");
        _nonce = clientNonce + _serverNonce;
        string serverFirst = "r=" + _nonce + ",s=" + Convert.ToBase64String(_credential.Salt) + ",i=" + _credential.Iterations;
        _authPrefix = bare + "," + serverFirst;
        return Encoding.UTF8.GetBytes(serverFirst);
    }

    public Result Last(byte[] message)
    {
        string msg = Encoding.UTF8.GetString(message);
        int at = msg.LastIndexOf(",p=", StringComparison.Ordinal);
        string withoutProof = msg.Substring(0, at);
        byte[] proof = Convert.FromBase64String(msg.Substring(at + 3));
        if (withoutProof != "c=biws,r=" + _nonce)
        {
            throw new Failed("channel binding or nonce does not match");
        }
        byte[] authMessage = Encoding.UTF8.GetBytes(_authPrefix + "," + withoutProof);
        byte[] clientSignature = HMACSHA512.HashData(_credential.StoredKey, authMessage);
        byte[] clientKey = new byte[proof.Length];
        for (int i = 0; i < clientKey.Length; i++)
        {
            clientKey[i] = (byte)(proof[i] ^ clientSignature[i]);
        }
        if (!CryptographicOperations.FixedTimeEquals(SHA512.HashData(clientKey), _credential.StoredKey))
        {
            throw new Failed("wrong password");
        }
        byte[] serverSignature = HMACSHA512.HashData(_credential.ServerKey, authMessage);
        return new Result(_user, Encoding.UTF8.GetBytes("v=" + Convert.ToBase64String(serverSignature)));
    }
}
