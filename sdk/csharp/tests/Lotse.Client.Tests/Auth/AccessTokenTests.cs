using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Lotse.Auth;
using Lotse.Proto;
using Lotse.Record;
using Xunit;

namespace Lotse.Client.Tests.Auth;

/// <summary>An access token's text and hash (ADR 0133), checked against texts and hashes computed outside this code.</summary>
public class AccessTokenTests
{
    private const ulong SampleId = 0x0123_4567_89ab_cdefUL;

    // Written by Python's base64.urlsafe_b64encode and hashlib.sha256 for the sample secret, not by this code.
    private const string SampleText = "lotse_tok_0123456789abcdef_CzBVep_E6Q4zWH2ix-wRNluApcrvFDleg6jN8hc8YYY";
    private const string SampleHash = "83b7a8ed859053c81d818870fab1f8b1ae44d06a98a9665d369a8fd7d2838ded";

    /// <summary>The secret of <c>lotse-auth</c>'s own token tests: byte <c>i</c> is <c>i * 37 + 11</c>.</summary>
    private static byte[] SampleSecret()
    {
        byte[] s = new byte[32];
        for (int i = 0; i < 32; i++)
        {
            s[i] = unchecked((byte)(i * 37 + 11));
        }
        return s;
    }

    private static AccessToken Sample() => new(SampleId, SampleSecret());

    private static AccessToken? Parsed(string text) => AccessToken.TryParse(text, out AccessToken? t) ? t : null;

    [Fact]
    public void TokenTextRoundTripsAndTrims()
    {
        AccessToken t = Sample();
        Assert.Equal(SampleText, t.Text());
        Assert.Equal(t, Parsed(SampleText));
        // A pasted token often carries a newline; Rust's trim_ascii takes space, tab, LF, FF and CR.
        Assert.Equal(t, Parsed("  " + SampleText + "\n"));
        Assert.Equal(t, Parsed("\t\r\n\f" + SampleText + " \r\n"));
        Assert.Equal(t, AccessToken.Parse(SampleText + "\r\n"));
        // Not whitespace to the node: a vertical tab or a no-break space stays and spoils the text.
        Assert.Null(Parsed("\u000b" + SampleText));
        Assert.Null(Parsed(SampleText + " "));
        Assert.Equal(SampleSecret(), Parsed(SampleText)!.Secret());
        Assert.Equal(SampleId, Parsed(SampleText)!.Id);
    }

    [Fact]
    public void TheHighestIdAndAnyRandomSecretRoundTrip()
    {
        foreach (ulong id in new ulong[] { 0, 1, 0x2a, long.MaxValue, ulong.MaxValue })
        {
            byte[] secret = RandomNumberGenerator.GetBytes(32);
            var t = new AccessToken(id, secret);
            string text = t.Text();
            Assert.Equal(10 + 16 + 1 + 43, text.Length);
            Assert.Equal(t, Parsed(text));
        }
        Assert.StartsWith("lotse_tok_ffffffffffffffff_", new AccessToken(ulong.MaxValue, new byte[32]).Text());
        Assert.StartsWith("lotse_tok_0000000000000000_", new AccessToken(0, new byte[32]).Text());
    }

    [Fact]
    public void MalformedTextsDoNotParse()
    {
        string secret = SampleText.Substring(SampleText.Length - 43);
        string[] bad =
        {
            SampleText.Replace("lotse_tok_", "lotse_key_"),
            "lotse_tok_0123456789abcde_" + secret,
            "lotse_tok_0123456789abcdef_" + secret.Substring(0, 42),
            "lotse_tok_0123456789abcdef_" + secret.Substring(0, 42) + "!",
            // 'Z' ends in non-zero spare bits: not the canonical text of any secret.
            "lotse_tok_0123456789abcdef_" + secret.Substring(0, 42) + "Z",
            "lotse_tok_0123456789abcdef_" + secret.Substring(0, 42) + "=",
            "lotse_tok_0123456789ABCDEF_" + secret,
            "lotse_tok_+123456789abcdef_" + secret,
            "lotse_tok_0123456789abcdef_" + secret + "x",
            "lotse_tok_0123456789abcdef" + secret,
            "LOTSE_TOK_0123456789abcdef_" + secret,
            string.Empty,
        };
        foreach (string b in bad)
        {
            Assert.False(AccessToken.TryParse(b, out AccessToken? t), b);
            Assert.Null(t);
            Assert.Throws<FormatException>(() => AccessToken.Parse(b));
        }
        Assert.False(AccessToken.TryParse(null, out _));
    }

    [Fact]
    public void TheHashIsTheSha256OfTheSecretBytes()
    {
        AccessToken t = Sample();
        Assert.Equal(SampleHash, Vectors.Hex(t.Hash()));
        Assert.NotEqual(t.Secret(), t.Hash());
        Assert.Throws<ArgumentException>(() => new AccessToken(1, new byte[31]));
    }

    [Fact]
    public void TheSecretIsNotPrinted()
    {
        AccessToken t = Sample();
        string secret = SampleText.Substring(SampleText.Length - 43);
        Assert.DoesNotContain(secret, t.ToString());
        Assert.Contains("0123456789abcdef", t.ToString());
        string shown = Credentials.Token(SampleText).ToString();
        Assert.DoesNotContain(secret, shown);
        Assert.DoesNotContain("CzBVep", shown);
    }

    [Fact]
    public void LifetimesAreTheFiveSteps()
    {
        int[] days = { 7, 30, 90, 180, 365 };
        for (byte code = 1; code <= 5; code++)
        {
            TokenLifetime l = TokenLifetimes.FromCode(code);
            Assert.Equal(code, (byte)l);
            Assert.Equal(TimeSpan.FromDays(days[code - 1]), l.Duration());
        }
        Assert.Equal(TokenLifetime.HalfYear, TokenLifetimes.FromCode(4));
        Assert.Throws<ArgumentOutOfRangeException>(() => TokenLifetimes.FromCode(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => TokenLifetimes.FromCode(6));
        Assert.Throws<ArgumentOutOfRangeException>(() => ((TokenLifetime)6).Duration());
    }

    /// <summary><c>LOTSE-TOKEN</c> is one round whose message is the token's text, as the client was given it.</summary>
    [Fact]
    public async Task ATokenSignsInWithItsTextAsTheOneMessage()
    {
        var sent = new List<Request>();
        ulong lifetime = await Authenticator.AuthenticateAsync(
            r =>
            {
                sent.Add(r);
                Response answer = r is Request.SaslHandshake
                    ? new Response.SaslHandshake(ErrorCode.None, new[] { "LOTSE-TOKEN" })
                    : new Response.SaslAuthenticate(ErrorCode.None, null, ByteString.Empty, 60_000);
                return Task.FromResult(answer);
            },
            Credentials.Token(SampleText),
            b => throw new InvalidOperationException("a token sign-in draws no randomness"));
        Assert.Equal(60_000UL, lifetime);
        Assert.Equal(2, sent.Count);
        Assert.Equal(new Request.SaslHandshake("LOTSE-TOKEN"), sent[0]);
        var auth = Assert.IsType<Request.SaslAuthenticate>(sent[1]);
        Assert.Equal(SampleText, Encoding.UTF8.GetString(auth.AuthBytes.ToByteArray()));
    }

    [Fact]
    public async Task ARefusedTokenFailsTheLogin()
    {
        var e = await Assert.ThrowsAsync<AuthenticationException>(() => Authenticator.AuthenticateAsync(
            r => Task.FromResult<Response>(
                r is Request.SaslHandshake
                    ? new Response.SaslHandshake(ErrorCode.None, new[] { "LOTSE-TOKEN" })
                    : new Response.SaslAuthenticate(ErrorCode.AuthenticationFailed, "authentication failed", ByteString.Empty, 0)),
            Credentials.Token(SampleText),
            b => RandomNumberGenerator.Fill(b)));
        Assert.Equal("authentication failed", e.Message);
    }
}
