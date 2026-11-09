using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Lotse.Auth;
using Lotse.Proto;
using Xunit;

namespace Lotse.Client.Tests.Auth;

public class ScramClientTests
{
    private static readonly byte[] Salt = Encoding.ASCII.GetBytes("0123456789abcdef");

    private static string Text(byte[] b) => Encoding.UTF8.GetString(b);

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    [Fact]
    public void EveryMessageOfTheBrokersExchangeIsReproducedByteForByte()
    {
        List<string[]> cases = Vectors.Lines("scram.txt");
        Assert.Equal(5, cases.Count);
        foreach (string[] c in cases)
        {
            string label = c[0];
            byte[] user = Vectors.Hex(c[1]);
            byte[] password = c[2].Length == 1 ? Array.Empty<byte>() : Vectors.Hex(c[2].Substring(1));
            byte[] salt = Vectors.Hex(c[3]);
            int iterations = int.Parse(c[4]);
            string clientNonce = c[5];
            string serverNonce = c[6];

            var client = new ScramClient(Text(user), Text(password), clientNonce);
            Assert.True(Vectors.Hex(c[7]).AsSpan().SequenceEqual(client.First()), label + ": client first");
            byte[] clientFinal = client.Respond(Vectors.Hex(c[8]));
            Assert.True(Vectors.Hex(c[9]).AsSpan().SequenceEqual(clientFinal), label + ": client final (the proof)");
            client.Verify(Vectors.Hex(c[10]));

            var credential = ScramCredential.Of(Text(password), salt, iterations);
            Assert.Equal(c[11], Vectors.Hex(credential.StoredKey));
            Assert.Equal(c[12], Vectors.Hex(credential.ServerKey));

            // The test server, written from the RFC, agrees with the broker's bytes too.
            var server = new TestScramServer(_ => credential, serverNonce);
            Assert.True(Vectors.Hex(c[8]).AsSpan().SequenceEqual(server.First(Vectors.Hex(c[7]))), label + ": server first");
            Assert.True(Vectors.Hex(c[10]).AsSpan().SequenceEqual(server.Last(clientFinal).ServerFinal), label + ": server final");
        }
    }

    [Fact]
    public void TheUserNameIsEscaped()
    {
        Assert.Equal("n,,n=a=2Cb=3Dc,r=x", Text(new ScramClient("a,b=c", "pw", "x").First()));
    }

    private static ScramClient AfterFirst(string nonce)
    {
        var c = new ScramClient("alice", "pw", nonce);
        c.First();
        return c;
    }

    private static byte[] ServerFirst(string nonce, long iterations) =>
        Bytes("r=" + nonce + ",s=" + Convert.ToBase64String(Salt) + ",i=" + iterations);

    private static ScramFailure FailureOf(Action action) => Assert.Throws<ScramException>(action).Kind;

    [Fact]
    public void TheServerNonceMustExtendTheClientsStrictly()
    {
        // equal: a server that adds nothing is not contributing a nonce
        Assert.Equal(ScramFailure.Malformed, FailureOf(() => AfterFirst("abc").Respond(ServerFirst("abc", 4096))));
        // does not start with the client's nonce
        Assert.Throws<ScramException>(() => AfterFirst("abc").Respond(ServerFirst("xbcdef", 4096)));
        // shorter
        Assert.Throws<ScramException>(() => AfterFirst("abc").Respond(ServerFirst("ab", 4096)));
        // a nonce with a character outside printable ASCII
        Assert.Throws<ScramException>(() => AfterFirst("abc").Respond(ServerFirst("abc d", 4096)));
        // accepted
        AfterFirst("abc").Respond(ServerFirst("abcd", 4096));
    }

    [Fact]
    public void TheIterationCountIsBounded()
    {
        Assert.Throws<ScramException>(() => AfterFirst("abc").Respond(ServerFirst("abcd", 4095)));
        Assert.Throws<ScramException>(() => AfterFirst("abc").Respond(ServerFirst("abcd", 0)));
        Assert.Throws<ScramException>(() => AfterFirst("abc").Respond(ServerFirst("abcd", (1 << 20) + 1)));
        Assert.Throws<ScramException>(() => AfterFirst("abc").Respond(ServerFirst("abcd", int.MaxValue)));
        Assert.Throws<ScramException>(() => AfterFirst("abc").Respond(Bytes("r=abcd,s=AAAA,i=99999999999")));
        Assert.Throws<ScramException>(() => AfterFirst("abc").Respond(Bytes("r=abcd,s=AAAA,i=-4096")));
        AfterFirst("abc").Respond(ServerFirst("abcd", 4096));
        // The largest count is accepted, and costs a moment of PBKDF2.
        AfterFirst("abc").Respond(ServerFirst("abcd", 1 << 20));
    }

    [Fact]
    public void AMalformedServerFirstMessageIsRefused()
    {
        foreach (string bad in new[] { "", "r=abcd", "r=abcd,s=AAAA", "s=AAAA,r=abcd,i=4096", "r=abcd,s=!!!!,i=4096", "r=abcd,s=AA AA,i=4096", "r=abcd,s=AAAA,i=x" })
        {
            Assert.Equal(ScramFailure.Malformed, FailureOf(() => AfterFirst("abc").Respond(Bytes(bad))));
        }
        Assert.Throws<ScramException>(() => AfterFirst("abc").Respond(new byte[] { 0xff, 0xfe }));
    }

    [Fact]
    public void TheServersSignatureIsChecked()
    {
        var credential = ScramCredential.Of("pw", Salt, 4096);
        foreach (string mode in new[] { "good", "wrong signature", "no v=", "not base64", "empty", "error message" })
        {
            var client = new ScramClient("alice", "pw", "cn");
            var server = new TestScramServer(_ => credential, "sn");
            byte[] first = server.First(client.First());
            byte[] last = client.Respond(first);
            byte[] goodFinal = server.Last(last).ServerFinal;
            byte[] given;
            switch (mode)
            {
                case "good":
                    given = goodFinal;
                    break;
                case "wrong signature":
                    given = (byte[])goodFinal.Clone();
                    given[10] = (byte)(given[10] == 'A' ? 'B' : 'A');
                    break;
                case "no v=":
                    given = Bytes(Text(goodFinal).Substring(2));
                    break;
                case "not base64":
                    given = Bytes("v=%%%%");
                    break;
                case "empty":
                    given = Array.Empty<byte>();
                    break;
                default:
                    given = Bytes("e=invalid-proof");
                    break;
            }
            if (mode == "good")
            {
                client.Verify(given);
            }
            else
            {
                Assert.Equal(ScramFailure.ServerNotVerified, FailureOf(() => client.Verify(given)));
            }
        }
    }

    [Fact]
    public void AServerThatDoesNotKnowTheCredentialCannotProveItself()
    {
        var client = new ScramClient("alice", "pw", "cn");
        var server = new TestScramServer(_ => ScramCredential.Of("another password", Salt, 4096), "sn");
        byte[] last = client.Respond(server.First(client.First()));
        Assert.Throws<TestScramServer.Failed>(() => server.Last(last));
        // A server that skips the check and answers anything is caught by the client.
        Assert.Throws<ScramException>(() => client.Verify(Bytes("v=" + Convert.ToBase64String(new byte[64]))));
    }

    [Fact]
    public void StepsOutOfOrderAreRefused()
    {
        var fresh = new ScramClient("alice", "pw", "cn");
        Assert.Equal(ScramFailure.OutOfOrder, FailureOf(() => fresh.Respond(ServerFirst("cnx", 4096))));
        Assert.Equal(ScramFailure.OutOfOrder, FailureOf(() => fresh.Verify(Bytes("v=AA"))));
        ScramClient client = AfterFirst("cn");
        client.Respond(ServerFirst("cnx", 4096));
        Assert.Throws<ScramException>(() => client.Respond(ServerFirst("cnx", 4096)));
        // A failed step ends the exchange.
        ScramClient failed = AfterFirst("cn");
        Assert.Throws<ScramException>(() => failed.Respond(ServerFirst("cn", 4096)));
        Assert.Throws<ScramException>(() => failed.Respond(ServerFirst("cnx", 4096)));
    }

    [Fact]
    public void TheCredentialForAlterUserHasASaltOfSixteenBytesAndTheMinimumIterations()
    {
        var a = ScramCredential.ForPassword("secret");
        var b = ScramCredential.ForPassword("secret");
        Assert.Equal(16, a.Salt.Length);
        Assert.Equal(4096, a.Iterations);
        Assert.Equal(64, a.StoredKey.Length);
        Assert.Equal(64, a.ServerKey.Length);
        Assert.False(a.Salt.AsSpan().SequenceEqual(b.Salt), "a fresh salt each time");
        Request.AlterUser request = a.AlterUser("bob");
        Assert.Equal("bob", request.User);
        Assert.Equal(4096u, request.Credential!.Iterations);
        Assert.True(a.StoredKey.AsSpan().SequenceEqual(request.Credential.StoredKey.ToByteArray()));
        Assert.Null(ScramCredential.Removal("bob").Credential);
        Assert.Throws<ArgumentOutOfRangeException>(() => ScramCredential.Of("pw", Salt, 4095));
        Assert.Throws<ArgumentOutOfRangeException>(() => ScramCredential.Of("pw", Salt, (1 << 20) + 1));
        Assert.Equal(ScramCredential.Of("pw", Salt, 4096), ScramCredential.Of("pw", Salt, 4096));
    }

    [Fact]
    public void OauthBearerSendsTheTokenInTheRfc7628Form()
    {
        Assert.Equal("n,,\u0001auth=Bearer abc.def\u0001\u0001", Text(Credentials.OAuthBearerLogin.InitialResponse("abc.def")));
    }

    [Fact]
    public void CredentialsDoNotShowTheirSecrets()
    {
        Assert.Equal("Scram[user=alice]", Credentials.Scram("alice", "hunter2").ToString());
        Assert.DoesNotContain("secret", Credentials.OAuthBearer(() => "secret-token").ToString());
        Assert.Throws<ArgumentException>(() => Credentials.Scram("", "pw"));
    }

    [Fact]
    public void TheNonceIs24RandomBytesInHex()
    {
        string nonce = Authenticator.Nonce(b => Array.Fill(b, (byte)0xab));
        Assert.Equal(48, nonce.Length);
        Assert.Equal(string.Concat(Enumerable.Repeat("ab", 24)), nonce);
    }
}
