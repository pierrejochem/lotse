using System;
using System.Collections.Concurrent;
using System.IO;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using Lotse.Proto;

namespace Lotse.Client.Tests.Net;

/// <summary>A TLS server in this process that answers <c>ApiVersions</c>.</summary>
internal sealed class TlsTestServer : IDisposable
{
    private readonly TcpListener _listener;
    private readonly X509Certificate2 _certificate;
    private readonly SslProtocols _protocols;
    private readonly X509Certificate2Collection? _clientAuthorities;
    private readonly ConcurrentBag<Socket> _sockets = new();

    /// <summary>Subjects of the client certificates the server saw.</summary>
    internal ConcurrentQueue<string> ClientSubjects { get; } = new();

    internal ConcurrentQueue<string> HandshakeFailures { get; } = new();

    /// <param name="certificate">The server's certificate with its private key.</param>
    /// <param name="protocols">The TLS versions the server offers.</param>
    /// <param name="clientAuthorities">When set, a client certificate is required, checked against these CAs.</param>
    internal TlsTestServer(X509Certificate2 certificate, SslProtocols protocols, X509Certificate2Collection? clientAuthorities)
    {
        _certificate = certificate;
        _protocols = protocols;
        _clientAuthorities = clientAuthorities;
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
        _ = Task.Run(AcceptLoop);
    }

    /// <summary>The node certificate and key that <c>lotse certs</c> wrote.</summary>
    internal TlsTestServer(Certs certs, SslProtocols protocols, bool clientAuth)
        : this(
            X509Certificate2.CreateFromPemFile(certs.NodeCert, certs.NodeKey),
            protocols,
            clientAuth ? ReadCa(certs) : null)
    {
    }

    private static X509Certificate2Collection ReadCa(Certs certs)
    {
        var c = new X509Certificate2Collection();
        c.ImportFromPemFile(certs.Ca);
        return c;
    }

    internal int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

    private async Task AcceptLoop()
    {
        while (true)
        {
            Socket s;
            try
            {
                s = await _listener.AcceptSocketAsync();
            }
            catch (Exception)
            {
                return;
            }
            _sockets.Add(s);
            _ = Task.Run(() => Serve(s));
        }
    }

    private bool ValidClient(X509Certificate? certificate, X509Chain? chain, SslPolicyErrors errors)
    {
        if (certificate is null)
        {
            return false;
        }
        using var own = new X509Chain();
        own.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        own.ChainPolicy.CustomTrustStore.AddRange(_clientAuthorities!);
        own.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        return own.Build(new X509Certificate2(certificate));
    }

    private async Task Serve(Socket socket)
    {
        try
        {
            using var ssl = new SslStream(new NetworkStream(socket, ownsSocket: true), false);
            var options = new SslServerAuthenticationOptions
            {
                ServerCertificate = _certificate,
                EnabledSslProtocols = _protocols,
                ClientCertificateRequired = _clientAuthorities is not null,
                RemoteCertificateValidationCallback = _clientAuthorities is null ? null : (_, cert, chain, errors) => ValidClient(cert, chain, errors),
            };
            await ssl.AuthenticateAsServerAsync(options);
            ClientSubjects.Enqueue(ssl.RemoteCertificate is null ? "none" : ((X509Certificate2)ssl.RemoteCertificate).GetNameInfo(X509NameType.SimpleName, false));
            byte[] prefix = new byte[4];
            while (true)
            {
                await ssl.ReadExactlyAsync(prefix);
                byte[] frame = new byte[Frames.ReadLength(prefix)];
                await ssl.ReadExactlyAsync(frame);
                Frames.DecodedRequest r = Frames.DecodeRequest(frame);
                if (r.Request is Request.ApiVersions)
                {
                    byte[] reply = Frames.WithLengthPrefix(Frames.EncodeResponse(
                        r.Header.CorrelationId, new Response.ApiVersions(Array.Empty<ApiVersionRange>(), new[] { "tls-test" })));
                    await ssl.WriteAsync(reply);
                    await ssl.FlushAsync();
                }
            }
        }
        catch (Exception e)
        {
            HandshakeFailures.Enqueue(e.Message);
        }
    }

    public void Dispose()
    {
        _listener.Stop();
        foreach (Socket s in _sockets)
        {
            try
            {
                s.Close();
            }
            catch (Exception)
            {
            }
        }
    }
}
