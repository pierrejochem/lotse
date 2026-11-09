using System;
using System.Linq;
using Lotse.Auth;
using Lotse.Client;
using Lotse.Net;

namespace Lotse.Examples;

/// <summary>
/// The examples' connection settings, from the environment:
/// <list type="bullet">
/// <item><c>LOTSE_BOOTSTRAP</c>: brokers, <c>host:port</c> separated by commas (default <c>127.0.0.1:9092</c>);</item>
/// <item><c>LOTSE_CA</c>, and optionally <c>LOTSE_CERT</c> and <c>LOTSE_KEY</c>: PEM files for TLS. Without
/// <c>LOTSE_CA</c> the examples use plain TCP, which the client allows on loopback only;</item>
/// <item><c>LOTSE_USER</c> and <c>LOTSE_PASSWORD</c>: a SCRAM-SHA-512 login, if the cluster asks for one.</item>
/// </list>
/// </summary>
internal static class Env
{
    public static ClientConfig.Builder Config()
    {
        string bootstrap = Environment.GetEnvironmentVariable("LOTSE_BOOTSTRAP") ?? "127.0.0.1:9092";
        ClientConfig.Builder builder = ClientConfig.CreateBuilder(bootstrap.Split(',').Select(a => a.Trim()));
        string? ca = Environment.GetEnvironmentVariable("LOTSE_CA");
        string? cert = Environment.GetEnvironmentVariable("LOTSE_CERT");
        if (ca is null)
        {
            builder.PlaintextOnLoopback();
        }
        else if (cert is not null)
        {
            string key = Environment.GetEnvironmentVariable("LOTSE_KEY")
                ?? throw new InvalidOperationException("LOTSE_CERT needs LOTSE_KEY, the certificate's private key");
            builder.Tls(TlsConfig.FromFiles(ca, cert, key));
        }
        else
        {
            builder.Tls(TlsConfig.FromFiles(ca));
        }
        string? user = Environment.GetEnvironmentVariable("LOTSE_USER");
        if (user is not null)
        {
            string password = Environment.GetEnvironmentVariable("LOTSE_PASSWORD")
                ?? throw new InvalidOperationException("LOTSE_USER needs LOTSE_PASSWORD");
            builder.WithCredentials(Credentials.Scram(user, password));
        }
        return builder;
    }
}
