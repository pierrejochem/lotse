using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace Lotse.Client.Tests.Net;

/// <summary>A throwaway CA and certificates made by the real <c>lotse certs</c>, for a one-node cluster file.</summary>
internal sealed class Certs
{
    internal string Dir { get; }

    internal string Config { get; }

    internal string Ca { get; }

    internal string NodeCert { get; }

    internal string NodeKey { get; }

    internal string ClientCert { get; }

    internal string ClientKey { get; }

    internal int PeerPort { get; }

    internal int ClientPort { get; }

    private Certs(string dir, int peerPort, int clientPort)
    {
        Dir = dir;
        PeerPort = peerPort;
        ClientPort = clientPort;
        Config = Path.Combine(dir, "cluster.toml");
        Ca = Path.Combine(dir, "tls", "ca.pem");
        NodeCert = Path.Combine(dir, "tls", "node-1.pem");
        NodeKey = Path.Combine(dir, "tls", "node-1.key");
        ClientCert = Path.Combine(dir, "clients", "alice.pem");
        ClientKey = Path.Combine(dir, "clients", "alice.key");
    }

    /// <param name="bin">The <c>lotse</c> executable.</param>
    /// <param name="extra">More lines for the <c>[auth]</c> section, such as <c>max_session_s = 3</c>.</param>
    internal static async Task<Certs> CreateAsync(string bin, string extra)
    {
        string dir = Directory.CreateTempSubdirectory("lotse-sdk-certs").FullName;
        var certs = new Certs(dir, FreePort(), FreePort());
        await File.WriteAllTextAsync(
            certs.Config,
            $"[cluster]\ndata_dir = \"{dir}/data/node-{{id}}\"\n\n"
            + $"[tls]\nca = \"{dir}/tls/ca.pem\"\ncert = \"{dir}/tls/node-{{id}}.pem\"\n"
            + $"key = \"{dir}/tls/node-{{id}}.key\"\n\n[retention]\nms = -1\n\n"
            + $"[auth]\nadmins = [\"alice\"]\n{extra}\n[[node]]\nid = 1\npeer = \"127.0.0.1:{certs.PeerPort}\"\nclient = \"127.0.0.1:{certs.ClientPort}\"\n");
        await LotseBinary.RunAsync(bin, "certs", "--config", certs.Config);
        await LotseBinary.RunAsync(bin, "certs", "--config", certs.Config, "--client", "alice", "--out", Path.Combine(dir, "clients"));
        return certs;
    }

    /// <summary>
    /// A port nothing listens on, chosen below the ephemeral range (32768 and up on Linux): a port the kernel handed out
    /// for listening is handed out again to the next outgoing connection, which could take it before the node starts.
    /// </summary>
    private static readonly System.Collections.Generic.HashSet<int> HandedOut = new();

    internal static int FreePort()
    {
        while (true)
        {
            int candidate = 20_000 + Random.Shared.Next(12_000);
            // Not one this process handed out already: those are free only until their node binds them.
            lock (HandedOut)
            {
                if (!HandedOut.Add(candidate))
                {
                    continue;
                }
            }
            var l = new TcpListener(IPAddress.Loopback, candidate);
            try
            {
                l.Start();
                return ((IPEndPoint)l.LocalEndpoint).Port;
            }
            catch (SocketException)
            {
                // taken: try another
            }
            finally
            {
                l.Stop();
            }
        }
    }
}
