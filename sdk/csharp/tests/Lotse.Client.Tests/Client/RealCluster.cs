using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using Lotse.Net;
using Xunit;

namespace Lotse.Client.Tests.Client;

/// <summary>
/// Real <c>lotse</c> processes started from a generated cluster file: plain TCP on loopback with no authentication.
/// Skipped, with a message, when <c>LOTSE_BIN</c> does not name an executable.
/// </summary>
public sealed class RealCluster : IAsyncLifetime
{
    private const int Nodes = 3;

    private readonly Process?[] _processes = new Process?[Nodes + 1];
    private string _bin = string.Empty;

    internal string Dir { get; private set; } = string.Empty;

    internal string Config { get; private set; } = string.Empty;

    internal List<Address> ClientAddresses { get; } = new();

    internal string[] Bootstrap => ClientAddresses.Select(a => a.ToString()).ToArray();

    internal Address AddressOf(int node) => ClientAddresses[node - 1];

    internal bool Available { get; private set; }

    public async Task InitializeAsync()
    {
        string? bin = LotseBinary.Find();
        if (bin is null)
        {
            return;
        }
        Available = true;
        _bin = bin;
        Dir = Directory.CreateTempSubdirectory("lotse-sdk-cluster").FullName;
        Config = Path.Combine(Dir, "cluster.toml");
        var text = new StringBuilder($"[cluster]\ndata_dir = \"{Dir}/data/node-{{id}}\"\n\n[tls]\nplaintext_on_loopback = true\n\n[retention]\nms = -1\n\n[auth]\nrequired = false\n");
        for (int id = 1; id <= Nodes; id++)
        {
            int peer = FreePort();
            int client = FreePort();
            ClientAddresses.Add(new Address("127.0.0.1", client));
            text.Append($"\n[[node]]\nid = {id}\npeer = \"127.0.0.1:{peer}\"\nclient = \"127.0.0.1:{client}\"\n");
        }
        await File.WriteAllTextAsync(Config, text.ToString());
        for (int id = 1; id <= Nodes; id++)
        {
            Start(id);
        }
        // Serving: every node answers metadata for itself.
        for (int id = 1; id <= Nodes; id++)
        {
            await WaitServingAsync(id);
        }
    }

    internal void Start(int id)
    {
        var info = new ProcessStartInfo(_bin) { RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (string a in new[] { "run", "--config", Config, "--node", id.ToString() })
        {
            info.ArgumentList.Add(a);
        }
        Process p = Process.Start(info)!;
        // Both pipes drain into the node's log so the node never blocks on a full pipe.
        FileStream log = new(Path.Combine(Dir, $"node-{id}.log"), FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
        _ = p.StandardOutput.BaseStream.CopyToAsync(log);
        _ = p.StandardError.BaseStream.CopyToAsync(log);
        _processes[id] = p;
    }

    internal async Task RestartAsync(int id)
    {
        Start(id);
        await WaitServingAsync(id);
    }

    internal async Task WaitServingAsync(int id)
    {
        var options = ClientConfig.CreateBuilder(AddressOf(id).ToString())
            .PlaintextOnLoopback()
            .WithRequestTimeout(TimeSpan.FromSeconds(5))
            .WithRetryFor(TimeSpan.FromSeconds(60))
            .Build();
        using var probe = new LotseClient(options);
        await probe.MetadataAsync();
    }

    /// <summary>Kills the node without warning (SIGKILL), as a crash.</summary>
    internal async Task KillAsync(int id)
    {
        Process? p = _processes[id];
        if (p is null)
        {
            return;
        }
        p.Kill(entireProcessTree: true);
        using var timeout = new System.Threading.CancellationTokenSource(TimeSpan.FromSeconds(10));
        await p.WaitForExitAsync(timeout.Token);
        _processes[id] = null;
    }

    internal string Log(int id)
    {
        string path = Path.Combine(Dir, $"node-{id}.log");
        return File.Exists(path) ? File.ReadAllText(path) : "(no log)";
    }

    private static int FreePort()
    {
        var l = new TcpListener(IPAddress.Loopback, 0);
        l.Start();
        int port = ((IPEndPoint)l.LocalEndpoint).Port;
        l.Stop();
        return port;
    }

    public Task DisposeAsync()
    {
        foreach (Process? p in _processes)
        {
            try
            {
                p?.Kill(entireProcessTree: true);
            }
            catch (Exception)
            {
                // already gone
            }
        }
        return Task.CompletedTask;
    }
}
