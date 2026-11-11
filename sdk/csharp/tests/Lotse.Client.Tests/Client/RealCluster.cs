using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
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
    private int _attempt = 1;

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
        // A port chosen free can be taken before the node binds it (by another test process choosing the same one): a
        // node that finds its port taken exits at once. Then the whole cluster starts again on new ports and a new data
        // directory, up to five times.
        for (_attempt = 1; ; _attempt++)
        {
            Config = Path.Combine(Dir, $"cluster-{_attempt}.toml");
            ClientAddresses.Clear();
            var text = new StringBuilder($"[cluster]\ndata_dir = \"{Dir}/data-{_attempt}/node-{{id}}\"\n\n[tls]\nplaintext_on_loopback = true\n\n[retention]\nms = -1\n\n[auth]\nrequired = false\n");
            for (int id = 1; id <= Nodes; id++)
            {
                int peer = Lotse.Client.Tests.Net.Certs.FreePort();
                int client = Lotse.Client.Tests.Net.Certs.FreePort();
                ClientAddresses.Add(new Address("127.0.0.1", client));
                text.Append($"\n[[node]]\nid = {id}\npeer = \"127.0.0.1:{peer}\"\nclient = \"127.0.0.1:{client}\"\n");
            }
            await File.WriteAllTextAsync(Config, text.ToString());
            for (int id = 1; id <= Nodes; id++)
            {
                Start(id);
            }
            await Task.Delay(TimeSpan.FromMilliseconds(1500));
            int[] exited = Enumerable.Range(1, Nodes).Where(id => _processes[id]?.HasExited ?? true).ToArray();
            if (exited.Length == 0)
            {
                break;
            }
            await DisposeAsync();
            if (_attempt >= 5)
            {
                throw new InvalidOperationException(
                    $"nodes {string.Join(", ", exited)} exit at start\n" + string.Join("\n", exited.Select(Log)));
            }
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
        FileStream log = new(LogPath(id), FileMode.Append, FileAccess.Write, FileShare.ReadWrite);
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

    private string LogPath(int id) => Path.Combine(Dir, $"node-{id}-{_attempt}.log");

    internal string Log(int id)
    {
        string path = LogPath(id);
        // The node may still be writing it.
        if (!File.Exists(path))
        {
            return "(no log)";
        }
        using var reader = new StreamReader(new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));
        return reader.ReadToEnd();
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
