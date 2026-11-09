using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Xunit;

namespace Lotse.Client.Tests;

/// <summary>A fact that needs the real <c>lotse</c> executable (<c>LOTSE_BIN</c>) and is skipped, with a message, without it.</summary>
public sealed class LotseFactAttribute : FactAttribute
{
    public LotseFactAttribute()
    {
        if (LotseBinary.Find() is null)
        {
            Skip = LotseBinary.WhySkipped;
        }
    }
}

/// <summary>Finds the <c>lotse</c> executable the tests that need a real node or real certificates use.</summary>
internal static class LotseBinary
{
    internal const string WhySkipped =
        "SKIPPED: LOTSE_BIN is not set to a lotse executable. Build one with `cargo build -p lotse-node` and run with "
        + "LOTSE_BIN=<path to target/debug/lotse>.";

    /// <summary><c>LOTSE_BIN</c> if it names an executable file, else <c>null</c>.</summary>
    internal static string? Find()
    {
        string? env = Environment.GetEnvironmentVariable("LOTSE_BIN");
        return !string.IsNullOrWhiteSpace(env) && File.Exists(env) ? Path.GetFullPath(env) : null;
    }

    /// <summary>Runs the executable and returns its output; fails on a non-zero exit.</summary>
    internal static async Task<string> RunAsync(string bin, params string[] args)
    {
        var info = new ProcessStartInfo(bin) { RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (string a in args)
        {
            info.ArgumentList.Add(a);
        }
        using Process p = Process.Start(info)!;
        Task<string> stdout = p.StandardOutput.ReadToEndAsync();
        Task<string> stderr = p.StandardError.ReadToEndAsync();
        using var timeout = new System.Threading.CancellationTokenSource(TimeSpan.FromSeconds(60));
        await p.WaitForExitAsync(timeout.Token);
        string output = await stdout + await stderr;
        if (p.ExitCode != 0)
        {
            throw new InvalidOperationException(bin + " " + string.Join(' ', args) + " failed:\n" + output);
        }
        return output;
    }
}
