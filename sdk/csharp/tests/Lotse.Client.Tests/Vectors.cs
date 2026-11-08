using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Lotse.Client.Tests;

/// <summary>Reads the golden vectors that the Rust side writes to <c>sdk/conformance</c>. They are not copied.</summary>
internal static class Vectors
{
    private static readonly Lazy<string> Directory = new(Locate);

    // The vectors live at <repo>/sdk/conformance; walk up from the test binary until that exists, so the tests
    // run from any working directory. LOTSE_CONFORMANCE overrides it.
    private static string Locate()
    {
        string? fromEnv = Environment.GetEnvironmentVariable("LOTSE_CONFORMANCE");
        if (!string.IsNullOrEmpty(fromEnv))
        {
            return fromEnv;
        }
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null)
        {
            string candidate = Path.Combine(dir.FullName, "sdk", "conformance");
            if (File.Exists(Path.Combine(candidate, "crc32c.txt")))
            {
                return candidate;
            }
            dir = dir.Parent;
        }
        throw new InvalidOperationException("sdk/conformance not found above " + AppContext.BaseDirectory);
    }

    /// <summary>The lines of a vector file, split at spaces, comments and blank lines left out.</summary>
    public static List<string[]> Lines(string file)
    {
        var output = new List<string[]>();
        foreach (string line in File.ReadAllLines(Path.Combine(Directory.Value, file), Encoding.UTF8))
        {
            if (!string.IsNullOrWhiteSpace(line) && !line.StartsWith('#'))
            {
                output.Add(line.Split(' '));
            }
        }
        if (output.Count == 0)
        {
            throw new InvalidOperationException(file + " holds no vectors");
        }
        return output;
    }

    /// <summary>Hex to bytes; <c>-</c> is no bytes.</summary>
    public static byte[] Hex(string s)
    {
        if (s == "-")
        {
            return Array.Empty<byte>();
        }
        return Convert.FromHexString(s);
    }

    /// <summary>Bytes to lowercase hex.</summary>
    public static string Hex(byte[] b) => Convert.ToHexString(b).ToLowerInvariant();

    /// <summary>The first <paramref name="length"/> bytes.</summary>
    public static byte[] Cut(byte[] b, int length) => b.AsSpan(0, length).ToArray();

    /// <summary><paramref name="b"/> with one byte added.</summary>
    public static byte[] Longer(byte[] b)
    {
        byte[] output = new byte[b.Length + 1];
        Buffer.BlockCopy(b, 0, output, 0, b.Length);
        return output;
    }
}
