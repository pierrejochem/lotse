using System;
using System.Globalization;

namespace Lotse.Net;

/// <summary>
/// A broker address, <c>host:port</c>. <c>[::1]:9092</c> is an IPv6 address. The host is also the name TLS checks the
/// node's certificate against.
/// </summary>
public sealed record Address
{
    /// <summary>Creates the address.</summary>
    /// <param name="host">The host name or IP address, without brackets.</param>
    /// <param name="port">The port, 1 to 65535.</param>
    public Address(string host, int port)
    {
        ArgumentNullException.ThrowIfNull(host);
        if (host.Length == 0)
        {
            throw new ArgumentException("the host is empty", nameof(host));
        }
        if (port < 1 || port > 65535)
        {
            throw new ArgumentOutOfRangeException(nameof(port), $"port {port} is not from 1 to 65535");
        }
        Host = host;
        Port = port;
    }

    /// <summary>The host name or IP address, without brackets.</summary>
    public string Host { get; }

    /// <summary>The port.</summary>
    public int Port { get; }

    /// <summary>Parses <c>host:port</c> or <c>[ipv6]:port</c>.</summary>
    public static Address Parse(string text)
    {
        ArgumentNullException.ThrowIfNull(text);
        int colon = text.LastIndexOf(':');
        if (colon < 0)
        {
            throw new ArgumentException($"\"{text}\" is not host:port", nameof(text));
        }
        string host = text.Substring(0, colon);
        if (host.StartsWith('[') && host.EndsWith(']'))
        {
            host = host.Substring(1, host.Length - 2);
        }
        else if (host.Contains(':', StringComparison.Ordinal))
        {
            throw new ArgumentException($"\"{text}\": an IPv6 address needs brackets, [::1]:9092", nameof(text));
        }
        if (!int.TryParse(text.AsSpan(colon + 1), NumberStyles.None, CultureInfo.InvariantCulture, out int port))
        {
            throw new ArgumentException($"\"{text}\": the port is not a number", nameof(text));
        }
        return new Address(host, port);
    }

    /// <inheritdoc/>
    public override string ToString() => Host.Contains(':', StringComparison.Ordinal) ? $"[{Host}]:{Port}" : $"{Host}:{Port}";
}
