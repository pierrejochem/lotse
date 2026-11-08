using System;
using Lotse.Proto;

namespace Lotse.Client;

/// <summary>What a call of <see cref="LotseClient"/> reports when it fails, whatever the reason.</summary>
public abstract class ClientException : Exception
{
    /// <summary>Creates the exception.</summary>
    protected ClientException(string message)
        : base(message)
    {
    }
}

/// <summary>The broker answered with an error that retrying does not help.</summary>
public sealed class BrokerErrorException : ClientException
{
    /// <summary>Creates the exception.</summary>
    public BrokerErrorException(ErrorCode code, string? brokerMessage)
        : base("broker error " + code + (brokerMessage is null ? string.Empty : ": " + brokerMessage))
    {
        Code = code;
        BrokerMessage = brokerMessage;
    }

    /// <summary>The error code.</summary>
    public ErrorCode Code { get; }

    /// <summary>What the broker said besides the code, or <c>null</c>.</summary>
    public string? BrokerMessage { get; }
}

/// <summary>A fetch asked for an offset that retention deleted, or that is past the end.</summary>
public sealed class OffsetOutOfRangeException : ClientException
{
    /// <summary>Creates the exception.</summary>
    public OffsetOutOfRangeException(string stream, int partition, ulong offset, ulong logStart)
        : base($"offset {offset} is not in {stream} partition {partition}, which starts at {logStart}")
    {
        Stream = stream;
        Partition = partition;
        Offset = offset;
        LogStart = logStart;
    }

    /// <summary>The stream.</summary>
    public string Stream { get; }

    /// <summary>The partition.</summary>
    public int Partition { get; }

    /// <summary>The offset asked for.</summary>
    public ulong Offset { get; }

    /// <summary>Where the partition starts now: a reader that was overtaken by retention goes on from here.</summary>
    public ulong LogStart { get; }
}

/// <summary>
/// The request may have been carried out, and nobody can tell. A produce that reports this was not sent again when
/// that could write it twice; with idempotence it was retried with the same sequence until time ran out.
/// </summary>
public sealed class OutcomeUnknownException : ClientException
{
    /// <summary>Creates the exception.</summary>
    public OutcomeUnknownException(string why)
        : base("outcome unknown: " + why)
    {
    }
}

/// <summary>Nothing was carried out and no broker could be reached, or none could serve the request, in time.</summary>
public sealed class GaveUpException : ClientException
{
    /// <summary>Creates the exception.</summary>
    public GaveUpException(TimeSpan after, string why)
        : base($"gave up after {Describe(after)}: {why}")
    {
        After = after;
    }

    /// <summary>How long the client tried.</summary>
    public TimeSpan After { get; }

    private static string Describe(TimeSpan span) =>
        span.TotalSeconds >= 1 ? $"{span.TotalSeconds:0.###} s" : $"{span.TotalMilliseconds:0} ms";
}

/// <summary>The stream or partition does not exist (any more).</summary>
public sealed class UnknownStreamException : ClientException
{
    /// <summary>Creates the exception.</summary>
    public UnknownStreamException(string name, int partition)
        : base($"unknown stream {name} or partition {partition}")
    {
        Name = name;
        Partition = partition;
    }

    /// <summary>The name asked for.</summary>
    public string Name { get; }

    /// <summary>The partition asked for.</summary>
    public int Partition { get; }
}

/// <summary>A create found the name taken. The exception names the existing resource, which a retried create finds too.</summary>
public sealed class ResourceExistsException : ClientException
{
    /// <summary>Creates the exception.</summary>
    public ResourceExistsException(string name, ulong resourceId)
        : base($"{name} exists already (id {resourceId})")
    {
        Name = name;
        ResourceId = resourceId;
    }

    /// <summary>The name.</summary>
    public string Name { get; }

    /// <summary>The existing resource's id.</summary>
    public ulong ResourceId { get; }
}

/// <summary>The broker did not accept the credentials, or requires some. Retrying with the same credentials does not help.</summary>
public sealed class ClientAuthenticationException : ClientException
{
    /// <summary>Creates the exception.</summary>
    public ClientAuthenticationException(string why)
        : base("authentication failed: " + why)
    {
    }
}
