using System;
using Lotse.Proto;

namespace Lotse.Net;

/// <summary>
/// The node refused a request as a whole: the response had the <c>ERROR</c> flag, so it carries only an error code
/// and perhaps a reason, whatever the request was (an unsupported version, a connection that is not authenticated, a
/// request the principal may not make). The request was not carried out.
/// <para>
/// Errors inside a typed response, such as a partition's error in <c>Produce</c>, are not exceptions; they stay in the
/// response.
/// </para>
/// </summary>
public sealed class BrokerException : Exception
{
    /// <summary>Creates the exception.</summary>
    /// <param name="code">The error code.</param>
    /// <param name="reason">The reason the node gave (tagged field 0), or <c>null</c>.</param>
    public BrokerException(ErrorCode code, string? reason)
        : base(reason is null ? "refused: " + code : "refused: " + code + ": " + reason)
    {
        Code = code;
        Reason = reason;
    }

    /// <summary>The error code.</summary>
    public ErrorCode Code { get; }

    /// <summary>The reason the node gave, or <c>null</c>.</summary>
    public string? Reason { get; }
}

/// <summary>
/// A request got no answer because the connection was not there, broke, was closed, or the node did not reply in time.
/// <para>
/// <see cref="RequestSent"/> says whether the request may have reached the node. When it is <c>false</c> the request
/// was certainly not carried out and may be sent again. When it is <c>true</c> the outcome is unknown: resend only
/// what is idempotent.
/// </para>
/// </summary>
public class ConnectionException : Exception
{
    /// <summary>Creates the exception.</summary>
    public ConnectionException(string message, bool requestSent, Exception? inner = null)
        : base(message, inner)
    {
        RequestSent = requestSent;
    }

    /// <summary>Whether the request may have reached the node.</summary>
    public bool RequestSent { get; }
}

/// <summary>No response came within the request timeout. The outcome of the request is unknown.</summary>
public sealed class RequestTimeoutException : ConnectionException
{
    /// <summary>Creates the exception.</summary>
    public RequestTimeoutException(TimeSpan timeout)
        : base($"no response within {timeout.TotalMilliseconds:0} ms", true)
    {
        Timeout = timeout;
    }

    /// <summary>How long the request waited.</summary>
    public TimeSpan Timeout { get; }
}

/// <summary>
/// The connection would be unencrypted by accident: the options name no transport, or plain TCP is asked for an
/// address that is not loopback. Nothing was sent.
/// </summary>
public sealed class InsecureTransportException : ConnectionException
{
    /// <summary>Creates the exception.</summary>
    public InsecureTransportException(string message)
        : base(message, false)
    {
    }
}
