using System;
using Lotse.Proto;

namespace Lotse.Auth;

/// <summary>
/// The node did not accept the credentials, or the node could not prove it knows the user's credential. The
/// connection must not be used afterwards: the node closes it after a failed attempt.
/// <para>
/// A node says no more than "authentication failed" when the user or the password is wrong; the two cannot be told
/// apart, by design.
/// </para>
/// </summary>
public class AuthenticationException : Exception
{
    /// <summary>Creates the exception.</summary>
    /// <param name="message">What happened and, where useful, what to do.</param>
    /// <param name="code">The error code the node answered with, or <c>null</c> if the failure was found on this side.</param>
    public AuthenticationException(string message, ErrorCode? code)
        : base(message)
    {
        Code = code;
    }

    /// <summary>
    /// The error code the node answered with (<c>AUTHENTICATION_FAILED</c>, <c>UNSUPPORTED_SASL_MECHANISM</c>), or
    /// <c>null</c>.
    /// </summary>
    public ErrorCode? Code { get; }
}
