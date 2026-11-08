namespace Lotse.Auth;

/// <summary>Why a SCRAM exchange stopped.</summary>
public enum ScramFailure
{
    /// <summary>
    /// A message does not follow the mechanism's grammar, or carries a nonce or iteration count that is not acceptable.
    /// </summary>
    Malformed,

    /// <summary>The server's signature is missing or wrong: it does not know the user's credential.</summary>
    ServerNotVerified,

    /// <summary>The step does not fit the exchange's state.</summary>
    OutOfOrder,
}

/// <summary>A SCRAM exchange that cannot go on: a message that breaks the grammar, or a server that fails its checks.</summary>
public sealed class ScramException : AuthenticationException
{
    internal ScramException(ScramFailure kind, string message)
        : base(message, null)
    {
        Kind = kind;
    }

    /// <summary>Why the exchange stopped.</summary>
    public ScramFailure Kind { get; }
}
