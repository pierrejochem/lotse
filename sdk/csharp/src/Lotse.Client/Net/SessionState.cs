namespace Lotse.Net;

/// <summary>Where a connection's session stands.</summary>
public enum SessionState
{
    /// <summary>The session lasts as long as the connection (a client certificate, or no authentication required).</summary>
    Unlimited,

    /// <summary>Valid, and not due for renewal yet.</summary>
    Active,

    /// <summary>Valid, but past four fifths of its lifetime: authenticate again on this connection.</summary>
    RenewDue,

    /// <summary>Over. The node answers <c>UNAUTHORIZED</c>; open a new connection and authenticate.</summary>
    Ended,
}
