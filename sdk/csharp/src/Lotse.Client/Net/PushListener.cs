using Lotse.Proto;

namespace Lotse.Net;

/// <summary>
/// Receives the pushes of one subscription on a connection.
/// <para>
/// Both methods run on the connection's read loop: return quickly, hand the work to another task, and never wait for a
/// response of the same connection (the read loop would be the one to deliver it). Pushes of one subscription arrive
/// in order, also those buffered before <see cref="Connection.Register"/> was called. An exception thrown by a listener
/// is swallowed: it must not end the connection.
/// </para>
/// </summary>
public interface IPushListener
{
    /// <summary>A delivery, or the end of the subscription (<see cref="Frames.DeliverPush.Error"/> set).</summary>
    void OnPush(Frames.DeliverPush push);

    /// <summary>The connection broke or was closed; no more pushes will come.</summary>
    void OnClose(ConnectionException reason);
}
