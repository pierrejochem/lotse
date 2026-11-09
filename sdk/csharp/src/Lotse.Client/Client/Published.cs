using System.Collections.Generic;

namespace Lotse.Client;

/// <summary>One queue that holds a published message.</summary>
/// <param name="Queue">The queue's name.</param>
/// <param name="Offset">The message's offset there.</param>
public sealed record RoutedQueue(string Queue, ulong Offset);

/// <summary>Where a published message went: each queue it was routed to and the message's offset there, in queue name order.</summary>
/// <param name="Routed">The queues and offsets.</param>
public sealed record Published(IReadOnlyList<RoutedQueue> Routed);
