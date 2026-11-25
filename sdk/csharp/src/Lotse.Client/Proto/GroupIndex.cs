namespace Lotse.Proto;

/// <summary>
/// A position in one Raft group's log: the group and the index applied or wanted there. Frame headers carry lists of
/// them as tag 0: <c>read_at_least</c> in a request, <c>applied</c> in a response.
/// </summary>
/// <param name="Group">The group: <see cref="Metadata"/> or <see cref="Coordinator"/>.</param>
/// <param name="Index">A log index in it.</param>
public readonly record struct GroupIndex(ulong Group, ulong Index)
{
    /// <summary>The metadata group's id.</summary>
    public const ulong Metadata = 1;

    /// <summary>The coordinator group's id.</summary>
    public const ulong Coordinator = 2;
}
