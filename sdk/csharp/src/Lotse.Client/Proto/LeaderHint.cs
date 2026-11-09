namespace Lotse.Proto;

/// <summary>
/// Where a partition's leader is, sent with <see cref="ErrorCode.NotLeader"/>. On the wire it is tagged field 0 of
/// the structure it belongs to: <c>leader u32 | term u64</c>.
/// </summary>
/// <param name="Leader">The leader's node id.</param>
/// <param name="Term">Its term.</param>
public sealed record LeaderHint(uint Leader, ulong Term);
