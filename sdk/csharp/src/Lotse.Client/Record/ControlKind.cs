namespace Lotse.Record;

/// <summary>The type of a control batch's record. Consumers skip all of them.</summary>
public enum ControlKind
{
    /// <summary>A transaction marker: abort.</summary>
    AbortMarker = 0,

    /// <summary>A transaction marker: commit.</summary>
    CommitMarker = 1,

    /// <summary>Raft's no-op entry of a new leader.</summary>
    RaftNoop = 2,

    /// <summary>Raft's membership entry.</summary>
    RaftConfig = 3,

    /// <summary>The coordinator's producer ids.</summary>
    ProducerId = 4,

    /// <summary>The coordinator's transaction state.</summary>
    TxnState = 5,

    /// <summary>The coordinator's group state.</summary>
    GroupState = 6,

    /// <summary>The coordinator's group offsets.</summary>
    GroupOffsets = 7,

    /// <summary>The metadata group's records.</summary>
    Metadata = 8,

    /// <summary>The coordinator forgets a tenant incarnation's ids.</summary>
    Forget = 9,

    /// <summary>Producers a partition's leader found idle long enough to forget (ADR 0110).</summary>
    ExpireProducers = 10,
}
