namespace Lotse.Record;

/// <summary>A transaction marker: whether it commits or aborts, and the coordinator's epoch.</summary>
public sealed record ControlMarker(ControlType Type, uint CoordinatorEpoch);
