namespace Lotse.Record;

/// <summary>The two transaction markers. The numbers are the wire codes.</summary>
public enum ControlType
{
    /// <summary>The transaction aborted.</summary>
    Abort = 0,

    /// <summary>The transaction committed.</summary>
    Commit = 1,
}
