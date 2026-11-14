namespace Lotse.Record;

/// <summary>What a batch holds. The numbers are the wire codes.</summary>
public enum BatchKind
{
    /// <summary>Messages.</summary>
    Data = 0,

    /// <summary>Acknowledgements of a queue.</summary>
    Ack = 1,

    /// <summary>A queue's state, written by the leader.</summary>
    QueueState = 2,

    /// <summary>A timer of a delayed message.</summary>
    Timer = 3,

    /// <summary>A dead-letter move.</summary>
    DeadLetter = 4,

    /// <summary>A message retired from a queue.</summary>
    Retire = 5,

    /// <summary>Messages a queue's leader hands to a consumer, written before it pushes them (ADR 0109).</summary>
    Checkout = 6,
}
