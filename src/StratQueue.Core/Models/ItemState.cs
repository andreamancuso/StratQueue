namespace StratQueue;

/// <summary>
/// The lifecycle state of a queue item.
/// </summary>
public enum ItemState
{
    Pending,
    CheckedOut,
    DeadLetter
}

/// <summary>
/// Policy for handling checked-out items found during crash recovery.
/// </summary>
public enum RecoveryPolicy
{
    /// <summary>Reset checked-out items to pending with attempts incremented.</summary>
    ResetToPending,

    /// <summary>Leave checked-out items as-is (consumer must reclaim).</summary>
    LeaveCheckedOut
}
