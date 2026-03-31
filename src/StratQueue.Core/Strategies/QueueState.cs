namespace StratQueue;

/// <summary>
/// Read-only view of a queue's in-memory state, exposed to dequeue strategies.
/// The actual mutable state lives in QueueManager.
/// Group keys are never null in the snapshot — null GroupKey on items maps to empty string "".
/// </summary>
public class QueueState
{
    /// <summary>Pending items grouped by priority (high=2, normal=1, low=0). Each list is in insertion order.</summary>
    public required IReadOnlyDictionary<int, IReadOnlyList<QueueItem>> PendingByPriority { get; init; }

    /// <summary>Pending items grouped by group key. Empty string "" = ungrouped items.</summary>
    public required IReadOnlyDictionary<string, IReadOnlyList<QueueItem>> GroupIndex { get; init; }

    /// <summary>Total number of pending items.</summary>
    public int PendingCount { get; init; }

    /// <summary>Ordered list of distinct group keys for round-robin cycling. "" = ungrouped.</summary>
    public required IReadOnlyList<string> GroupKeys { get; init; }
}
