namespace StratQueue;

/// <summary>
/// Round-robin strategy: cycles through distinct group key values.
/// Within each group, picks by priority then insertion order.
/// Re-evaluates the group set on every call (no stale cache).
/// Tracks the last-served group key to determine the next starting point.
/// When a group disappears, picks up from where it would have been in sort order.
/// </summary>
public class RoundRobinStrategy : IDequeueStrategy
{
    private string? _lastServedGroup;

    public QueueItem? SelectNext(QueueState state, DequeueContext context)
    {
        var groups = state.GroupKeys; // Sorted deterministically
        if (groups.Count == 0)
            return null;

        int startIdx = FindStartIndex(groups);

        // Cycle through all groups from starting position
        for (int i = 0; i < groups.Count; i++)
        {
            int idx = (startIdx + i) % groups.Count;
            var groupKey = groups[idx];

            var item = SelectBestFromGroup(state, groupKey);
            if (item != null)
            {
                _lastServedGroup = groupKey;
                return item;
            }
        }

        return null;
    }

    private int FindStartIndex(IReadOnlyList<string> groups)
    {
        if (_lastServedGroup == null)
            return 0;

        // Try to find the exact last-served group
        for (int i = 0; i < groups.Count; i++)
        {
            int cmp = string.Compare(groups[i], _lastServedGroup, StringComparison.Ordinal);
            if (cmp == 0)
            {
                // Found it — start from next group
                return (i + 1) % groups.Count;
            }
            if (cmp > 0)
            {
                // Passed where it would have been — this is the next group
                return i;
            }
        }

        // Last group was beyond all current groups — wrap to start
        return 0;
    }

    private static QueueItem? SelectBestFromGroup(QueueState state, string groupKey)
    {
        if (!state.GroupIndex.TryGetValue(groupKey, out var items) || items.Count == 0)
            return null;

        QueueItem? best = null;
        foreach (var item in items)
        {
            if (best == null || item.Priority > best.Priority)
                best = item;
        }

        return best;
    }
}
