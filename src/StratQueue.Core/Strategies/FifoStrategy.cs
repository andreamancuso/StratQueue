namespace StratQueue;

/// <summary>
/// Default FIFO strategy: highest priority first, then insertion order.
/// </summary>
public class FifoStrategy : IDequeueStrategy
{
    public QueueItem? SelectNext(QueueState state, DequeueContext context)
    {
        // Check priorities high (2) → normal (1) → low (0)
        for (int priority = 2; priority >= 0; priority--)
        {
            if (state.PendingByPriority.TryGetValue(priority, out var items) && items.Count > 0)
            {
                return items[0]; // First item in insertion order
            }
        }

        return null;
    }
}
