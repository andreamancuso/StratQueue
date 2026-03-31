namespace StratQueue;

/// <summary>
/// Selects the next item to dequeue from available pending items.
/// Implementations must be fast and non-blocking — called under lock.
/// </summary>
public interface IDequeueStrategy
{
    /// <summary>
    /// Selects the next item to dequeue, or null if no suitable item is available.
    /// </summary>
    QueueItem? SelectNext(QueueState state, DequeueContext context);
}
