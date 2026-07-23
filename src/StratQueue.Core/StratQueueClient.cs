using StratQueue.Internal;

namespace StratQueue;

/// <summary>
/// Main entry point for StratQueue. In-memory work queue with pluggable
/// dequeue strategies and SQLite persistence.
/// </summary>
public class StratQueueClient : IDisposable
{
    private readonly SqliteJournal _journal;
    private readonly QueueManager _manager;

    public StratQueueClient(string sqlitePath, StratQueueOptions? options = null)
    {
        options ??= new StratQueueOptions();
        _journal = new SqliteJournal(sqlitePath, options.EnableWalMode);
        _manager = new QueueManager(_journal, options.RecoveryPolicy);
    }

    /// <summary>Enqueue a single item.</summary>
    public QueueItem Enqueue(string queueName, string payload, EnqueueOptions? options = null)
        => _manager.Enqueue(queueName, payload, options);

    /// <summary>Enqueue multiple items in a single SQLite transaction.</summary>
    public IReadOnlyList<QueueItem> EnqueueBatch(string queueName, IEnumerable<EnqueueRequest> items)
        => _manager.EnqueueBatch(queueName, items);

    /// <summary>Dequeue the next item using the given strategy (default: FIFO).</summary>
    public CheckedOutItem? Dequeue(string queueName, IDequeueStrategy? strategy = null)
        => _manager.Dequeue(queueName, strategy);

    /// <summary>Asynchronously dequeue — blocks until an item is available.</summary>
    public Task<CheckedOutItem> DequeueAsync(string queueName, IDequeueStrategy? strategy = null, CancellationToken cancellationToken = default)
        => _manager.DequeueAsync(queueName, strategy, cancellationToken);

    /// <summary>Commit a checked-out item (removes it from the queue).</summary>
    public void Commit(string checkoutId)
        => _manager.Commit(checkoutId);

    /// <summary>Abort a checked-out item (returns to pending or dead-letters).</summary>
    public void Abort(string checkoutId, string? error = null)
        => _manager.Abort(checkoutId, error);

    /// <summary>Release a checked-out item back to pending without consuming a retry.</summary>
    public void Release(string checkoutId)
        => _manager.Release(checkoutId);

    /// <summary>Peek at the next item without checking it out.</summary>
    public QueueItem? Peek(string queueName, IDequeueStrategy? strategy = null)
        => _manager.Peek(queueName, strategy);

    /// <summary>Count items in a queue, optionally filtered by state.</summary>
    public int Count(string queueName, ItemState? state = null)
        => _manager.Count(queueName, state);

    /// <summary>Get all queue names that have items.</summary>
    public IReadOnlyList<string> GetQueueNames()
        => _manager.GetQueueNames();

    /// <summary>List items in a queue with optional filtering and pagination.</summary>
    public IReadOnlyList<QueueItem> List(string queueName, ListOptions? options = null)
        => _manager.List(queueName, options);

    /// <summary>Get dead-lettered items for a queue.</summary>
    public IReadOnlyList<QueueItem> GetDeadLetterItems(string queueName, int limit = 100, int offset = 0)
        => _manager.GetDeadLetterItems(queueName, limit, offset);

    /// <summary>Retry a dead-lettered item (re-enqueue with attempts reset).</summary>
    public void Retry(string itemId)
        => _manager.Retry(itemId);

    /// <summary>Retry all dead-lettered items for a queue.</summary>
    public void RetryAll(string queueName)
        => _manager.RetryAll(queueName);

    /// <summary>Remove all items from a queue.</summary>
    public void Purge(string queueName)
        => _manager.Purge(queueName);

    /// <summary>Remove all dead-lettered items from a queue.</summary>
    public void PurgeDeadLetter(string queueName)
        => _manager.PurgeDeadLetter(queueName);

    public void Dispose()
    {
        _manager.Dispose();
        _journal.Dispose();
    }
}
