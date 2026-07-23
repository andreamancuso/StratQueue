using System.Collections.Concurrent;

namespace StratQueue.Internal;

/// <summary>
/// In-memory queue orchestration with SQLite persistence.
/// All scheduling decisions happen here in C#. SQLite is the journal.
/// </summary>
internal class QueueManager : IDisposable
{
    private readonly SqliteJournal _journal;
    private readonly ConcurrentDictionary<string, MutableQueueState> _queues = new();
    private readonly RecoveryPolicy _recoveryPolicy;

    public QueueManager(SqliteJournal journal, RecoveryPolicy recoveryPolicy = RecoveryPolicy.ResetToPending)
    {
        _journal = journal;
        _recoveryPolicy = recoveryPolicy;
        Recover();
    }

    private void Recover()
    {
        var items = _journal.LoadAll();
        foreach (var item in items)
        {
            var state = GetOrCreateQueue(item.QueueName);
            switch (item.State)
            {
                case ItemState.Pending:
                    state.AddPending(item);
                    break;

                case ItemState.CheckedOut:
                    if (_recoveryPolicy == RecoveryPolicy.ResetToPending)
                    {
                        var recovered = item with { State = ItemState.Pending, Attempts = item.Attempts + 1, CheckoutId = null, CheckedOutAt = null };
                        _journal.UpdateState(item.Id, ItemState.Pending, incrementAttempts: true);
                        state.AddPending(recovered);
                    }
                    else
                    {
                        state.AddCheckedOut(item.CheckoutId!, item);
                    }
                    break;

                case ItemState.DeadLetter:
                    state.AddDeadLetter(item);
                    break;
            }
        }
    }

    public QueueItem Enqueue(string queueName, string payload, EnqueueOptions? options = null)
    {
        options ??= new EnqueueOptions();
        var item = new QueueItem
        {
            Id = Guid.NewGuid().ToString(),
            QueueName = queueName,
            Payload = payload,
            State = ItemState.Pending,
            Priority = options.Priority,
            GroupKey = options.GroupKey,
            Attempts = 0,
            MaxRetries = options.MaxRetries,
            EnqueuedAt = DateTime.UtcNow
        };

        _journal.Insert(item);

        var state = GetOrCreateQueue(queueName);
        lock (state.Lock)
        {
            state.AddPending(item);
            state.NotifyItemAvailable();
        }

        return item;
    }

    public IReadOnlyList<QueueItem> EnqueueBatch(string queueName, IEnumerable<EnqueueRequest> requests)
    {
        var items = new List<QueueItem>();
        foreach (var req in requests)
        {
            var opts = req.Options ?? new EnqueueOptions();
            items.Add(new QueueItem
            {
                Id = Guid.NewGuid().ToString(),
                QueueName = queueName,
                Payload = req.Payload,
                State = ItemState.Pending,
                Priority = opts.Priority,
                GroupKey = opts.GroupKey,
                Attempts = 0,
                MaxRetries = opts.MaxRetries,
                EnqueuedAt = DateTime.UtcNow
            });
        }

        _journal.InsertBatch(items);

        var state = GetOrCreateQueue(queueName);
        lock (state.Lock)
        {
            foreach (var item in items)
            {
                state.AddPending(item);
                state.NotifyItemAvailable();
            }
        }

        return items;
    }

    public CheckedOutItem? Dequeue(string queueName, IDequeueStrategy? strategy = null)
    {
        strategy ??= new FifoStrategy();
        var state = GetOrCreateQueue(queueName);

        lock (state.Lock)
        {
            var snapshot = state.BuildSnapshot();
            var selected = strategy.SelectNext(snapshot, new DequeueContext());
            if (selected == null) return null;

            return Checkout(state, selected);
        }
    }

    public async Task<CheckedOutItem> DequeueAsync(string queueName, IDequeueStrategy? strategy = null, CancellationToken cancellationToken = default)
    {
        strategy ??= new FifoStrategy();
        var state = GetOrCreateQueue(queueName);

        while (true)
        {
            await state.WaitForItemAsync(cancellationToken);

            lock (state.Lock)
            {
                var snapshot = state.BuildSnapshot();
                var selected = strategy.SelectNext(snapshot, new DequeueContext());
                if (selected != null)
                {
                    return Checkout(state, selected);
                }
                // Item was taken by another consumer — loop and wait again
            }
        }
    }

    private CheckedOutItem Checkout(MutableQueueState state, QueueItem selected)
    {
        var checkoutId = Guid.NewGuid().ToString();
        var checkedOut = selected with
        {
            State = ItemState.CheckedOut,
            CheckoutId = checkoutId,
            CheckedOutAt = DateTime.UtcNow
        };

        state.RemovePending(selected.Id);
        state.AddCheckedOut(checkoutId, checkedOut);

        // Synchronous write — crash safety
        _journal.UpdateState(selected.Id, ItemState.CheckedOut, checkoutId: checkoutId);

        return new CheckedOutItem { CheckoutId = checkoutId, Item = checkedOut };
    }

    public void Commit(string checkoutId)
    {
        var (queueName, item) = FindCheckedOut(checkoutId);
        var state = _queues[queueName];

        lock (state.Lock)
        {
            state.RemoveCheckedOut(checkoutId);
        }

        _journal.Delete(item.Id);
    }

    public void Abort(string checkoutId, string? error = null)
    {
        var (queueName, item) = FindCheckedOut(checkoutId);
        var state = _queues[queueName];

        lock (state.Lock)
        {
            state.RemoveCheckedOut(checkoutId);

            var newAttempts = item.Attempts + 1;
            if (newAttempts >= item.MaxRetries)
            {
                // Dead letter
                var deadItem = item with { State = ItemState.DeadLetter, LastError = error, Attempts = newAttempts, CheckoutId = null, CheckedOutAt = null };
                state.AddDeadLetter(deadItem);
                _journal.UpdateState(item.Id, ItemState.DeadLetter, error: error, incrementAttempts: true);
            }
            else
            {
                // Return to pending
                var pending = item with { State = ItemState.Pending, LastError = error, Attempts = newAttempts, CheckoutId = null, CheckedOutAt = null };
                state.AddPending(pending);
                state.NotifyItemAvailable();
                _journal.UpdateState(item.Id, ItemState.Pending, error: error, incrementAttempts: true);
            }
        }
    }

    public void Release(string checkoutId)
    {
        var (queueName, item) = FindCheckedOut(checkoutId);
        var state = _queues[queueName];

        lock (state.Lock)
        {
            state.RemoveCheckedOut(checkoutId);
            var pending = item with
            {
                State = ItemState.Pending,
                CheckoutId = null,
                CheckedOutAt = null
            };
            state.AddPending(pending);
            state.NotifyItemAvailable();
            _journal.UpdateState(item.Id, ItemState.Pending, error: item.LastError);
        }
    }

    public QueueItem? Peek(string queueName, IDequeueStrategy? strategy = null)
    {
        strategy ??= new FifoStrategy();
        if (!_queues.TryGetValue(queueName, out var state)) return null;

        lock (state.Lock)
        {
            var snapshot = state.BuildSnapshot();
            return strategy.SelectNext(snapshot, new DequeueContext());
        }
    }

    public int Count(string queueName, ItemState? stateFilter = null)
    {
        if (!_queues.TryGetValue(queueName, out var state)) return 0;

        lock (state.Lock)
        {
            if (stateFilter == null)
                return state.PendingCount + state.CheckedOutCount + state.DeadLetterCount;

            return stateFilter switch
            {
                ItemState.Pending => state.PendingCount,
                ItemState.CheckedOut => state.CheckedOutCount,
                ItemState.DeadLetter => state.DeadLetterCount,
                _ => 0
            };
        }
    }

    public IReadOnlyList<string> GetQueueNames()
    {
        return _queues.Keys.ToList();
    }

    public IReadOnlyList<QueueItem> List(string queueName, ListOptions? options = null)
    {
        options ??= new ListOptions();
        if (!_queues.TryGetValue(queueName, out var state)) return [];

        lock (state.Lock)
        {
            var allItems = state.GetAllItems(options.State);
            return allItems.Skip(options.Offset).Take(options.Limit).ToList();
        }
    }

    public IReadOnlyList<QueueItem> GetDeadLetterItems(string queueName, int limit = 100, int offset = 0)
    {
        if (!_queues.TryGetValue(queueName, out var state)) return [];

        lock (state.Lock)
        {
            return state.DeadLetterItems.Skip(offset).Take(limit).ToList();
        }
    }

    public void Retry(string itemId)
    {
        foreach (var (queueName, state) in _queues)
        {
            lock (state.Lock)
            {
                var deadItem = state.FindDeadLetter(itemId);
                if (deadItem == null) continue;

                state.RemoveDeadLetter(itemId);
                var pending = deadItem with { State = ItemState.Pending, Attempts = 0, LastError = null };
                state.AddPending(pending);
                state.NotifyItemAvailable();
                _journal.UpdateState(itemId, ItemState.Pending);
                // Reset attempts in DB
                return;
            }
        }

        throw new InvalidOperationException($"Dead letter item '{itemId}' not found.");
    }

    public void RetryAll(string queueName)
    {
        if (!_queues.TryGetValue(queueName, out var state)) return;

        lock (state.Lock)
        {
            var deadItems = state.DeadLetterItems.ToList();
            foreach (var item in deadItems)
            {
                state.RemoveDeadLetter(item.Id);
                var pending = item with { State = ItemState.Pending, Attempts = 0, LastError = null };
                state.AddPending(pending);
                state.NotifyItemAvailable();
                _journal.UpdateState(item.Id, ItemState.Pending);
            }
        }
    }

    public void Purge(string queueName)
    {
        if (_queues.TryRemove(queueName, out var state))
        {
            lock (state.Lock)
            {
                state.Clear();
            }
        }
        _journal.DeleteByQueue(queueName);
    }

    public void PurgeDeadLetter(string queueName)
    {
        if (!_queues.TryGetValue(queueName, out var state)) return;

        lock (state.Lock)
        {
            var deadItems = state.DeadLetterItems.ToList();
            foreach (var item in deadItems)
            {
                state.RemoveDeadLetter(item.Id);
                _journal.Delete(item.Id);
            }
        }
    }

    private MutableQueueState GetOrCreateQueue(string queueName)
    {
        return _queues.GetOrAdd(queueName, _ => new MutableQueueState());
    }

    private (string QueueName, QueueItem Item) FindCheckedOut(string checkoutId)
    {
        foreach (var (queueName, state) in _queues)
        {
            lock (state.Lock)
            {
                if (state.TryGetCheckedOut(checkoutId, out var item))
                    return (queueName, item);
            }
        }
        throw new InvalidOperationException($"Checkout '{checkoutId}' not found.");
    }

    public void Dispose()
    {
        foreach (var (_, state) in _queues)
        {
            state.Dispose();
        }
    }

    /// <summary>
    /// Internal mutable state for a single queue. All access must be under Lock.
    /// </summary>
    private class MutableQueueState : IDisposable
    {
        // Pending items by priority: 2=high, 1=normal, 0=low. Each list in insertion order.
        private readonly Dictionary<int, List<QueueItem>> _pendingByPriority = new()
        {
            [2] = [],
            [1] = [],
            [0] = []
        };

        // Group index: group key → items in that group (insertion order)
        // Null group keys are stored as empty string (Dictionary doesn't allow null keys)
        private readonly Dictionary<string, List<QueueItem>> _groupIndex = new();

        // Checked out items by checkout ID
        private readonly Dictionary<string, QueueItem> _checkedOut = new();

        // Dead letter items
        private readonly List<QueueItem> _deadLetter = [];

        // Semaphore for async dequeue
        private readonly SemaphoreSlim _itemAvailable = new(0);

        public object Lock { get; } = new();

        public int PendingCount => _pendingByPriority.Values.Sum(l => l.Count);
        public int CheckedOutCount => _checkedOut.Count;
        public int DeadLetterCount => _deadLetter.Count;
        public IReadOnlyList<QueueItem> DeadLetterItems => _deadLetter;

        private static string NormalizeGroupKey(string? key) => key ?? "";

        public void AddPending(QueueItem item)
        {
            _pendingByPriority[item.Priority].Add(item);

            var gk = NormalizeGroupKey(item.GroupKey);
            if (!_groupIndex.TryGetValue(gk, out var groupList))
            {
                groupList = [];
                _groupIndex[gk] = groupList;
            }
            groupList.Add(item);
        }

        public void RemovePending(string itemId)
        {
            foreach (var (_, list) in _pendingByPriority)
            {
                var idx = list.FindIndex(i => i.Id == itemId);
                if (idx >= 0)
                {
                    var item = list[idx];
                    list.RemoveAt(idx);

                    var gk = NormalizeGroupKey(item.GroupKey);
                    if (_groupIndex.TryGetValue(gk, out var groupList))
                    {
                        groupList.RemoveAll(i => i.Id == itemId);
                        if (groupList.Count == 0) _groupIndex.Remove(gk);
                    }
                    return;
                }
            }
        }

        public void AddCheckedOut(string checkoutId, QueueItem item)
        {
            _checkedOut[checkoutId] = item;
        }

        public void RemoveCheckedOut(string checkoutId)
        {
            _checkedOut.Remove(checkoutId);
        }

        public bool TryGetCheckedOut(string checkoutId, out QueueItem item)
        {
            return _checkedOut.TryGetValue(checkoutId, out item!);
        }

        public void AddDeadLetter(QueueItem item)
        {
            _deadLetter.Add(item);
        }

        public void RemoveDeadLetter(string itemId)
        {
            _deadLetter.RemoveAll(i => i.Id == itemId);
        }

        public QueueItem? FindDeadLetter(string itemId)
        {
            return _deadLetter.FirstOrDefault(i => i.Id == itemId);
        }

        public void NotifyItemAvailable()
        {
            _itemAvailable.Release();
        }

        public Task WaitForItemAsync(CancellationToken ct)
        {
            return _itemAvailable.WaitAsync(ct);
        }

        public QueueState BuildSnapshot()
        {
            var pendingByPriority = new Dictionary<int, IReadOnlyList<QueueItem>>();
            foreach (var (priority, list) in _pendingByPriority)
            {
                pendingByPriority[priority] = list.ToList();
            }

            var groupIndex = new Dictionary<string, IReadOnlyList<QueueItem>>();
            var groupKeys = new List<string>();
            foreach (var (key, list) in _groupIndex)
            {
                groupIndex[key] = list.ToList();
                groupKeys.Add(key);
            }
            groupKeys.Sort(StringComparer.Ordinal); // Deterministic order for strategies

            return new QueueState
            {
                PendingByPriority = pendingByPriority,
                GroupIndex = groupIndex,
                PendingCount = PendingCount,
                GroupKeys = groupKeys
            };
        }

        public IEnumerable<QueueItem> GetAllItems(ItemState? filter)
        {
            if (filter == null || filter == ItemState.Pending)
            {
                foreach (var (_, list) in _pendingByPriority.OrderByDescending(kv => kv.Key))
                    foreach (var item in list)
                        yield return item;
            }

            if (filter == null || filter == ItemState.CheckedOut)
            {
                foreach (var item in _checkedOut.Values)
                    yield return item;
            }

            if (filter == null || filter == ItemState.DeadLetter)
            {
                foreach (var item in _deadLetter)
                    yield return item;
            }
        }

        public void Clear()
        {
            foreach (var (_, list) in _pendingByPriority) list.Clear();
            _groupIndex.Clear();
            _checkedOut.Clear();
            _deadLetter.Clear();
        }

        public void Dispose()
        {
            _itemAvailable.Dispose();
        }
    }
}
