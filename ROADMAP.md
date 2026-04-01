# StratQueue Roadmap

## Problem Statement

Persistent embedded queues in .NET (LiteQueue, DiskQueue, etc.) are all FIFO-only. They have no concept of *what* they're dequeueing — just *when* it was enqueued. This breaks down when queue items belong to logical groups that need fair scheduling.

**Concrete example**: A web scraping pipeline enqueues job URLs from multiple portal workers. When a portal processes a large company (e.g., JPMorgan with 7,644 jobs), thousands of URLs from the same domain burst into the queue. FIFO processing hammers that one domain with no natural gap, while URLs from other domains sit idle — even though those domains could be processed immediately.

No NuGet package solves this. The options are either FIFO-only embedded queues or heavyweight distributed brokers (RabbitMQ, Kafka) that require external infrastructure.

## Design Principles

1. **In-memory orchestration, SQLite journal** — All scheduling decisions happen in C# using concurrent collections. SQLite is the persistence layer (write-ahead journal), not the orchestration layer. No SQL gymnastics for strategy logic.
2. **Strategy is a consumer concern** — The queue stores items. The dequeue strategy decides what comes out next. The same queue can be consumed with different strategies by different consumers.
3. **Small surface area** — This is ~300-400 lines of core C# plus ~100 lines of SQLite schema. Not a framework. Not an abstraction astronaut's playground.
4. **Crash recovery from SQLite** — On startup, rebuild in-memory state from SQLite. Checked-out items with no commit get reset to pending (or increment attempt count). The journal is always the source of truth for recovery.
5. **Zero external dependencies beyond SQLite** — No external servers, no background services, no network calls. Single-process, embedded.

## Architecture

```
┌─────────────────────────────────────────────────┐
│                  Consumer Code                   │
│         queue.Dequeue(strategy, ...)             │
└──────────────────┬──────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────┐
│              StratQueue Core                     │
│                                                  │
│  ┌──────────────┐  ┌─────────────────────────┐  │
│  │ QueueManager │  │  IDequeueStrategy       │  │
│  │              │  │  ├─ FifoStrategy         │  │
│  │  In-memory   │  │  └─ RoundRobinStrategy   │  │
│  │  state +     │  │     (groupField-based)   │  │
│  │  scheduling  │  └─────────────────────────┘  │
│  └──────┬───────┘                                │
│         │                                        │
│  ┌──────▼───────┐                                │
│  │ SqliteJournal│  Persistence layer             │
│  │  - enqueue   │  (write-ahead, recovery)       │
│  │  - checkout  │                                │
│  │  - commit    │                                │
│  │  - abort     │                                │
│  └──────────────┘                                │
└──────────────────────────────────────────────────┘
```

### State Machine

Each queue item has a lifecycle:

```
  enqueue()        dequeue()        commit()
 ─────────► PENDING ────────► CHECKED_OUT ────────► (deleted)
                ▲                   │
                │      abort()      │
                │   attempts < max  │
                └───────────────────┘
                                    │
                        abort()     │
                     attempts >= max│
                                    ▼
                               DEAD_LETTER
```

### In-Memory Structures

```
ConcurrentDictionary<string, QueueState> _queues   // keyed by queue name

QueueState:
  - PendingItems: grouped by priority (high/normal/low)
  - CheckedOutItems: dictionary keyed by checkout ID
  - GroupIndex: dictionary<string, List<QueueItem>> keyed by group field value
    (maintained alongside PendingItems for O(1) group-aware dequeue)
  - RoundRobinCursor: current position in group rotation
  - Lock: SemaphoreSlim for thread-safe state transitions
```

### SQLite Schema

```sql
CREATE TABLE queue_items (
    id            TEXT PRIMARY KEY,   -- UUID
    queue_name    TEXT NOT NULL,
    state         TEXT NOT NULL,      -- 'pending', 'checked_out', 'dead_letter'
    priority      INTEGER NOT NULL DEFAULT 1,  -- 0=low, 1=normal, 2=high
    group_key     TEXT,               -- optional, for strategy-based dequeue
    payload       TEXT NOT NULL,      -- JSON
    attempts      INTEGER NOT NULL DEFAULT 0,
    max_retries   INTEGER NOT NULL DEFAULT 3,
    enqueued_at   TEXT NOT NULL,      -- ISO 8601
    checked_out_at TEXT,
    last_error    TEXT,
    checkout_id   TEXT                -- UUID assigned on dequeue, used for commit/abort
);

CREATE INDEX ix_queue_pending ON queue_items (queue_name, state, priority DESC, id);
CREATE INDEX ix_queue_group   ON queue_items (queue_name, state, group_key);
```

Single table. Dead-letter items live in the same table with `state = 'dead_letter'` (no separate collection like LiteQueue's `dead_{name}`).

### Concurrency Model

Multiple consumers (worker threads) can dequeue simultaneously:

1. Consumer calls `Dequeue(queueName, strategy)`
2. Strategy selects the next item from in-memory state (under `SemaphoreSlim`)
3. Item is moved from `PendingItems` to `CheckedOutItems` in memory
4. **Synchronous write** to SQLite: `UPDATE state = 'checked_out'` — this must complete before the item is handed to the consumer (crash safety: if the app dies between checkout and commit, recovery knows it was in-flight)
5. Item returned to consumer

Commits and aborts also write synchronously — the state transitions are small and SQLite WAL mode handles concurrent writers efficiently. No batching needed for these operations.

Enqueue can optionally batch (high-throughput burst scenarios), but defaults to synchronous for simplicity.

### Async Dequeue (Blocking Wait)

`DequeueAsync` blocks until an item becomes available, using a `SemaphoreSlim` signaled on enqueue — no polling. This eliminates the spin-loop problem where consumers busy-wait on an empty queue.

Flow:
1. Consumer calls `await DequeueAsync(queueName, strategy, cancellationToken)`
2. If pending items exist → immediate checkout (same path as sync `Dequeue`)
3. If queue is empty → consumer awaits a per-queue `SemaphoreSlim`
4. When `Enqueue` adds an item → `SemaphoreSlim.Release()` wakes one waiting consumer
5. Woken consumer performs checkout as normal

`EnqueueBatch` releases the semaphore N times (once per item). Cancellation token allows clean shutdown without orphaned waiters.

### Dequeue Strategies

#### `IDequeueStrategy`

```csharp
public interface IDequeueStrategy
{
    /// <summary>
    /// Selects the next item to dequeue from the available pending items.
    /// Called under lock — implementation must be fast and non-blocking.
    /// </summary>
    QueueItem? SelectNext(QueueState state, DequeueContext context);
}
```

`DequeueContext` carries per-consumer state (e.g., last group processed, consumer ID) so strategies can make consumer-aware decisions.

#### Built-in Strategies

**FifoStrategy** — Default. Picks the highest-priority item with the lowest ID (insertion order). Ignores group field entirely. This is the baseline — equivalent to what LiteQueue does today.

**RoundRobinStrategy** — Cycles through distinct values of the group field. Within each group, picks by priority then insertion order. When a group is exhausted, it's removed from the rotation. Falls back to FIFO when all remaining items share the same group (no point rotating over one value).

The group set is re-evaluated on every `SelectNext` call — if a new group appears mid-drain (e.g., Google URLs enqueued while 5,000 JPMorgan items are still processing), the next call picks it up immediately. No stale rotation cache.

```csharp
var strategy = new RoundRobinStrategy(); // uses the group_key set at enqueue time
```

### Crash Recovery

On `StratQueue` initialization:

1. Read all rows from `queue_items` into memory
2. Items with `state = 'pending'` → add to `PendingItems`
3. Items with `state = 'checked_out'` → either:
   - Reset to `pending` with `attempts++` (default behavior — assumes the checkout was lost)
   - Keep as `checked_out` with a configurable timeout (consumer can reclaim or let it expire)
4. Items with `state = 'dead_letter'` → add to dead-letter index
5. Rebuild `GroupIndex` from pending items

The recovery policy is configurable at construction time.

---

## Public API Surface

```csharp
namespace StratQueue;

// --- Core ---

public class StratQueueClient : IDisposable
{
    // Construction
    public StratQueueClient(string sqlitePath, StratQueueOptions? options = null);

    // Enqueue
    public QueueItem Enqueue(string queueName, string payload, EnqueueOptions? options = null);
    public IReadOnlyList<QueueItem> EnqueueBatch(string queueName, IEnumerable<EnqueueRequest> items);

    // Dequeue
    public CheckedOutItem? Dequeue(string queueName, IDequeueStrategy? strategy = null);
    public Task<CheckedOutItem> DequeueAsync(string queueName, IDequeueStrategy? strategy = null, CancellationToken cancellationToken = default);

    // Lifecycle
    public void Commit(string checkoutId);
    public void Abort(string checkoutId, string? error = null);

    // Inspection
    public QueueItem? Peek(string queueName, IDequeueStrategy? strategy = null);
    public int Count(string queueName, ItemState? state = null);
    public IReadOnlyList<string> GetQueueNames();
    public IReadOnlyList<QueueItem> List(string queueName, ListOptions? options = null);

    // Dead letter
    public IReadOnlyList<QueueItem> GetDeadLetterItems(string queueName, int limit = 100, int offset = 0);
    public void Retry(string itemId);         // re-enqueue from dead letter, reset attempts
    public void RetryAll(string queueName);

    // Maintenance
    public void Purge(string queueName);       // delete all items in a queue
    public void PurgeDeadLetter(string queueName);
}

// --- Models ---

public record QueueItem
{
    public string Id { get; init; }
    public string QueueName { get; init; }
    public string Payload { get; init; }      // JSON string
    public ItemState State { get; init; }
    public int Priority { get; init; }
    public string? GroupKey { get; init; }
    public int Attempts { get; init; }
    public int MaxRetries { get; init; }
    public DateTime EnqueuedAt { get; init; }
    public DateTime? CheckedOutAt { get; init; }
    public string? LastError { get; init; }
}

public record CheckedOutItem
{
    public string CheckoutId { get; init; }
    public QueueItem Item { get; init; }
}

public record EnqueueOptions
{
    public int Priority { get; init; } = 1;        // 0=low, 1=normal, 2=high
    public string? GroupKey { get; init; }
    public int MaxRetries { get; init; } = 3;
}

public record EnqueueRequest
{
    public string Payload { get; init; }
    public EnqueueOptions? Options { get; init; }
}

public record ListOptions
{
    public ItemState? State { get; init; }
    public int Limit { get; init; } = 100;
    public int Offset { get; init; } = 0;
}

public enum ItemState { Pending, CheckedOut, DeadLetter }

// --- Strategies ---

public interface IDequeueStrategy
{
    QueueItem? SelectNext(QueueState state, DequeueContext context);
}

public class FifoStrategy : IDequeueStrategy { }
public class RoundRobinStrategy : IDequeueStrategy { }

// --- Configuration ---

public record StratQueueOptions
{
    /// <summary>
    /// What to do with checked-out items found during recovery.
    /// Default: ResetToPending.
    /// </summary>
    public RecoveryPolicy RecoveryPolicy { get; init; } = RecoveryPolicy.ResetToPending;

    /// <summary>
    /// Enable WAL mode on the SQLite connection. Default: true.
    /// </summary>
    public bool EnableWalMode { get; init; } = true;
}

public enum RecoveryPolicy { ResetToPending, LeaveCheckedOut }
```

---

## Design Constraints & Known Limits

**In-memory footprint** — All pending items are loaded into memory on startup. At ~1KB per item (payload + C# object overhead), 100K items ≈ 100MB. This is fine for the target use case (tens of thousands of items). If millions of items are needed, a working-set approach (load first N per group, lazy-hydrate from SQLite as the buffer drains) could be added as a future optimization — not worth designing for now.

**SQLite single-writer** — WAL mode allows concurrent reads, but writes are serialized. With consumers doing network I/O (seconds per item), the microsecond write lock for checkout/commit is invisible. If consumers are doing fast CPU-bound work, this could become a bottleneck — but that's not the target use case.

**Deferred optimizations** — Batch commit (`CommitBatch(IEnumerable<string>)`) and `byte[]` payload (avoiding string allocation for consumers that immediately deserialize) are both reasonable but deferred. String payload keeps the API simple for now.

---

## Phase 1 — Core Library

**Goal**: Working NuGet package with FIFO + round-robin strategies, SQLite persistence, full test coverage.

### 1.1 — Project scaffolding
- Solution structure mirroring Janet-CSharp: `src/StratQueue.Core/`, `tests/StratQueue.Tests/`
- `Directory.Build.props` — .NET 9, nullable, latest C#, version `0.1.0`
- `StratQueue.Core.csproj` — NuGet metadata, `Microsoft.Data.Sqlite` dependency
- `StratQueue.Tests.csproj` — xUnit, project reference
- `StratQueue.sln`
- `CLAUDE.md` — build/test instructions for AI coding

### 1.2 — SQLite journal layer
- `SqliteJournal.cs` — schema creation, CRUD operations, WAL mode
- All SQL in one file — no ORM, no abstraction layers
- Parameterized queries throughout (no string interpolation in SQL)
- Methods: `Insert`, `UpdateState`, `Delete`, `LoadAll`, `DeleteByQueue`

### 1.3 — In-memory state management
- `QueueState.cs` — per-queue in-memory structures (pending items by priority, checked-out tracking, group index)
- `QueueManager.cs` — thread-safe state transitions under `SemaphoreSlim`, delegates persistence to `SqliteJournal`
- Crash recovery: load from SQLite, apply recovery policy, rebuild indexes

### 1.4 — Dequeue strategies
- `IDequeueStrategy.cs` — interface + `DequeueContext`
- `FifoStrategy.cs` — priority then insertion order
- `RoundRobinStrategy.cs` — cycle across group key values, priority within group, fallback to FIFO on single group

### 1.5 — Public API
- `StratQueueClient.cs` — the single entry point, wires `QueueManager` + `SqliteJournal`
- Enqueue, Dequeue, DequeueAsync, Commit, Abort, Peek, Count, List, dead letter operations, Purge
- `EnqueueBatch` — bulk insert in a single SQLite transaction
- `DequeueAsync` — `SemaphoreSlim`-based blocking wait, signaled by `Enqueue`/`EnqueueBatch`
- `IDisposable` — disposes SQLite connection, cancels any pending `DequeueAsync` waiters

### 1.6 — Tests
- **Journal tests** — SQLite round-trip: insert, load, update state, delete
- **FIFO tests** — priority ordering, insertion order within same priority, empty queue returns null
- **Round-robin tests** — group interleaving, group exhaustion, single-group fallback, mixed priorities across groups
- **Concurrency tests** — multiple threads dequeueing simultaneously, no double-checkout
- **Async dequeue tests** — DequeueAsync blocks on empty queue, wakes on enqueue, cancellation token works, multiple waiters wake in order
- **Recovery tests** — simulate crash (dispose without commit), reopen, verify checked-out items are reset
- **Dead letter tests** — abort past max retries, retry re-enqueues, purge
- **Batch tests** — enqueue batch, verify ordering

### 1.7 — Documentation + packaging
- `README.md` — problem statement, quick start, API overview, strategy examples
- NuGet package metadata in `.csproj`
- `PackageReadmeFile` pointing to README.md

---

## Phase 2 — CI/CD + Publish

### 2.1 — GitHub Actions
- Build + test workflow (Windows + Linux matrix)
- Pack workflow — version from tag or `0.1.0-ci.{run_number}`
- Publish workflow — conditional on version tags, push to NuGet.org

### 2.2 — First NuGet release
- Tag `v0.1.0`, verify pipeline, confirm package on nuget.org

---

## Phase 3 — Future Strategies (Deferred)

Ideas for additional strategies beyond the initial two. Not planned, just captured:

- **WeightedRoundRobin** — groups get proportional share (e.g., small companies get more turns per cycle than large ones, preventing starvation)
- **CooldownStrategy** — enforce a minimum time gap between items from the same group (explicit rate limiting per domain)
- **RandomStrategy** — random selection from pending items (useful when order doesn't matter but you want to avoid patterns)
- **ShuffledRoundRobin** — round-robin but the group order is randomized each cycle (prevents predictable patterns)
- **Consumer-aware strategies** — different consumers get different group assignments (worker A handles domains X,Y; worker B handles Z,W)

---

## Dependencies

| Package | Purpose | License |
|---------|---------|---------|
| `Microsoft.Data.Sqlite` | SQLite access | MIT |
| `xunit` | Test framework | Apache-2.0 |
| `Microsoft.NET.Test.Sdk` | Test runner | MIT |

That's it. Deliberately minimal.
