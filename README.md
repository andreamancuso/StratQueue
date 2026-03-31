# StratQueue

An in-memory work queue with pluggable dequeue strategies and SQLite persistence for .NET. No external infrastructure required.

## The Problem

Embedded persistent queues in .NET (LiteQueue, DiskQueue, etc.) are all FIFO-only. They have no concept of *what* they're dequeueing — just *when* it was enqueued. This breaks down when queue items belong to logical groups that need fair scheduling. The alternative is heavyweight distributed brokers (RabbitMQ, Kafka) that require external infrastructure you may not need.

StratQueue fills the gap: an embedded, single-process queue where you control the dequeue order.

## Features

- **Pluggable dequeue strategies** — FIFO, round-robin, or implement your own `IDequeueStrategy`
- **Group-aware scheduling** — round-robin across a field (e.g., domain name) so one burst doesn't starve other groups
- **SQLite persistence** — WAL mode, crash recovery rebuilds in-memory state on startup
- **Async dequeue** — `DequeueAsync` blocks until an item is available (no polling)
- **Thread-safe** — multiple concurrent consumers, no double-checkout
- **Checkout/commit/abort lifecycle** — with automatic dead-lettering after max retries
- **Batch enqueue** — insert thousands of items in a single SQLite transaction
- **Zero external dependencies** — just `Microsoft.Data.Sqlite`. No servers, no background services

## Quick Start

```csharp
using StratQueue;

using var queue = new StratQueueClient("myapp-queue.db");

// Enqueue items with group keys
queue.Enqueue("jobs", """{"url":"https://google.com/job/1"}""",
    new EnqueueOptions { GroupKey = "google.com" });
queue.Enqueue("jobs", """{"url":"https://google.com/job/2"}""",
    new EnqueueOptions { GroupKey = "google.com" });
queue.Enqueue("jobs", """{"url":"https://meta.com/job/1"}""",
    new EnqueueOptions { GroupKey = "meta.com" });

// Dequeue with round-robin — cycles across groups instead of FIFO
var strategy = new RoundRobinStrategy();

var item1 = queue.Dequeue("jobs", strategy); // google.com/job/1
var item2 = queue.Dequeue("jobs", strategy); // meta.com/job/1
var item3 = queue.Dequeue("jobs", strategy); // google.com/job/2

// Commit when done, abort on failure
queue.Commit(item1!.CheckoutId);
queue.Commit(item2!.CheckoutId);
queue.Abort(item3!.CheckoutId, "connection timeout"); // Returns to pending
```

## Async Consumers

`DequeueAsync` blocks until an item is available — no polling loops, no `Thread.Sleep`.

```csharp
using var cts = new CancellationTokenSource();

// Consumer blocks until work arrives, wakes immediately on enqueue
while (!cts.Token.IsCancellationRequested)
{
    var item = await queue.DequeueAsync("jobs", new RoundRobinStrategy(), cts.Token);
    try
    {
        await ProcessAsync(item.Item.Payload);
        queue.Commit(item.CheckoutId);
    }
    catch (Exception ex)
    {
        queue.Abort(item.CheckoutId, ex.Message);
    }
}
```

## Installation

```bash
dotnet add package StratQueue
```

Requires .NET 9.0+.

### Build from Source

```bash
git clone https://github.com/andreamancuso/StratQueue.git
cd StratQueue
dotnet build StratQueue.sln
dotnet test StratQueue.sln
```

## API Overview

| Type | Description |
|------|-------------|
| `StratQueueClient` | Main entry point. Enqueue, dequeue, commit, abort, inspect. `IDisposable`. |
| `QueueItem` | An item with its metadata: payload, state, priority, group key, attempts. |
| `CheckedOutItem` | Returned by `Dequeue` — wraps the item with a checkout ID for commit/abort. |
| `EnqueueOptions` | Priority (0-2), group key, max retries. |
| `IDequeueStrategy` | Interface for custom dequeue logic. Receives a read-only state snapshot. |
| `FifoStrategy` | Default. Highest priority first, then insertion order. |
| `RoundRobinStrategy` | Cycles through distinct group key values. Priority within each group. |
| `StratQueueOptions` | Client configuration: recovery policy, WAL mode toggle. |
| `ItemState` | Enum: `Pending`, `CheckedOut`, `DeadLetter`. |

## How It Works

All scheduling decisions happen in C# using in-memory data structures. SQLite is the persistence layer — a write-ahead journal for crash recovery, not the orchestration engine. Dequeue strategies receive a read-only snapshot of the queue state and select the next item purely in C#. On startup, all items are loaded from SQLite into memory; checked-out items are reset to pending (configurable). State transitions (checkout, commit, abort) are written synchronously to SQLite before returning to the caller.

## Custom Strategies

Implement `IDequeueStrategy` to define your own dequeue logic:

```csharp
public class HighPriorityOnlyStrategy : IDequeueStrategy
{
    public QueueItem? SelectNext(QueueState state, DequeueContext context)
    {
        // Only dequeue high-priority items, skip everything else
        if (state.PendingByPriority.TryGetValue(2, out var items) && items.Count > 0)
            return items[0];
        return null;
    }
}
```

The `QueueState` snapshot exposes `PendingByPriority` (items grouped by priority level) and `GroupIndex` (items grouped by group key), giving strategies full visibility into the queue without mutating it.

## Roadmap

See [ROADMAP.md](ROADMAP.md) for the full design document and development plan.

## License

[MIT](LICENSE)
