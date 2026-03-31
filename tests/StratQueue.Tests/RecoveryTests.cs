using Microsoft.Data.Sqlite;
using StratQueue;
using Xunit;

namespace StratQueue.Tests;

public class RecoveryTests
{
    private static string NewDbPath() =>
        Path.Combine(Path.GetTempPath(), $"stratqueue_test_{Guid.NewGuid():N}.db");

    private static void Cleanup(string dbPath)
    {
        SqliteConnection.ClearAllPools();
        if (File.Exists(dbPath)) File.Delete(dbPath);
        if (File.Exists(dbPath + "-wal")) File.Delete(dbPath + "-wal");
        if (File.Exists(dbPath + "-shm")) File.Delete(dbPath + "-shm");
    }

    [Fact]
    public void CheckedOut_items_reset_to_pending_on_recovery()
    {
        var dbPath = NewDbPath();
        try
        {
            // Enqueue and dequeue (no commit) — simulates crash
            using (var client = new StratQueueClient(dbPath))
            {
                client.Enqueue("jobs", "payload");
                var result = client.Dequeue("jobs");
                Assert.NotNull(result);
                // Don't commit — simulate crash
            }

            SqliteConnection.ClearAllPools();

            // Reopen — checked-out item should be back in pending
            using (var client = new StratQueueClient(dbPath))
            {
                Assert.Equal(1, client.Count("jobs", ItemState.Pending));
                var peeked = client.Peek("jobs");
                Assert.NotNull(peeked);
                Assert.Equal("payload", peeked.Payload);
                Assert.Equal(1, peeked.Attempts); // Incremented on recovery
            }
        }
        finally { Cleanup(dbPath); }
    }

    [Fact]
    public void Pending_items_survive_restart()
    {
        var dbPath = NewDbPath();
        try
        {
            using (var client = new StratQueueClient(dbPath))
            {
                client.Enqueue("jobs", "a");
                client.Enqueue("jobs", "b");
            }

            SqliteConnection.ClearAllPools();

            using (var client = new StratQueueClient(dbPath))
            {
                Assert.Equal(2, client.Count("jobs", ItemState.Pending));
                Assert.Equal("a", client.Dequeue("jobs")!.Item.Payload);
                Assert.Equal("b", client.Dequeue("jobs")!.Item.Payload);
            }
        }
        finally { Cleanup(dbPath); }
    }

    [Fact]
    public void DeadLetter_items_survive_restart()
    {
        var dbPath = NewDbPath();
        try
        {
            using (var client = new StratQueueClient(dbPath))
            {
                client.Enqueue("jobs", "payload", new EnqueueOptions { MaxRetries = 1 });
                var item = client.Dequeue("jobs")!;
                client.Abort(item.CheckoutId, "fatal");
            }

            SqliteConnection.ClearAllPools();

            using (var client = new StratQueueClient(dbPath))
            {
                Assert.Equal(0, client.Count("jobs", ItemState.Pending));
                Assert.Equal(1, client.Count("jobs", ItemState.DeadLetter));
                var dead = client.GetDeadLetterItems("jobs");
                Assert.Equal("fatal", dead[0].LastError);
            }
        }
        finally { Cleanup(dbPath); }
    }

    [Fact]
    public void GroupIndex_rebuilt_correctly_on_recovery()
    {
        var dbPath = NewDbPath();
        try
        {
            using (var client = new StratQueueClient(dbPath))
            {
                client.Enqueue("jobs", "A1", new EnqueueOptions { GroupKey = "A" });
                client.Enqueue("jobs", "B1", new EnqueueOptions { GroupKey = "B" });
                client.Enqueue("jobs", "A2", new EnqueueOptions { GroupKey = "A" });
            }

            SqliteConnection.ClearAllPools();

            using (var client = new StratQueueClient(dbPath))
            {
                var strategy = new RoundRobinStrategy();
                var r1 = client.Dequeue("jobs", strategy)!;
                var r2 = client.Dequeue("jobs", strategy)!;

                // Round-robin should alternate groups
                Assert.Equal("A", r1.Item.GroupKey);
                Assert.Equal("B", r2.Item.GroupKey);
            }
        }
        finally { Cleanup(dbPath); }
    }

    [Fact]
    public void LeaveCheckedOut_policy_keeps_items_checked_out()
    {
        var dbPath = NewDbPath();
        try
        {
            string checkoutId;
            using (var client = new StratQueueClient(dbPath))
            {
                client.Enqueue("jobs", "payload");
                var result = client.Dequeue("jobs")!;
                checkoutId = result.CheckoutId;
            }

            SqliteConnection.ClearAllPools();

            using (var client = new StratQueueClient(dbPath, new StratQueueOptions { RecoveryPolicy = RecoveryPolicy.LeaveCheckedOut }))
            {
                // Item should still be checked out, not pending
                Assert.Equal(0, client.Count("jobs", ItemState.Pending));
                Assert.Equal(1, client.Count("jobs", ItemState.CheckedOut));
            }
        }
        finally { Cleanup(dbPath); }
    }
}
