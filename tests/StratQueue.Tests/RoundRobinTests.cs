using Microsoft.Data.Sqlite;
using StratQueue;
using Xunit;

namespace StratQueue.Tests;

public class RoundRobinTests : IDisposable
{
    private readonly string _dbPath;
    private readonly StratQueueClient _client;
    private readonly RoundRobinStrategy _strategy = new();

    public RoundRobinTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"stratqueue_test_{Guid.NewGuid():N}.db");
        _client = new StratQueueClient(_dbPath);
    }

    public void Dispose()
    {
        _client.Dispose();
        SqliteConnection.ClearAllPools();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        if (File.Exists(_dbPath + "-wal")) File.Delete(_dbPath + "-wal");
        if (File.Exists(_dbPath + "-shm")) File.Delete(_dbPath + "-shm");
    }

    [Fact]
    public void Cycles_through_groups_A_B_C()
    {
        // Enqueue: 3 items per group
        for (int i = 0; i < 3; i++)
        {
            _client.Enqueue("q", $"A{i}", new EnqueueOptions { GroupKey = "A" });
            _client.Enqueue("q", $"B{i}", new EnqueueOptions { GroupKey = "B" });
            _client.Enqueue("q", $"C{i}", new EnqueueOptions { GroupKey = "C" });
        }

        // First cycle: A, B, C
        var r1 = _client.Dequeue("q", _strategy)!;
        var r2 = _client.Dequeue("q", _strategy)!;
        var r3 = _client.Dequeue("q", _strategy)!;
        _client.Commit(r1.CheckoutId);
        _client.Commit(r2.CheckoutId);
        _client.Commit(r3.CheckoutId);

        Assert.Equal("A", r1.Item.GroupKey);
        Assert.Equal("B", r2.Item.GroupKey);
        Assert.Equal("C", r3.Item.GroupKey);

        // Second cycle: A, B, C again
        var r4 = _client.Dequeue("q", _strategy)!;
        var r5 = _client.Dequeue("q", _strategy)!;
        var r6 = _client.Dequeue("q", _strategy)!;
        _client.Commit(r4.CheckoutId);
        _client.Commit(r5.CheckoutId);
        _client.Commit(r6.CheckoutId);

        Assert.Equal("A", r4.Item.GroupKey);
        Assert.Equal("B", r5.Item.GroupKey);
        Assert.Equal("C", r6.Item.GroupKey);
    }

    [Fact]
    public void Group_exhaustion_removes_group_from_rotation()
    {
        _client.Enqueue("q", "A1", new EnqueueOptions { GroupKey = "A" });
        _client.Enqueue("q", "A2", new EnqueueOptions { GroupKey = "A" });
        _client.Enqueue("q", "B1", new EnqueueOptions { GroupKey = "B" }); // Only 1 item in B
        _client.Enqueue("q", "C1", new EnqueueOptions { GroupKey = "C" });
        _client.Enqueue("q", "C2", new EnqueueOptions { GroupKey = "C" });

        // First cycle: A, B, C
        var r1 = _client.Dequeue("q", _strategy)!;
        var r2 = _client.Dequeue("q", _strategy)!;
        var r3 = _client.Dequeue("q", _strategy)!;
        _client.Commit(r1.CheckoutId);
        _client.Commit(r2.CheckoutId);
        _client.Commit(r3.CheckoutId);

        Assert.Equal("A", r1.Item.GroupKey);
        Assert.Equal("B", r2.Item.GroupKey);
        Assert.Equal("C", r3.Item.GroupKey);

        // B is exhausted — next cycle should be A, C
        var r4 = _client.Dequeue("q", _strategy)!;
        var r5 = _client.Dequeue("q", _strategy)!;
        _client.Commit(r4.CheckoutId);
        _client.Commit(r5.CheckoutId);

        Assert.Equal("A", r4.Item.GroupKey);
        Assert.Equal("C", r5.Item.GroupKey);
    }

    [Fact]
    public void Single_group_remaining_falls_back_to_fifo()
    {
        _client.Enqueue("q", "A1", new EnqueueOptions { GroupKey = "A" });
        _client.Enqueue("q", "A2", new EnqueueOptions { GroupKey = "A" });
        _client.Enqueue("q", "A3", new EnqueueOptions { GroupKey = "A" });

        var r1 = _client.Dequeue("q", _strategy)!;
        var r2 = _client.Dequeue("q", _strategy)!;
        var r3 = _client.Dequeue("q", _strategy)!;

        Assert.Equal("A1", r1.Item.Payload);
        Assert.Equal("A2", r2.Item.Payload);
        Assert.Equal("A3", r3.Item.Payload);
    }

    [Fact]
    public void Mixed_priorities_within_groups()
    {
        _client.Enqueue("q", "A-low", new EnqueueOptions { GroupKey = "A", Priority = 0 });
        _client.Enqueue("q", "A-high", new EnqueueOptions { GroupKey = "A", Priority = 2 });
        _client.Enqueue("q", "B-normal", new EnqueueOptions { GroupKey = "B", Priority = 1 });
        _client.Enqueue("q", "B-high", new EnqueueOptions { GroupKey = "B", Priority = 2 });

        var r1 = _client.Dequeue("q", _strategy)!;
        var r2 = _client.Dequeue("q", _strategy)!;
        _client.Commit(r1.CheckoutId);
        _client.Commit(r2.CheckoutId);

        // Should pick highest priority from each group
        Assert.Equal("A-high", r1.Item.Payload);
        Assert.Equal("B-high", r2.Item.Payload);
    }

    [Fact]
    public void New_group_mid_drain_immediately_included()
    {
        // Start with only group A
        _client.Enqueue("q", "A1", new EnqueueOptions { GroupKey = "A" });
        _client.Enqueue("q", "A2", new EnqueueOptions { GroupKey = "A" });
        _client.Enqueue("q", "A3", new EnqueueOptions { GroupKey = "A" });

        var r1 = _client.Dequeue("q", _strategy)!;
        _client.Commit(r1.CheckoutId);
        Assert.Equal("A", r1.Item.GroupKey);

        // Now add group B mid-drain
        _client.Enqueue("q", "B1", new EnqueueOptions { GroupKey = "B" });

        // Next dequeue should pick B (new group in rotation)
        var r2 = _client.Dequeue("q", _strategy)!;
        _client.Commit(r2.CheckoutId);
        Assert.Equal("B", r2.Item.GroupKey);

        // Then back to A
        var r3 = _client.Dequeue("q", _strategy)!;
        _client.Commit(r3.CheckoutId);
        Assert.Equal("A", r3.Item.GroupKey);
    }

    [Fact]
    public void Items_with_no_group_key_treated_as_single_group()
    {
        _client.Enqueue("q", "ungrouped1");
        _client.Enqueue("q", "ungrouped2");
        _client.Enqueue("q", "ungrouped3");

        // All in same "" group → FIFO
        var r1 = _client.Dequeue("q", _strategy)!;
        var r2 = _client.Dequeue("q", _strategy)!;
        var r3 = _client.Dequeue("q", _strategy)!;

        Assert.Equal("ungrouped1", r1.Item.Payload);
        Assert.Equal("ungrouped2", r2.Item.Payload);
        Assert.Equal("ungrouped3", r3.Item.Payload);
    }
}
