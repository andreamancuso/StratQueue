using Microsoft.Data.Sqlite;
using StratQueue;
using Xunit;

namespace StratQueue.Tests;

public class BatchTests : IDisposable
{
    private readonly string _dbPath;
    private readonly StratQueueClient _client;

    public BatchTests()
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
    public void EnqueueBatch_adds_all_items()
    {
        var items = Enumerable.Range(0, 10).Select(i => new EnqueueRequest
        {
            Payload = $"item-{i}"
        }).ToList();

        var result = _client.EnqueueBatch("jobs", items);

        Assert.Equal(10, result.Count);
        Assert.Equal(10, _client.Count("jobs"));
    }

    [Fact]
    public void EnqueueBatch_maintains_insertion_order()
    {
        var items = Enumerable.Range(0, 5).Select(i => new EnqueueRequest
        {
            Payload = $"item-{i}"
        }).ToList();

        _client.EnqueueBatch("jobs", items);

        for (int i = 0; i < 5; i++)
        {
            var dequeued = _client.Dequeue("jobs")!;
            Assert.Equal($"item-{i}", dequeued.Item.Payload);
            _client.Commit(dequeued.CheckoutId);
        }
    }

    [Fact]
    public void GetQueueNames_returns_distinct_names()
    {
        _client.Enqueue("alpha", "a");
        _client.Enqueue("beta", "b");
        _client.Enqueue("alpha", "a2");

        var names = _client.GetQueueNames().OrderBy(n => n).ToList();
        Assert.Equal(["alpha", "beta"], names);
    }

    [Fact]
    public void List_with_state_filter()
    {
        _client.Enqueue("jobs", "pending-item");
        _client.Enqueue("jobs", "to-checkout");
        var co = _client.Dequeue("jobs")!;

        var pendingItems = _client.List("jobs", new ListOptions { State = ItemState.Pending });
        var checkedOutItems = _client.List("jobs", new ListOptions { State = ItemState.CheckedOut });

        Assert.Single(pendingItems);
        Assert.Single(checkedOutItems);
    }

    [Fact]
    public void List_with_limit_and_offset()
    {
        for (int i = 0; i < 10; i++)
            _client.Enqueue("jobs", $"item-{i}");

        var page1 = _client.List("jobs", new ListOptions { Limit = 3, Offset = 0 });
        var page2 = _client.List("jobs", new ListOptions { Limit = 3, Offset = 3 });

        Assert.Equal(3, page1.Count);
        Assert.Equal(3, page2.Count);
        Assert.NotEqual(page1[0].Id, page2[0].Id);
    }

    [Fact]
    public void Purge_removes_all_items()
    {
        _client.Enqueue("jobs", "a");
        _client.Enqueue("jobs", "b");
        _client.Enqueue("other", "c");

        _client.Purge("jobs");

        Assert.Equal(0, _client.Count("jobs"));
        Assert.Equal(1, _client.Count("other")); // Unaffected
    }

    [Fact]
    public void Count_with_state_filter()
    {
        _client.Enqueue("jobs", "a");
        _client.Enqueue("jobs", "b");
        var co = _client.Dequeue("jobs")!;

        Assert.Equal(2, _client.Count("jobs")); // Total
        Assert.Equal(1, _client.Count("jobs", ItemState.Pending));
        Assert.Equal(1, _client.Count("jobs", ItemState.CheckedOut));
        Assert.Equal(0, _client.Count("jobs", ItemState.DeadLetter));
    }
}
