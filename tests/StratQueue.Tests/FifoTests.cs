using Microsoft.Data.Sqlite;
using StratQueue;
using Xunit;

namespace StratQueue.Tests;

public class FifoTests : IDisposable
{
    private readonly string _dbPath;
    private readonly StratQueueClient _client;

    public FifoTests()
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
    public void Enqueue_and_Dequeue_returns_item()
    {
        _client.Enqueue("jobs", """{"url":"https://example.com"}""");

        var result = _client.Dequeue("jobs");

        Assert.NotNull(result);
        Assert.Equal("""{"url":"https://example.com"}""", result.Item.Payload);
        Assert.Equal("jobs", result.Item.QueueName);
    }

    [Fact]
    public void Dequeue_on_empty_queue_returns_null()
    {
        var result = _client.Dequeue("jobs");
        Assert.Null(result);
    }

    [Fact]
    public void Priority_ordering_high_before_normal_before_low()
    {
        _client.Enqueue("jobs", "low", new EnqueueOptions { Priority = 0 });
        _client.Enqueue("jobs", "normal", new EnqueueOptions { Priority = 1 });
        _client.Enqueue("jobs", "high", new EnqueueOptions { Priority = 2 });

        var first = _client.Dequeue("jobs");
        var second = _client.Dequeue("jobs");
        var third = _client.Dequeue("jobs");

        Assert.Equal("high", first!.Item.Payload);
        Assert.Equal("normal", second!.Item.Payload);
        Assert.Equal("low", third!.Item.Payload);
    }

    [Fact]
    public void Same_priority_preserves_insertion_order()
    {
        _client.Enqueue("jobs", "first");
        _client.Enqueue("jobs", "second");
        _client.Enqueue("jobs", "third");

        Assert.Equal("first", _client.Dequeue("jobs")!.Item.Payload);
        Assert.Equal("second", _client.Dequeue("jobs")!.Item.Payload);
        Assert.Equal("third", _client.Dequeue("jobs")!.Item.Payload);
    }

    [Fact]
    public void Dequeue_removes_item_from_pending()
    {
        _client.Enqueue("jobs", "only-one");

        var first = _client.Dequeue("jobs");
        Assert.NotNull(first);

        var second = _client.Dequeue("jobs");
        Assert.Null(second);
    }

    [Fact]
    public void Commit_deletes_item_and_count_drops()
    {
        _client.Enqueue("jobs", "payload");
        Assert.Equal(1, _client.Count("jobs"));

        var result = _client.Dequeue("jobs");
        // Count of pending should be 0 after dequeue (item is checked out)
        Assert.Equal(0, _client.Count("jobs", ItemState.Pending));

        _client.Commit(result!.CheckoutId);
        // Total count should be 0 after commit
        Assert.Equal(0, _client.Count("jobs"));
    }

    [Fact]
    public void Peek_returns_item_without_checking_out()
    {
        _client.Enqueue("jobs", "peek-me");

        var peeked = _client.Peek("jobs");
        Assert.NotNull(peeked);
        Assert.Equal("peek-me", peeked.Payload);

        // Item should still be available for dequeue
        var dequeued = _client.Dequeue("jobs");
        Assert.NotNull(dequeued);
        Assert.Equal("peek-me", dequeued.Item.Payload);
    }

    [Fact]
    public void Enqueue_sets_metadata_correctly()
    {
        var item = _client.Enqueue("jobs", "payload", new EnqueueOptions
        {
            Priority = 2,
            GroupKey = "example.com",
            MaxRetries = 5
        });

        Assert.Equal(2, item.Priority);
        Assert.Equal("example.com", item.GroupKey);
        Assert.Equal(5, item.MaxRetries);
        Assert.Equal(ItemState.Pending, item.State);
        Assert.Equal(0, item.Attempts);
    }
}
