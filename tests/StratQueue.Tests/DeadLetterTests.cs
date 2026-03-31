using Microsoft.Data.Sqlite;
using StratQueue;
using Xunit;

namespace StratQueue.Tests;

public class DeadLetterTests : IDisposable
{
    private readonly string _dbPath;
    private readonly StratQueueClient _client;

    public DeadLetterTests()
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
    public void Abort_with_attempts_below_maxRetries_returns_to_pending()
    {
        _client.Enqueue("jobs", "payload", new EnqueueOptions { MaxRetries = 3 });
        var item = _client.Dequeue("jobs")!;

        _client.Abort(item.CheckoutId, "failed");

        Assert.Equal(1, _client.Count("jobs", ItemState.Pending));
        Assert.Equal(0, _client.Count("jobs", ItemState.DeadLetter));
    }

    [Fact]
    public void Abort_increments_attempt_count()
    {
        _client.Enqueue("jobs", "payload", new EnqueueOptions { MaxRetries = 5 });
        var item = _client.Dequeue("jobs")!;
        _client.Abort(item.CheckoutId);

        var peeked = _client.Peek("jobs");
        Assert.Equal(1, peeked!.Attempts);
    }

    [Fact]
    public void Abort_past_maxRetries_moves_to_dead_letter()
    {
        _client.Enqueue("jobs", "payload", new EnqueueOptions { MaxRetries = 2 });

        // First attempt
        var item1 = _client.Dequeue("jobs")!;
        _client.Abort(item1.CheckoutId, "error 1");

        // Second attempt — should dead-letter (attempts will be 2 >= maxRetries 2)
        var item2 = _client.Dequeue("jobs")!;
        _client.Abort(item2.CheckoutId, "error 2");

        Assert.Equal(0, _client.Count("jobs", ItemState.Pending));
        Assert.Equal(1, _client.Count("jobs", ItemState.DeadLetter));
    }

    [Fact]
    public void Abort_stores_error_message()
    {
        _client.Enqueue("jobs", "payload", new EnqueueOptions { MaxRetries = 1 });
        var item = _client.Dequeue("jobs")!;
        _client.Abort(item.CheckoutId, "connection timeout");

        var deadItems = _client.GetDeadLetterItems("jobs");
        Assert.Single(deadItems);
        Assert.Equal("connection timeout", deadItems[0].LastError);
    }

    [Fact]
    public void GetDeadLetterItems_returns_dead_lettered_items()
    {
        _client.Enqueue("jobs", "a", new EnqueueOptions { MaxRetries = 1 });
        _client.Enqueue("jobs", "b", new EnqueueOptions { MaxRetries = 1 });

        var item1 = _client.Dequeue("jobs")!;
        _client.Abort(item1.CheckoutId, "err");

        var item2 = _client.Dequeue("jobs")!;
        _client.Abort(item2.CheckoutId, "err");

        var deadItems = _client.GetDeadLetterItems("jobs");
        Assert.Equal(2, deadItems.Count);
    }

    [Fact]
    public void Retry_moves_from_dead_letter_to_pending()
    {
        _client.Enqueue("jobs", "payload", new EnqueueOptions { MaxRetries = 1 });
        var item = _client.Dequeue("jobs")!;
        _client.Abort(item.CheckoutId, "err");

        var deadItems = _client.GetDeadLetterItems("jobs");
        Assert.Single(deadItems);

        _client.Retry(deadItems[0].Id);

        Assert.Equal(0, _client.Count("jobs", ItemState.DeadLetter));
        Assert.Equal(1, _client.Count("jobs", ItemState.Pending));

        // Attempts should be reset
        var peeked = _client.Peek("jobs");
        Assert.Equal(0, peeked!.Attempts);
    }

    [Fact]
    public void RetryAll_re_enqueues_all_dead_letter_items()
    {
        _client.Enqueue("jobs", "a", new EnqueueOptions { MaxRetries = 1 });
        _client.Enqueue("jobs", "b", new EnqueueOptions { MaxRetries = 1 });

        var item1 = _client.Dequeue("jobs")!;
        _client.Abort(item1.CheckoutId);
        var item2 = _client.Dequeue("jobs")!;
        _client.Abort(item2.CheckoutId);

        Assert.Equal(2, _client.Count("jobs", ItemState.DeadLetter));

        _client.RetryAll("jobs");

        Assert.Equal(0, _client.Count("jobs", ItemState.DeadLetter));
        Assert.Equal(2, _client.Count("jobs", ItemState.Pending));
    }

    [Fact]
    public void PurgeDeadLetter_clears_dead_letter_items()
    {
        _client.Enqueue("jobs", "payload", new EnqueueOptions { MaxRetries = 1 });
        var item = _client.Dequeue("jobs")!;
        _client.Abort(item.CheckoutId);

        Assert.Equal(1, _client.Count("jobs", ItemState.DeadLetter));

        _client.PurgeDeadLetter("jobs");

        Assert.Equal(0, _client.Count("jobs", ItemState.DeadLetter));
    }
}
