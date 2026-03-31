using Microsoft.Data.Sqlite;
using StratQueue;
using Xunit;

namespace StratQueue.Tests;

public class AsyncDequeueTests : IDisposable
{
    private readonly string _dbPath;
    private readonly StratQueueClient _client;

    public AsyncDequeueTests()
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
    public async Task DequeueAsync_returns_immediately_when_items_available()
    {
        _client.Enqueue("jobs", "payload");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var result = await _client.DequeueAsync("jobs", cancellationToken: cts.Token);

        Assert.Equal("payload", result.Item.Payload);
    }

    [Fact]
    public async Task DequeueAsync_blocks_until_enqueue()
    {
        var dequeueTask = _client.DequeueAsync("jobs");

        // Give it a moment to start waiting
        await Task.Delay(50);
        Assert.False(dequeueTask.IsCompleted);

        // Now enqueue
        _client.Enqueue("jobs", "arrived");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var result = await dequeueTask.WaitAsync(cts.Token);

        Assert.Equal("arrived", result.Item.Payload);
    }

    [Fact]
    public async Task Multiple_waiters_each_enqueue_wakes_one()
    {
        var t1 = _client.DequeueAsync("jobs");
        var t2 = _client.DequeueAsync("jobs");
        var t3 = _client.DequeueAsync("jobs");

        await Task.Delay(50);
        Assert.False(t1.IsCompleted);
        Assert.False(t2.IsCompleted);
        Assert.False(t3.IsCompleted);

        _client.Enqueue("jobs", "first");
        _client.Enqueue("jobs", "second");
        _client.Enqueue("jobs", "third");

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var results = await Task.WhenAll(
            t1.WaitAsync(cts.Token),
            t2.WaitAsync(cts.Token),
            t3.WaitAsync(cts.Token));

        var payloads = results.Select(r => r.Item.Payload).OrderBy(p => p).ToList();
        Assert.Equal(["first", "second", "third"], payloads);
    }

    [Fact]
    public async Task CancellationToken_cancels_the_wait()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));

        await Assert.ThrowsAsync<OperationCanceledException>(
            () => _client.DequeueAsync("jobs", cancellationToken: cts.Token));
    }

    [Fact]
    public async Task EnqueueBatch_wakes_multiple_waiters()
    {
        var t1 = _client.DequeueAsync("jobs");
        var t2 = _client.DequeueAsync("jobs");

        await Task.Delay(50);

        _client.EnqueueBatch("jobs", [
            new EnqueueRequest { Payload = "batch1" },
            new EnqueueRequest { Payload = "batch2" }
        ]);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var results = await Task.WhenAll(
            t1.WaitAsync(cts.Token),
            t2.WaitAsync(cts.Token));

        Assert.Equal(2, results.Length);
    }

    [Fact]
    public async Task Dispose_cancels_pending_waiters()
    {
        var dequeueTask = _client.DequeueAsync("jobs");
        await Task.Delay(50);

        _client.Dispose();

        // The task should eventually throw or complete
        // (SemaphoreSlim.WaitAsync throws ObjectDisposedException when disposed)
        await Assert.ThrowsAnyAsync<Exception>(() => dequeueTask.WaitAsync(TimeSpan.FromSeconds(2)));
    }
}
