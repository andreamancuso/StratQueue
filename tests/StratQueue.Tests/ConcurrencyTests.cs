using System.Collections.Concurrent;
using Microsoft.Data.Sqlite;
using StratQueue;
using Xunit;

namespace StratQueue.Tests;

public class ConcurrencyTests : IDisposable
{
    private readonly string _dbPath;
    private readonly StratQueueClient _client;

    public ConcurrencyTests()
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
    public async Task No_double_checkout_under_concurrent_dequeue()
    {
        const int itemCount = 100;
        const int workerCount = 10;

        for (int i = 0; i < itemCount; i++)
            _client.Enqueue("jobs", $"item-{i}");

        var dequeued = new ConcurrentBag<string>();
        var tasks = Enumerable.Range(0, workerCount).Select(_ => Task.Run(() =>
        {
            while (true)
            {
                var result = _client.Dequeue("jobs");
                if (result == null) break;
                dequeued.Add(result.Item.Payload);
                _client.Commit(result.CheckoutId);
            }
        }));

        await Task.WhenAll(tasks);

        // Every item should be dequeued exactly once
        Assert.Equal(itemCount, dequeued.Count);
        Assert.Equal(itemCount, dequeued.Distinct().Count());
    }

    [Fact]
    public async Task Concurrent_enqueue_and_dequeue_no_lost_items()
    {
        const int itemCount = 100;
        var dequeued = new ConcurrentBag<string>();

        // Start dequeuers first
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var consumers = Enumerable.Range(0, 4).Select(_ => Task.Run(async () =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                try
                {
                    var result = await _client.DequeueAsync("jobs", cancellationToken: cts.Token);
                    dequeued.Add(result.Item.Payload);
                    _client.Commit(result.CheckoutId);
                }
                catch (OperationCanceledException) { break; }
            }
        })).ToArray(); // Materialize to start tasks

        // Enqueue from multiple threads
        var producers = Enumerable.Range(0, 4).Select(p => Task.Run(() =>
        {
            for (int i = 0; i < itemCount / 4; i++)
                _client.Enqueue("jobs", $"p{p}-item-{i}");
        })).ToArray();

        await Task.WhenAll(producers);

        // Wait for all items to be consumed
        while (dequeued.Count < itemCount && !cts.Token.IsCancellationRequested)
            await Task.Delay(10);

        cts.Cancel();
        try { await Task.WhenAll(consumers); } catch { }

        Assert.Equal(itemCount, dequeued.Count);
        Assert.Equal(itemCount, dequeued.Distinct().Count());
    }

    [Fact]
    public async Task Concurrent_commits_all_succeed()
    {
        const int itemCount = 50;

        for (int i = 0; i < itemCount; i++)
            _client.Enqueue("jobs", $"item-{i}");

        // Dequeue all items first
        var checkedOut = new List<CheckedOutItem>();
        for (int i = 0; i < itemCount; i++)
        {
            var result = _client.Dequeue("jobs");
            Assert.NotNull(result);
            checkedOut.Add(result);
        }

        // Commit all concurrently
        var tasks = checkedOut.Select(co => Task.Run(() => _client.Commit(co.CheckoutId)));
        await Task.WhenAll(tasks);

        Assert.Equal(0, _client.Count("jobs"));
    }
}
