using Microsoft.Data.Sqlite;
using StratQueue;
using StratQueue.Internal;
using Xunit;

namespace StratQueue.Tests;

public class JournalTests : IDisposable
{
    private readonly string _dbPath;
    private readonly SqliteJournal _journal;

    public JournalTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"stratqueue_test_{Guid.NewGuid():N}.db");
        _journal = new SqliteJournal(_dbPath);
    }

    public void Dispose()
    {
        _journal.Dispose();
        SqliteConnection.ClearAllPools();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        if (File.Exists(_dbPath + "-wal")) File.Delete(_dbPath + "-wal");
        if (File.Exists(_dbPath + "-shm")) File.Delete(_dbPath + "-shm");
    }

    private static QueueItem MakeItem(string queueName = "test", string? groupKey = null, int priority = 1) => new()
    {
        Id = Guid.NewGuid().ToString(),
        QueueName = queueName,
        Payload = """{"url":"https://example.com"}""",
        State = ItemState.Pending,
        Priority = priority,
        GroupKey = groupKey,
        Attempts = 0,
        MaxRetries = 3,
        EnqueuedAt = DateTime.UtcNow
    };

    [Fact]
    public void Insert_and_LoadAll_roundtrips_all_fields()
    {
        var item = MakeItem(groupKey: "example.com");
        _journal.Insert(item);

        var loaded = _journal.LoadAll();
        Assert.Single(loaded);

        var got = loaded[0];
        Assert.Equal(item.Id, got.Id);
        Assert.Equal(item.QueueName, got.QueueName);
        Assert.Equal(item.Payload, got.Payload);
        Assert.Equal(ItemState.Pending, got.State);
        Assert.Equal(item.Priority, got.Priority);
        Assert.Equal("example.com", got.GroupKey);
        Assert.Equal(0, got.Attempts);
        Assert.Equal(3, got.MaxRetries);
        Assert.Null(got.CheckedOutAt);
        Assert.Null(got.LastError);
        Assert.Null(got.CheckoutId);
    }

    [Fact]
    public void Insert_multiple_items_LoadAll_returns_all()
    {
        var a = MakeItem();
        var b = MakeItem();
        var c = MakeItem();
        _journal.Insert(a);
        _journal.Insert(b);
        _journal.Insert(c);

        var loaded = _journal.LoadAll();
        Assert.Equal(3, loaded.Count);
    }

    [Fact]
    public void UpdateState_changes_state_and_sets_checkout_fields()
    {
        var item = MakeItem();
        _journal.Insert(item);

        var checkoutId = Guid.NewGuid().ToString();
        _journal.UpdateState(item.Id, ItemState.CheckedOut, checkoutId: checkoutId);

        var loaded = _journal.LoadAll();
        var got = loaded[0];
        Assert.Equal(ItemState.CheckedOut, got.State);
        Assert.Equal(checkoutId, got.CheckoutId);
        Assert.NotNull(got.CheckedOutAt);
    }

    [Fact]
    public void UpdateState_sets_error_and_increments_attempts()
    {
        var item = MakeItem();
        _journal.Insert(item);

        _journal.UpdateState(item.Id, ItemState.Pending, error: "timeout", incrementAttempts: true);

        var got = _journal.LoadAll()[0];
        Assert.Equal(ItemState.Pending, got.State);
        Assert.Equal("timeout", got.LastError);
        Assert.Equal(1, got.Attempts);
    }

    [Fact]
    public void Delete_removes_the_row()
    {
        var item = MakeItem();
        _journal.Insert(item);
        Assert.Single(_journal.LoadAll());

        _journal.Delete(item.Id);
        Assert.Empty(_journal.LoadAll());
    }

    [Fact]
    public void DeleteByQueue_removes_only_that_queues_items()
    {
        var a = MakeItem("queue_a");
        var b = MakeItem("queue_b");
        _journal.Insert(a);
        _journal.Insert(b);

        _journal.DeleteByQueue("queue_a");

        var loaded = _journal.LoadAll();
        Assert.Single(loaded);
        Assert.Equal("queue_b", loaded[0].QueueName);
    }

    [Fact]
    public void InsertBatch_adds_all_items_in_one_call()
    {
        var items = Enumerable.Range(0, 50).Select(_ => MakeItem()).ToList();
        _journal.InsertBatch(items);

        Assert.Equal(50, _journal.LoadAll().Count);
    }

    [Fact]
    public void Schema_is_created_on_first_use()
    {
        // The constructor should have created the schema.
        // Verify by inserting — if schema doesn't exist, this throws.
        var item = MakeItem();
        _journal.Insert(item);
        Assert.Single(_journal.LoadAll());
    }

    [Fact]
    public void Wal_mode_is_enabled()
    {
        Assert.True(_journal.IsWalMode);
    }
}
