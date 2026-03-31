using Microsoft.Data.Sqlite;

namespace StratQueue.Internal;

/// <summary>
/// SQLite persistence layer for queue items.
/// All SQL lives in this file — no ORM, parameterized queries only.
/// </summary>
internal class SqliteJournal : IDisposable
{
    private readonly SqliteConnection _connection;
    private readonly object _dbLock = new();

    public bool IsWalMode { get; }

    public SqliteJournal(string dbPath, bool enableWal = true)
    {
        var builder = new SqliteConnectionStringBuilder
        {
            DataSource = dbPath,
            Mode = SqliteOpenMode.ReadWriteCreate
        };
        _connection = new SqliteConnection(builder.ConnectionString);
        _connection.Open();

        if (enableWal)
        {
            using var walCmd = _connection.CreateCommand();
            walCmd.CommandText = "PRAGMA journal_mode=WAL;";
            var result = walCmd.ExecuteScalar()?.ToString();
            IsWalMode = string.Equals(result, "wal", StringComparison.OrdinalIgnoreCase);
        }

        CreateSchema();
    }

    private void CreateSchema()
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE IF NOT EXISTS queue_items (
                id              TEXT PRIMARY KEY,
                queue_name      TEXT NOT NULL,
                state           TEXT NOT NULL,
                priority        INTEGER NOT NULL DEFAULT 1,
                group_key       TEXT,
                payload         TEXT NOT NULL,
                attempts        INTEGER NOT NULL DEFAULT 0,
                max_retries     INTEGER NOT NULL DEFAULT 3,
                enqueued_at     TEXT NOT NULL,
                checked_out_at  TEXT,
                last_error      TEXT,
                checkout_id     TEXT
            );

            CREATE INDEX IF NOT EXISTS ix_queue_pending ON queue_items (queue_name, state, priority DESC, id);
            CREATE INDEX IF NOT EXISTS ix_queue_group ON queue_items (queue_name, state, group_key);
            """;
        cmd.ExecuteNonQuery();
    }

    public void Insert(QueueItem item)
    {
        lock (_dbLock)
        {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = """
                INSERT INTO queue_items (id, queue_name, state, priority, group_key, payload, attempts, max_retries, enqueued_at, checked_out_at, last_error, checkout_id)
                VALUES (@id, @queueName, @state, @priority, @groupKey, @payload, @attempts, @maxRetries, @enqueuedAt, @checkedOutAt, @lastError, @checkoutId);
                """;
            BindItemParams(cmd, item);
            cmd.ExecuteNonQuery();
        }
    }

    public void InsertBatch(IEnumerable<QueueItem> items)
    {
        lock (_dbLock)
        {
            using var transaction = _connection.BeginTransaction();
            try
            {
                foreach (var item in items)
                {
                    using var cmd = _connection.CreateCommand();
                    cmd.Transaction = transaction;
                    cmd.CommandText = """
                        INSERT INTO queue_items (id, queue_name, state, priority, group_key, payload, attempts, max_retries, enqueued_at, checked_out_at, last_error, checkout_id)
                        VALUES (@id, @queueName, @state, @priority, @groupKey, @payload, @attempts, @maxRetries, @enqueuedAt, @checkedOutAt, @lastError, @checkoutId);
                        """;
                    BindItemParams(cmd, item);
                    cmd.ExecuteNonQuery();
                }
                transaction.Commit();
            }
            catch
            {
                transaction.Rollback();
                throw;
            }
        }
    }

    public void UpdateState(string id, ItemState state, string? checkoutId = null, string? error = null, bool incrementAttempts = false)
    {
        lock (_dbLock)
        {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = incrementAttempts
                ? """
                  UPDATE queue_items
                  SET state = @state, checkout_id = @checkoutId, checked_out_at = @checkedOutAt, last_error = @error, attempts = attempts + 1
                  WHERE id = @id;
                  """
                : """
                  UPDATE queue_items
                  SET state = @state, checkout_id = @checkoutId, checked_out_at = @checkedOutAt, last_error = @error
                  WHERE id = @id;
                  """;

            cmd.Parameters.AddWithValue("@id", id);
            cmd.Parameters.AddWithValue("@state", state.ToString());
            cmd.Parameters.AddWithValue("@checkoutId", (object?)checkoutId ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@checkedOutAt",
                state == ItemState.CheckedOut ? DateTime.UtcNow.ToString("O") : (object)DBNull.Value);
            cmd.Parameters.AddWithValue("@error", (object?)error ?? DBNull.Value);

            cmd.ExecuteNonQuery();
        }
    }

    public void Delete(string id)
    {
        lock (_dbLock)
        {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = "DELETE FROM queue_items WHERE id = @id;";
            cmd.Parameters.AddWithValue("@id", id);
            cmd.ExecuteNonQuery();
        }
    }

    public void DeleteByQueue(string queueName)
    {
        lock (_dbLock)
        {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = "DELETE FROM queue_items WHERE queue_name = @queueName;";
            cmd.Parameters.AddWithValue("@queueName", queueName);
            cmd.ExecuteNonQuery();
        }
    }

    public IReadOnlyList<QueueItem> LoadAll()
    {
        lock (_dbLock)
        {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = "SELECT id, queue_name, state, priority, group_key, payload, attempts, max_retries, enqueued_at, checked_out_at, last_error, checkout_id FROM queue_items;";

            using var reader = cmd.ExecuteReader();
            var items = new List<QueueItem>();
            while (reader.Read())
            {
                items.Add(new QueueItem
                {
                    Id = reader.GetString(0),
                    QueueName = reader.GetString(1),
                    State = Enum.Parse<ItemState>(reader.GetString(2)),
                    Priority = reader.GetInt32(3),
                    GroupKey = reader.IsDBNull(4) ? null : reader.GetString(4),
                    Payload = reader.GetString(5),
                    Attempts = reader.GetInt32(6),
                    MaxRetries = reader.GetInt32(7),
                    EnqueuedAt = DateTime.Parse(reader.GetString(8)),
                    CheckedOutAt = reader.IsDBNull(9) ? null : DateTime.Parse(reader.GetString(9)),
                    LastError = reader.IsDBNull(10) ? null : reader.GetString(10),
                    CheckoutId = reader.IsDBNull(11) ? null : reader.GetString(11)
                });
            }
            return items;
        }
    }

    private static void BindItemParams(SqliteCommand cmd, QueueItem item)
    {
        cmd.Parameters.AddWithValue("@id", item.Id);
        cmd.Parameters.AddWithValue("@queueName", item.QueueName);
        cmd.Parameters.AddWithValue("@state", item.State.ToString());
        cmd.Parameters.AddWithValue("@priority", item.Priority);
        cmd.Parameters.AddWithValue("@groupKey", (object?)item.GroupKey ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@payload", item.Payload);
        cmd.Parameters.AddWithValue("@attempts", item.Attempts);
        cmd.Parameters.AddWithValue("@maxRetries", item.MaxRetries);
        cmd.Parameters.AddWithValue("@enqueuedAt", item.EnqueuedAt.ToString("O"));
        cmd.Parameters.AddWithValue("@checkedOutAt", item.CheckedOutAt.HasValue ? item.CheckedOutAt.Value.ToString("O") : DBNull.Value);
        cmd.Parameters.AddWithValue("@lastError", (object?)item.LastError ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@checkoutId", (object?)item.CheckoutId ?? DBNull.Value);
    }

    public void Dispose()
    {
        _connection.Dispose();
    }
}
