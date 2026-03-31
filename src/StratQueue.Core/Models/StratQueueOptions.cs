namespace StratQueue;

/// <summary>
/// Configuration options for StratQueueClient.
/// </summary>
public record StratQueueOptions
{
    /// <summary>
    /// What to do with checked-out items found during recovery.
    /// Default: ResetToPending.
    /// </summary>
    public RecoveryPolicy RecoveryPolicy { get; init; } = RecoveryPolicy.ResetToPending;

    /// <summary>
    /// Enable WAL mode on the SQLite connection. Default: true.
    /// </summary>
    public bool EnableWalMode { get; init; } = true;
}
