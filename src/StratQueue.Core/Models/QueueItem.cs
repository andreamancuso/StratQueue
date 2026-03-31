namespace StratQueue;

/// <summary>
/// A queue item with its full metadata.
/// </summary>
public record QueueItem
{
    public required string Id { get; init; }
    public required string QueueName { get; init; }
    public required string Payload { get; init; }
    public ItemState State { get; init; }
    public int Priority { get; init; }
    public string? GroupKey { get; init; }
    public int Attempts { get; init; }
    public int MaxRetries { get; init; } = 3;
    public DateTime EnqueuedAt { get; init; }
    public DateTime? CheckedOutAt { get; init; }
    public string? LastError { get; init; }
    public string? CheckoutId { get; init; }
}
