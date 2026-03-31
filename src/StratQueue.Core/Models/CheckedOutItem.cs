namespace StratQueue;

/// <summary>
/// Returned by Dequeue — wraps the item with its checkout ID for commit/abort.
/// </summary>
public record CheckedOutItem
{
    public required string CheckoutId { get; init; }
    public required QueueItem Item { get; init; }
}
