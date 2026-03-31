namespace StratQueue;

/// <summary>
/// Options for listing queue items.
/// </summary>
public record ListOptions
{
    public ItemState? State { get; init; }
    public int Limit { get; init; } = 100;
    public int Offset { get; init; } = 0;
}
