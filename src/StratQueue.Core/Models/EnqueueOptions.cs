namespace StratQueue;

/// <summary>
/// Options for enqueueing a single item.
/// </summary>
public record EnqueueOptions
{
    /// <summary>Priority: 0=low, 1=normal, 2=high.</summary>
    public int Priority { get; init; } = 1;

    /// <summary>Optional group key for strategy-based dequeue (e.g., domain name).</summary>
    public string? GroupKey { get; init; }

    /// <summary>Maximum retry attempts before dead-lettering. Default: 3.</summary>
    public int MaxRetries { get; init; } = 3;
}

/// <summary>
/// A single item in a batch enqueue request.
/// </summary>
public record EnqueueRequest
{
    public required string Payload { get; init; }
    public EnqueueOptions? Options { get; init; }
}
