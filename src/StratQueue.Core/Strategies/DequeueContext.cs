namespace StratQueue;

/// <summary>
/// Per-consumer state passed to dequeue strategies.
/// Allows strategies to make consumer-aware decisions.
/// </summary>
public class DequeueContext
{
    /// <summary>Identifier for this consumer (e.g., worker thread ID).</summary>
    public string? ConsumerId { get; init; }

    /// <summary>The group key of the last item this consumer processed.</summary>
    public string? LastGroupKey { get; set; }
}
