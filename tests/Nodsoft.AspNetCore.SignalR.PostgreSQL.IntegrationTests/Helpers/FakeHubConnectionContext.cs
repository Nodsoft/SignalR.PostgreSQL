using System.Collections.Concurrent;

namespace Nodsoft.AspNetCore.SignalR.PostgreSQL.IntegrationTests.Helpers;

// ── Test hub type ────────────────────────────────────────────────────────────

/// <summary>Hub used for integration tests.</summary>
public sealed class ChatHub : Hub;

// ── Connection helper ─────────────────────────────────────────────────────────

/// <summary>
/// A <see cref="HubConnectionContext"/> test double that captures delivered
/// <see cref="HubMessage"/> instances and allows awaiting the first delivery.
/// </summary>
internal sealed class FakeHubConnectionContext : HubConnectionContext
{
    private readonly ConcurrentBag<HubMessage> _receivedMessages = [];
    private readonly TaskCompletionSource<HubMessage> _firstMessage
        = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public IReadOnlyCollection<HubMessage> ReceivedMessages => _receivedMessages;

    /// <summary>
    /// Returns a task that completes when the first message arrives, or is cancelled
    /// via <paramref name="cancellationToken"/>.
    /// </summary>
    public Task<HubMessage> WaitForMessageAsync(CancellationToken cancellationToken = default)
        => _firstMessage.Task.WaitAsync(cancellationToken);

    public FakeHubConnectionContext(string connectionId, string? userId = null)
        : base(new DefaultConnectionContext(connectionId), new HubConnectionContextOptions(), NullLoggerFactory.Instance)
    {
        if (userId is not null)
        {
            UserIdentifier = userId;
        }
    }

    public override ValueTask WriteAsync(HubMessage message, CancellationToken cancellationToken = default)
    {
        _receivedMessages.Add(message);
        _firstMessage.TrySetResult(message);
        return ValueTask.CompletedTask;
    }
}
