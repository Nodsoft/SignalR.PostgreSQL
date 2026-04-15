using System.Collections.Concurrent;

namespace Nodsoft.AspNetCore.SignalR.PostgreSQL.Tests.Helpers;

/// <summary>
/// A <see cref="HubConnectionContext"/> test double that captures all messages written to it
/// without requiring a real transport. <see cref="WriteAsync"/> is overridden to record messages
/// in <see cref="ReceivedMessages"/> and to complete a per-instance <see cref="TaskCompletionSource{T}"/>
/// so tests can await the first delivery.
/// </summary>
internal sealed class FakeHubConnectionContext : HubConnectionContext
{
    private readonly ConcurrentBag<HubMessage> _receivedMessages = [];
    private readonly TaskCompletionSource<HubMessage> _firstMessage = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>All hub messages that have been written to this connection.</summary>
    public IReadOnlyCollection<HubMessage> ReceivedMessages => _receivedMessages;

    /// <summary>
    /// Returns a <see cref="Task{T}"/> that completes when the first message is delivered to this connection,
    /// or faults when <paramref name="cancellationToken"/> is cancelled.
    /// </summary>
    public Task<HubMessage> WaitForMessageAsync(CancellationToken cancellationToken = default)
        => _firstMessage.Task.WaitAsync(cancellationToken);

    public FakeHubConnectionContext(string connectionId, string? userId = null)
        : base(new DefaultConnectionContext(connectionId), new(), NullLoggerFactory.Instance)
    {
        if (userId is not null)
        {
            UserIdentifier = userId;
        }
    }

    /// <inheritdoc/>
    public override ValueTask WriteAsync(HubMessage message, CancellationToken cancellationToken = default)
    {
        _receivedMessages.Add(message);
        _firstMessage.TrySetResult(message);
        return ValueTask.CompletedTask;
    }
}
