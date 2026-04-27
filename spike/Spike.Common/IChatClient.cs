namespace Spike.Common;

/// <summary>
/// Defines the methods the server pushes to connected clients.
/// </summary>
public interface IChatClient
{
    /// <summary>A new chat message has arrived.</summary>
    Task ReceiveMessage(ChatMessage message);

    /// <summary>A user has connected to the hub.</summary>
    Task UserJoined(string username);

    /// <summary>A user has disconnected from the hub.</summary>
    Task UserLeft(string username);

    /// <summary>The caller has been added to a group.</summary>
    Task JoinedGroup(string groupName);

    /// <summary>The caller has been removed from a group.</summary>
    Task LeftGroup(string groupName);
}
