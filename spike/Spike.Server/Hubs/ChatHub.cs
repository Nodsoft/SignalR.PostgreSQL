using Microsoft.AspNetCore.SignalR;
using Spike.Common;

namespace Spike.Server.Hubs;

/// <summary>
/// SignalR hub that demonstrates all major hub communication patterns:
/// broadcast, group messaging, and direct user messaging.
/// </summary>
public sealed class ChatHub : Hub<IChatClient>
{
    private static readonly string AnonymousName = "Anonymous";

    // NOTE: For spike/demo purposes only – username is read from the query string.
    // Production code should use proper authentication (e.g. JWT, cookies) instead.
    private string Username
    {
        get
        {
            var raw = Context.User?.Identity?.Name
                ?? Context.GetHttpContext()?.Request.Query["username"].ToString()
                ?? AnonymousName;

            // Sanitize: allow only printable non-control characters, max 50 chars.
            var sanitized = new string(raw.Where(c => !char.IsControl(c)).Take(50).ToArray()).Trim();
            return string.IsNullOrEmpty(sanitized) ? AnonymousName : sanitized;
        }
    }

    // ── Connection lifecycle ────────────────────────────────────────────────

    public override async Task OnConnectedAsync()
    {
        await Clients.Others.UserJoined(Username);
        await base.OnConnectedAsync();
    }

    public override async Task OnDisconnectedAsync(Exception? exception)
    {
        await Clients.Others.UserLeft(Username);
        await base.OnDisconnectedAsync(exception);
    }

    // ── Hub methods ─────────────────────────────────────────────────────────

    /// <summary>Broadcasts a message to all connected clients.</summary>
    public Task SendMessage(string content)
        => Clients.All.ReceiveMessage(new ChatMessage(Username, content, DateTimeOffset.UtcNow));

    /// <summary>Sends a message to every member of the specified group.</summary>
    public Task SendToGroup(string groupName, string content)
        => Clients.Group(groupName).ReceiveMessage(
            new ChatMessage(Username, content, DateTimeOffset.UtcNow, groupName));

    /// <summary>Sends a private message directly to a specific user by identity name.</summary>
    public Task SendToUser(string targetUsername, string content)
        => Clients.User(targetUsername).ReceiveMessage(
            new ChatMessage(Username, content, DateTimeOffset.UtcNow));

    /// <summary>Adds the calling connection to a named group.</summary>
    public async Task JoinGroup(string groupName)
    {
        await Groups.AddToGroupAsync(Context.ConnectionId, groupName);
        await Clients.Caller.JoinedGroup(groupName);
        await Clients.Group(groupName).ReceiveMessage(
            new ChatMessage("System", $"{Username} joined group '{groupName}'.", DateTimeOffset.UtcNow, groupName));
    }

    /// <summary>Removes the calling connection from a named group.</summary>
    public async Task LeaveGroup(string groupName)
    {
        await Groups.RemoveFromGroupAsync(Context.ConnectionId, groupName);
        await Clients.Caller.LeftGroup(groupName);
        await Clients.Group(groupName).ReceiveMessage(
            new ChatMessage("System", $"{Username} left group '{groupName}'.", DateTimeOffset.UtcNow, groupName));
    }
}
