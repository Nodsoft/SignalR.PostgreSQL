using Microsoft.AspNetCore.SignalR;

namespace Nodsoft.AspNetCore.SignalR.PostgreSQL.IntegrationTests.Infrastructure;

/// <summary>
/// Defines the client-side contract for the <see cref="TestHub"/>.
/// Methods listed here are invoked by the server and received by test clients.
/// </summary>
public interface ITestClient
{
    /// <summary>A generic message has been received.</summary>
    Task ReceiveMessage(string message);
}

/// <summary>
/// A minimal SignalR hub used exclusively by integration tests.
/// Exposes hub-method counterparts for every <see cref="HubLifetimeManager{THub}"/>
/// routing variant so tests can drive the backplane from the server side.
/// </summary>
public sealed class TestHub : Hub<ITestClient>
{
    /// <summary>Broadcasts <paramref name="message"/> to all connected clients.</summary>
    public Task SendAll(string message) => Clients.All.ReceiveMessage(message);

    /// <summary>Broadcasts <paramref name="message"/> to all clients except <paramref name="excludedIds"/>.</summary>
    public Task SendAllExcept(string message, string[] excludedIds)
        => Clients.AllExcept(excludedIds).ReceiveMessage(message);

    /// <summary>Sends <paramref name="message"/> to a single connection identified by <paramref name="connectionId"/>.</summary>
    public Task SendConnection(string connectionId, string message)
        => Clients.Client(connectionId).ReceiveMessage(message);

    /// <summary>Sends <paramref name="message"/> to a set of specific connections.</summary>
    public Task SendConnections(string[] connectionIds, string message)
        => Clients.Clients(connectionIds).ReceiveMessage(message);

    /// <summary>Sends <paramref name="message"/> to all members of <paramref name="groupName"/>.</summary>
    public Task SendGroup(string groupName, string message)
        => Clients.Group(groupName).ReceiveMessage(message);

    /// <summary>Sends <paramref name="message"/> to group members, excluding <paramref name="excludedIds"/>.</summary>
    public Task SendGroupExcept(string groupName, string message, string[] excludedIds)
        => Clients.GroupExcept(groupName, excludedIds).ReceiveMessage(message);

    /// <summary>Sends <paramref name="message"/> to all members of multiple groups.</summary>
    public Task SendGroups(string[] groupNames, string message)
        => Clients.Groups(groupNames).ReceiveMessage(message);

    /// <summary>Sends <paramref name="message"/> to a specific user.</summary>
    public Task SendUser(string userId, string message)
        => Clients.User(userId).ReceiveMessage(message);

    /// <summary>Sends <paramref name="message"/> to multiple users.</summary>
    public Task SendUsers(string[] userIds, string message)
        => Clients.Users(userIds).ReceiveMessage(message);

    /// <summary>Adds the calling connection to <paramref name="groupName"/>.</summary>
    public Task JoinGroup(string groupName)
        => Groups.AddToGroupAsync(Context.ConnectionId, groupName);

    /// <summary>Removes the calling connection from <paramref name="groupName"/>.</summary>
    public Task LeaveGroup(string groupName)
        => Groups.RemoveFromGroupAsync(Context.ConnectionId, groupName);

    /// <summary>Returns the current connection ID so tests can obtain it over the hub channel.</summary>
    public string GetConnectionId() => Context.ConnectionId;
}
