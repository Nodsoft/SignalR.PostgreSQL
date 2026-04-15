namespace Nodsoft.AspNetCore.SignalR.PostgreSQL.Internal;

/// <summary>
/// Represents a backplane message that is published via PostgreSQL NOTIFY
/// and consumed by all connected server instances via LISTEN.
/// Each message encodes a SignalR hub method invocation along with its routing target.
/// </summary>
internal sealed class BackplaneMessage
{
    /// <summary>
    /// Gets the unique identifier of the server instance that originated this message.
    /// Used to prevent a server from processing its own outbound messages.
    /// </summary>
    public required string ServerInstanceId { get; init; }

    /// <summary>
    /// Gets the routing type that determines which connections will receive this message.
    /// </summary>
    public required BackplaneMessageType Type { get; init; }

    /// <summary>
    /// Gets the name of the hub client method to invoke on the target connection(s).
    /// </summary>
    public required string MethodName { get; init; }

    /// <summary>
    /// Gets the serialized arguments to pass to the hub client method.
    /// Each element is a JSON-encoded representation of one argument.
    /// </summary>
    public JsonElement[] Args { get; init; } = [];

    /// <summary>
    /// Gets the single-target filter value whose meaning depends on <see cref="Type"/>:
    /// a connection ID for <see cref="BackplaneMessageType.Connection"/>,
    /// a group name for <see cref="BackplaneMessageType.Group"/> or <see cref="BackplaneMessageType.GroupExcept"/>,
    /// or a user identifier for <see cref="BackplaneMessageType.User"/>.
    /// </summary>
    public string? Filter { get; init; }

    /// <summary>
    /// Gets the connection IDs that must be excluded from delivery.
    /// Used with <see cref="BackplaneMessageType.AllExcept"/> and <see cref="BackplaneMessageType.GroupExcept"/>.
    /// </summary>
    public string[]? ExcludedConnectionIds { get; init; }

    /// <summary>
    /// Gets the collection of filter values for multi-target routing.
    /// Used with <see cref="BackplaneMessageType.Connections"/>, <see cref="BackplaneMessageType.Groups"/>,
    /// and <see cref="BackplaneMessageType.Users"/>.
    /// </summary>
    public string[]? Filters { get; init; }
}

/// <summary>
/// Describes the routing strategy for a <see cref="BackplaneMessage"/>,
/// mapping directly to the <c>Send*</c> methods on <see cref="Microsoft.AspNetCore.SignalR.HubLifetimeManager{THub}"/>.
/// </summary>
internal enum BackplaneMessageType
{
    /// <summary>Deliver to all connected clients (corresponds to <c>SendAllAsync</c>).</summary>
    All,

    /// <summary>Deliver to all clients except the specified connection IDs (corresponds to <c>SendAllExceptAsync</c>).</summary>
    AllExcept,

    /// <summary>Deliver to all clients in a named group (corresponds to <c>SendGroupAsync</c>).</summary>
    Group,

    /// <summary>Deliver to all clients in a named group, excluding specific connection IDs (corresponds to <c>SendGroupExceptAsync</c>).</summary>
    GroupExcept,

    /// <summary>Deliver to all clients in multiple named groups (corresponds to <c>SendGroupsAsync</c>).</summary>
    Groups,

    /// <summary>Deliver to all connections of a specific user (corresponds to <c>SendUserAsync</c>).</summary>
    User,

    /// <summary>Deliver to all connections of multiple users (corresponds to <c>SendUsersAsync</c>).</summary>
    Users,

    /// <summary>Deliver to a single specific connection (corresponds to <c>SendConnectionAsync</c>).</summary>
    Connection,

    /// <summary>Deliver to multiple specific connections (corresponds to <c>SendConnectionsAsync</c>).</summary>
    Connections,
}

