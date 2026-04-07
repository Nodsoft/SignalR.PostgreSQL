namespace Nodsoft.AspNetCore.SignalR.PostgreSQL.Internal;

internal sealed class BackplaneMessage
{
    public required string ServerInstanceId { get; init; }
    public required BackplaneMessageType Type { get; init; }
    public required string MethodName { get; init; }
    public JsonElement[] Args { get; init; } = [];
    public string? Filter { get; init; }
    public string[]? ExcludedConnectionIds { get; init; }
    public string[]? Filters { get; init; }
}

internal enum BackplaneMessageType
{
    All,
    AllExcept,
    Group,
    GroupExcept,
    Groups,
    User,
    Users,
    Connection,
    Connections,
}
