using System.Text.Json;
using Nodsoft.AspNetCore.SignalR.PostgreSQL.Internal;

namespace Nodsoft.AspNetCore.SignalR.PostgreSQL.Tests;

/// <summary>
/// Verifies that <see cref="BackplaneMessage"/> serialises and deserialises
/// correctly for every <see cref="BackplaneMessageType"/> routing variant.
/// </summary>
public sealed class BackplaneMessageSerializationTests
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private static BackplaneMessage? RoundTrip(BackplaneMessage message)
    {
        var json = JsonSerializer.Serialize(message, JsonOptions);
        return JsonSerializer.Deserialize<BackplaneMessage>(json, JsonOptions);
    }

    [Fact]
    public void All_RoundTrips_Correctly()
    {
        var message = new BackplaneMessage
        {
            ServerInstanceId = "srv1",
            Type = BackplaneMessageType.All,
            MethodName = "Broadcast",
            Args = [JsonSerializer.SerializeToElement("hello", JsonOptions)],
        };

        var result = RoundTrip(message);

        Assert.NotNull(result);
        Assert.Equal(message.ServerInstanceId, result.ServerInstanceId);
        Assert.Equal(BackplaneMessageType.All, result.Type);
        Assert.Equal(message.MethodName, result.MethodName);
        Assert.Single(result.Args);
        Assert.Equal("hello", result.Args[0].GetString());
        Assert.Null(result.Filter);
        Assert.Null(result.ExcludedConnectionIds);
        Assert.Null(result.Filters);
    }

    [Fact]
    public void AllExcept_RoundTrips_WithExcludedConnectionIds()
    {
        var message = new BackplaneMessage
        {
            ServerInstanceId = "srv1",
            Type = BackplaneMessageType.AllExcept,
            MethodName = "Broadcast",
            Args = [],
            ExcludedConnectionIds = ["conn1", "conn2"],
        };

        var result = RoundTrip(message);

        Assert.NotNull(result);
        Assert.Equal(BackplaneMessageType.AllExcept, result.Type);
        Assert.NotNull(result.ExcludedConnectionIds);
        Assert.Equal(["conn1", "conn2"], result.ExcludedConnectionIds);
    }

    [Fact]
    public void Connection_RoundTrips_WithFilter()
    {
        var message = new BackplaneMessage
        {
            ServerInstanceId = "srv1",
            Type = BackplaneMessageType.Connection,
            MethodName = "Direct",
            Args = [],
            Filter = "target-conn",
        };

        var result = RoundTrip(message);

        Assert.NotNull(result);
        Assert.Equal(BackplaneMessageType.Connection, result.Type);
        Assert.Equal("target-conn", result.Filter);
    }

    [Fact]
    public void Connections_RoundTrips_WithFilters()
    {
        var message = new BackplaneMessage
        {
            ServerInstanceId = "srv1",
            Type = BackplaneMessageType.Connections,
            MethodName = "Direct",
            Args = [],
            Filters = ["conn-a", "conn-b", "conn-c"],
        };

        var result = RoundTrip(message);

        Assert.NotNull(result);
        Assert.Equal(BackplaneMessageType.Connections, result.Type);
        Assert.NotNull(result.Filters);
        Assert.Equal(["conn-a", "conn-b", "conn-c"], result.Filters);
    }

    [Fact]
    public void Group_RoundTrips_WithFilter()
    {
        var message = new BackplaneMessage
        {
            ServerInstanceId = "srv1",
            Type = BackplaneMessageType.Group,
            MethodName = "GroupMsg",
            Args = [],
            Filter = "my-group",
        };

        var result = RoundTrip(message);

        Assert.NotNull(result);
        Assert.Equal(BackplaneMessageType.Group, result.Type);
        Assert.Equal("my-group", result.Filter);
    }

    [Fact]
    public void GroupExcept_RoundTrips_WithFilterAndExcludedIds()
    {
        var message = new BackplaneMessage
        {
            ServerInstanceId = "srv1",
            Type = BackplaneMessageType.GroupExcept,
            MethodName = "GroupMsg",
            Args = [],
            Filter = "my-group",
            ExcludedConnectionIds = ["excluded-1"],
        };

        var result = RoundTrip(message);

        Assert.NotNull(result);
        Assert.Equal(BackplaneMessageType.GroupExcept, result.Type);
        Assert.Equal("my-group", result.Filter);
        Assert.NotNull(result.ExcludedConnectionIds);
        Assert.Equal(["excluded-1"], result.ExcludedConnectionIds);
    }

    [Fact]
    public void Groups_RoundTrips_WithFilters()
    {
        var message = new BackplaneMessage
        {
            ServerInstanceId = "srv1",
            Type = BackplaneMessageType.Groups,
            MethodName = "GroupMsg",
            Args = [],
            Filters = ["group-a", "group-b"],
        };

        var result = RoundTrip(message);

        Assert.NotNull(result);
        Assert.Equal(BackplaneMessageType.Groups, result.Type);
        Assert.NotNull(result.Filters);
        Assert.Equal(["group-a", "group-b"], result.Filters);
    }

    [Fact]
    public void User_RoundTrips_WithFilter()
    {
        var message = new BackplaneMessage
        {
            ServerInstanceId = "srv1",
            Type = BackplaneMessageType.User,
            MethodName = "UserMsg",
            Args = [],
            Filter = "user-42",
        };

        var result = RoundTrip(message);

        Assert.NotNull(result);
        Assert.Equal(BackplaneMessageType.User, result.Type);
        Assert.Equal("user-42", result.Filter);
    }

    [Fact]
    public void Users_RoundTrips_WithFilters()
    {
        var message = new BackplaneMessage
        {
            ServerInstanceId = "srv1",
            Type = BackplaneMessageType.Users,
            MethodName = "UserMsg",
            Args = [],
            Filters = ["user-1", "user-2"],
        };

        var result = RoundTrip(message);

        Assert.NotNull(result);
        Assert.Equal(BackplaneMessageType.Users, result.Type);
        Assert.NotNull(result.Filters);
        Assert.Equal(["user-1", "user-2"], result.Filters);
    }

    [Fact]
    public void MultipleArgs_Preserve_Values()
    {
        var message = new BackplaneMessage
        {
            ServerInstanceId = "srv1",
            Type = BackplaneMessageType.All,
            MethodName = "Method",
            Args =
            [
                JsonSerializer.SerializeToElement(42, JsonOptions),
                JsonSerializer.SerializeToElement("text", JsonOptions),
                JsonSerializer.SerializeToElement(true, JsonOptions),
            ],
        };

        var result = RoundTrip(message);

        Assert.NotNull(result);
        Assert.Equal(3, result.Args.Length);
        Assert.Equal(42, result.Args[0].GetInt32());
        Assert.Equal("text", result.Args[1].GetString());
        Assert.True(result.Args[2].GetBoolean());
    }

    [Fact]
    public void EmptyArgs_Deserializes_ToEmptyArray()
    {
        var message = new BackplaneMessage
        {
            ServerInstanceId = "srv1",
            Type = BackplaneMessageType.All,
            MethodName = "NoArgs",
            Args = [],
        };

        var result = RoundTrip(message);

        Assert.NotNull(result);
        Assert.Empty(result.Args);
    }

    [Fact]
    public void MissingRequiredProperties_Deserialize_ThrowsJsonException()
    {
        // BackplaneMessage uses `required` init-only properties.
        // Deserializing an object that omits required fields should throw JsonException.
        Assert.Throws<JsonException>(() =>
            JsonSerializer.Deserialize<BackplaneMessage>("{}", JsonOptions));
    }
}
