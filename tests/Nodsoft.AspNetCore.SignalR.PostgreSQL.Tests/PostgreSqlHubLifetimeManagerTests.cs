namespace Nodsoft.AspNetCore.SignalR.PostgreSQL.Tests;

/// <summary>
/// Unit tests for <see cref="PostgreSqlHubLifetimeManager{THub}"/>.
/// <para>
/// Tests in this class exercise construction validation, connection/group lifecycle,
/// and internal message-routing helpers.  The background LISTEN task is started but
/// immediately encounters a refused connection; disposal cancels the retry delay so
/// each test completes in milliseconds.
/// </para>
/// </summary>
public sealed class PostgreSqlHubLifetimeManagerTests : IAsyncDisposable
{
    private PostgreSqlHubLifetimeManager<TestHub>? _manager;

    // ── Construction ─────────────────────────────────────────────────────────

    [Fact]
    public void Constructor_ThrowsInvalidOperationException_WhenNeitherDataSourceNorConnectionStringIsConfigured()
    {
        var options = Options.Create(new PostgreSqlBackplaneOptions());

        Assert.Throws<InvalidOperationException>(
            () => new PostgreSqlHubLifetimeManager<TestHub>(options, NullLogger<PostgreSqlHubLifetimeManager<TestHub>>.Instance));
    }

    [Fact]
    public void Constructor_UsesDataSource_WhenDataSourceIsProvided()
    {
        // Should not throw; the manager starts successfully with a valid (even unreachable) data source.
        _manager = ManagerFactory.Create<TestHub>();
        Assert.NotNull(_manager);
    }

    [Fact]
    public void Constructor_UsesConnectionString_WhenConnectionStringIsProvided()
    {
        var options = Options.Create(new PostgreSqlBackplaneOptions
        {
            ConnectionString = "Host=127.0.0.1;Port=9;Database=test;Timeout=1;"
        });

        _manager = new PostgreSqlHubLifetimeManager<TestHub>(options, NullLogger<PostgreSqlHubLifetimeManager<TestHub>>.Instance);
        Assert.NotNull(_manager);
    }

    // ── Connection lifecycle ─────────────────────────────────────────────────

    [Fact]
    public async Task OnConnectedAsync_TracksConnection()
    {
        _manager = ManagerFactory.Create<TestHub>();
        var conn = new FakeHubConnectionContext("conn-1");

        await _manager.OnConnectedAsync(conn);

        // Verify by calling AddToGroupAsync – it only adds if the connection is tracked.
        await _manager.AddToGroupAsync("conn-1", "group-a", TestContext.Current.CancellationToken);

        // And verify by confirming the connection receives a message routed by connection ID.
        ManagerFactory.InvokeDeliverToConnection(_manager, "conn-1", "ping", []);

        var message = Assert.Single(conn.ReceivedMessages);
        var invocation = Assert.IsType<InvocationMessage>(message);
        Assert.Equal("ping", invocation.Target);
    }

    [Fact]
    public async Task OnConnectedAsync_WithUserIdentifier_TracksUserConnection()
    {
        _manager = ManagerFactory.Create<TestHub>();
        var conn = new FakeHubConnectionContext("conn-user-1", userId: "alice");

        await _manager.OnConnectedAsync(conn);

        ManagerFactory.InvokeDeliverToUser(_manager, "alice", "ping", []);

        Assert.Single(conn.ReceivedMessages);
    }

    [Fact]
    public async Task OnDisconnectedAsync_RemovesConnectionFromTracker()
    {
        _manager = ManagerFactory.Create<TestHub>();
        var conn = new FakeHubConnectionContext("conn-disc-1");

        await _manager.OnConnectedAsync(conn);
        await _manager.OnDisconnectedAsync(conn);

        ManagerFactory.InvokeDeliverToConnection(_manager, "conn-disc-1", "ping", []);

        Assert.Empty(conn.ReceivedMessages);
    }

    [Fact]
    public async Task OnDisconnectedAsync_RemovesConnectionFromUserTracker()
    {
        _manager = ManagerFactory.Create<TestHub>();
        var conn = new FakeHubConnectionContext("conn-disc-user-1", userId: "bob");

        await _manager.OnConnectedAsync(conn);
        await _manager.OnDisconnectedAsync(conn);

        ManagerFactory.InvokeDeliverToUser(_manager, "bob", "ping", []);

        Assert.Empty(conn.ReceivedMessages);
    }

    [Fact]
    public async Task OnDisconnectedAsync_RemovesConnectionFromAllGroups()
    {
        _manager = ManagerFactory.Create<TestHub>();
        var conn = new FakeHubConnectionContext("conn-grp-disc-1");

        await _manager.OnConnectedAsync(conn);
        await _manager.AddToGroupAsync("conn-grp-disc-1", "room-a", TestContext.Current.CancellationToken);
        await _manager.AddToGroupAsync("conn-grp-disc-1", "room-b", TestContext.Current.CancellationToken);
        await _manager.OnDisconnectedAsync(conn);

        ManagerFactory.InvokeDeliverToGroup(_manager, "room-a", "ping", [], []);
        ManagerFactory.InvokeDeliverToGroup(_manager, "room-b", "ping", [], []);

        Assert.Empty(conn.ReceivedMessages);
    }

    // ── Group management ─────────────────────────────────────────────────────

    [Fact]
    public async Task AddToGroupAsync_AddsConnectionToGroup()
    {
        _manager = ManagerFactory.Create<TestHub>();
        var conn = new FakeHubConnectionContext("conn-grp-1");

        await _manager.OnConnectedAsync(conn);
        await _manager.AddToGroupAsync("conn-grp-1", "group-x", TestContext.Current.CancellationToken);

        ManagerFactory.InvokeDeliverToGroup(_manager, "group-x", "ping", [], []);

        Assert.Single(conn.ReceivedMessages);
    }

    [Fact]
    public async Task AddToGroupAsync_IgnoresUnknownConnection()
    {
        _manager = ManagerFactory.Create<TestHub>();

        // Connection was never registered via OnConnectedAsync.
        await _manager.AddToGroupAsync("unknown-conn", "group-x", TestContext.Current.CancellationToken);

        ManagerFactory.InvokeDeliverToGroup(_manager, "group-x", "ping", [], []);

        // No connections in group → nothing delivered, no exception.
    }

    [Fact]
    public async Task RemoveFromGroupAsync_RemovesConnectionFromGroup()
    {
        _manager = ManagerFactory.Create<TestHub>();
        var conn = new FakeHubConnectionContext("conn-rm-grp-1");

        await _manager.OnConnectedAsync(conn);
        await _manager.AddToGroupAsync("conn-rm-grp-1", "group-y", TestContext.Current.CancellationToken);
        await _manager.RemoveFromGroupAsync("conn-rm-grp-1", "group-y", TestContext.Current.CancellationToken);

        ManagerFactory.InvokeDeliverToGroup(_manager, "group-y", "ping", [], []);

        Assert.Empty(conn.ReceivedMessages);
    }

    [Fact]
    public async Task RemoveFromGroupAsync_IgnoresUnknownGroup()
    {
        _manager = ManagerFactory.Create<TestHub>();
        var conn = new FakeHubConnectionContext("conn-rm-no-grp-1");
        await _manager.OnConnectedAsync(conn);

        // Should not throw even when the group does not exist.
        await _manager.RemoveFromGroupAsync("conn-rm-no-grp-1", "nonexistent-group", TestContext.Current.CancellationToken);
    }

    // ── Delivery helpers (routing logic) ─────────────────────────────────────

    [Fact]
    public async Task DeliverToAll_DeliversToAllTrackedConnections()
    {
        _manager = ManagerFactory.Create<TestHub>();
        var conn1 = new FakeHubConnectionContext("c1");
        var conn2 = new FakeHubConnectionContext("c2");
        var conn3 = new FakeHubConnectionContext("c3");

        await _manager.OnConnectedAsync(conn1);
        await _manager.OnConnectedAsync(conn2);
        await _manager.OnConnectedAsync(conn3);

        ManagerFactory.InvokeDeliverToAll(_manager, "broadcast", [], []);

        Assert.Single(conn1.ReceivedMessages);
        Assert.Single(conn2.ReceivedMessages);
        Assert.Single(conn3.ReceivedMessages);
    }

    [Fact]
    public async Task DeliverToAll_ExcludesSpecifiedConnectionIds()
    {
        _manager = ManagerFactory.Create<TestHub>();
        var conn1 = new FakeHubConnectionContext("d1");
        var conn2 = new FakeHubConnectionContext("d2");
        var conn3 = new FakeHubConnectionContext("d3");

        await _manager.OnConnectedAsync(conn1);
        await _manager.OnConnectedAsync(conn2);
        await _manager.OnConnectedAsync(conn3);

        ManagerFactory.InvokeDeliverToAll(_manager, "broadcast", [], ["d2"]);

        Assert.Single(conn1.ReceivedMessages);
        Assert.Empty(conn2.ReceivedMessages);
        Assert.Single(conn3.ReceivedMessages);
    }

    [Fact]
    public async Task DeliverToConnection_DeliversToCorrectConnection()
    {
        _manager = ManagerFactory.Create<TestHub>();
        var conn1 = new FakeHubConnectionContext("e1");
        var conn2 = new FakeHubConnectionContext("e2");

        await _manager.OnConnectedAsync(conn1);
        await _manager.OnConnectedAsync(conn2);

        ManagerFactory.InvokeDeliverToConnection(_manager, "e1", "dm", []);

        Assert.Single(conn1.ReceivedMessages);
        Assert.Empty(conn2.ReceivedMessages);
    }

    [Fact]
    public async Task DeliverToConnection_IgnoresUnknownConnectionId()
    {
        _manager = ManagerFactory.Create<TestHub>();
        var conn = new FakeHubConnectionContext("f1");
        await _manager.OnConnectedAsync(conn);

        // Should not throw.
        ManagerFactory.InvokeDeliverToConnection(_manager, "nonexistent", "dm", []);

        Assert.Empty(conn.ReceivedMessages);
    }

    [Fact]
    public async Task DeliverToGroup_DeliversToAllGroupMembers()
    {
        _manager = ManagerFactory.Create<TestHub>();
        var conn1 = new FakeHubConnectionContext("g1");
        var conn2 = new FakeHubConnectionContext("g2");
        var conn3 = new FakeHubConnectionContext("g3");

        await _manager.OnConnectedAsync(conn1);
        await _manager.OnConnectedAsync(conn2);
        await _manager.OnConnectedAsync(conn3);
        await _manager.AddToGroupAsync("g1", "room", TestContext.Current.CancellationToken);
        await _manager.AddToGroupAsync("g2", "room", TestContext.Current.CancellationToken);

        ManagerFactory.InvokeDeliverToGroup(_manager, "room", "msg", [], []);

        Assert.Single(conn1.ReceivedMessages);
        Assert.Single(conn2.ReceivedMessages);
        Assert.Empty(conn3.ReceivedMessages);
    }

    [Fact]
    public async Task DeliverToGroup_ExcludesSpecifiedConnectionIds()
    {
        _manager = ManagerFactory.Create<TestHub>();
        var conn1 = new FakeHubConnectionContext("h1");
        var conn2 = new FakeHubConnectionContext("h2");

        await _manager.OnConnectedAsync(conn1);
        await _manager.OnConnectedAsync(conn2);
        await _manager.AddToGroupAsync("h1", "room2", TestContext.Current.CancellationToken);
        await _manager.AddToGroupAsync("h2", "room2", TestContext.Current.CancellationToken);

        ManagerFactory.InvokeDeliverToGroup(_manager, "room2", "msg", [], ["h1"]);

        Assert.Empty(conn1.ReceivedMessages);
        Assert.Single(conn2.ReceivedMessages);
    }

    [Fact]
    public async Task DeliverToUser_DeliversToAllConnectionsOfUser()
    {
        _manager = ManagerFactory.Create<TestHub>();
        var conn1 = new FakeHubConnectionContext("i1", userId: "charlie");
        var conn2 = new FakeHubConnectionContext("i2", userId: "charlie");
        var conn3 = new FakeHubConnectionContext("i3", userId: "dave");

        await _manager.OnConnectedAsync(conn1);
        await _manager.OnConnectedAsync(conn2);
        await _manager.OnConnectedAsync(conn3);

        ManagerFactory.InvokeDeliverToUser(_manager, "charlie", "greet", []);

        Assert.Single(conn1.ReceivedMessages);
        Assert.Single(conn2.ReceivedMessages);
        Assert.Empty(conn3.ReceivedMessages);
    }

    [Fact]
    public async Task DeliverToUser_IgnoresUnknownUserId()
    {
        _manager = ManagerFactory.Create<TestHub>();

        // Should not throw.
        ManagerFactory.InvokeDeliverToUser(_manager, "unknown-user", "ping", []);
    }

    // ── Delivery content ──────────────────────────────────────────────────────

    [Fact]
    public async Task DeliverToConnection_DeliveredInvocationMessage_HasCorrectMethodNameAndArgs()
    {
        _manager = ManagerFactory.Create<TestHub>();
        var conn = new FakeHubConnectionContext("j1");
        await _manager.OnConnectedAsync(conn);

        ManagerFactory.InvokeDeliverToConnection(_manager, "j1", "testMethod", ["hello", 42]);

        var msg = Assert.Single(conn.ReceivedMessages);
        var invocation = Assert.IsType<InvocationMessage>(msg);
        Assert.Equal("testMethod", invocation.Target);
        Assert.Equal(2, invocation.Arguments.Length);
    }

    // ── Disposal ──────────────────────────────────────────────────────────────

    public async ValueTask DisposeAsync()
    {
        if (_manager is not null)
        {
            await _manager.DisposeAsync();
        }
    }
}
