using Microsoft.AspNetCore.SignalR.Client;
using Nodsoft.AspNetCore.SignalR.PostgreSQL.IntegrationTests.Infrastructure;

namespace Nodsoft.AspNetCore.SignalR.PostgreSQL.IntegrationTests;

/// <summary>
/// End-to-end integration tests for the PostgreSQL SignalR backplane.
///
/// Each test spins up two separate in-process server instances connected to the
/// same PostgreSQL database and verifies that messages published on one instance
/// are correctly delivered to clients connected to the other — the core guarantee
/// of a backplane.
///
/// Infrastructure used:
/// <list type="bullet">
///   <item><see cref="PostgresContainerFixture"/> — provisions a PostgreSQL container
///     via .NET Aspire.</item>
///   <item><see cref="BackplaneWebApplicationFactory"/> — creates in-process ASP.NET Core
///     servers hosting <see cref="TestHub"/> with the backplane wired up.</item>
///   <item><see cref="HubConnection"/> — SignalR client using LongPolling so it works
///     with the <c>TestServer</c> in-process HTTP handler.</item>
/// </list>
/// </summary>
[Collection("Postgres")]
public sealed class BackplaneIntegrationTests : IAsyncLifetime
{
    private readonly PostgresContainerFixture _postgres;

    // Two independent server instances sharing the same Postgres channel.
    private BackplaneWebApplicationFactory _server1 = null!;
    private BackplaneWebApplicationFactory _server2 = null!;

    public BackplaneIntegrationTests(PostgresContainerFixture postgres)
    {
        _postgres = postgres;
    }

    public ValueTask InitializeAsync()
    {
        _server1 = new BackplaneWebApplicationFactory(_postgres.ConnectionString);
        _server2 = new BackplaneWebApplicationFactory(_postgres.ConnectionString);
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        await _server1.DisposeAsync();
        await _server2.DisposeAsync();
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    /// <summary>Creates a connected <see cref="HubConnection"/> on the given factory and returns it together with a task that resolves to a received "ReceiveMessage" payload.</summary>
    private static async Task<(HubConnection Connection, TaskCompletionSource<string> Received)>
        ConnectAsync(BackplaneWebApplicationFactory factory)
    {
        var tcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var conn = factory.CreateHubConnection();
        conn.On<string>("ReceiveMessage", msg => tcs.TrySetResult(msg));
        await conn.StartAsync();
        return (conn, tcs);
    }

    /// <summary>Waits up to <paramref name="timeout"/> for a <see cref="TaskCompletionSource{T}"/> to complete.</summary>
    private static Task<T> WaitAsync<T>(TaskCompletionSource<T> tcs, TimeSpan? timeout = null)
        => tcs.Task.WaitAsync(timeout ?? TimeSpan.FromSeconds(15));

    // ── SendAll ────────────────────────────────────────────────────────────

    [Fact]
    public async Task SendAll_DeliversToAllClients_OnSameServer()
    {
        var (conn1, recv1) = await ConnectAsync(_server1);
        var (conn2, recv2) = await ConnectAsync(_server1);

        await conn1.InvokeAsync("SendAll", "hello-all");

        Assert.Equal("hello-all", await WaitAsync(recv1));
        Assert.Equal("hello-all", await WaitAsync(recv2));

        await Task.WhenAll(conn1.DisposeAsync().AsTask(), conn2.DisposeAsync().AsTask());
    }

    [Fact]
    public async Task SendAll_DeliversAcrossServerInstances()
    {
        var (conn1, recv1) = await ConnectAsync(_server1);
        var (conn2, recv2) = await ConnectAsync(_server2);

        await conn1.InvokeAsync("SendAll", "cross-instance");

        Assert.Equal("cross-instance", await WaitAsync(recv1));
        Assert.Equal("cross-instance", await WaitAsync(recv2));

        await Task.WhenAll(conn1.DisposeAsync().AsTask(), conn2.DisposeAsync().AsTask());
    }

    // ── SendAllExcept ──────────────────────────────────────────────────────

    [Fact]
    public async Task SendAllExcept_ExcludesSpecifiedConnection()
    {
        var (conn1, recv1) = await ConnectAsync(_server1);
        var (conn2, recv2) = await ConnectAsync(_server2);

        // Obtain the connection ID of conn2 via the hub.
        var conn2Id = await conn2.InvokeAsync<string>("GetConnectionId");

        // Send from conn1, excluding conn2.
        var excluded = new[] { conn2Id };
        await conn1.InvokeAsync("SendAllExcept", "except-test", excluded);

        // conn1 should receive it; conn2 should NOT.
        Assert.Equal("except-test", await WaitAsync(recv1));

        await Task.Delay(500); // brief wait to confirm conn2 does not receive
        Assert.False(recv2.Task.IsCompleted, "Excluded connection should not receive the message.");

        await Task.WhenAll(conn1.DisposeAsync().AsTask(), conn2.DisposeAsync().AsTask());
    }

    // ── SendConnection ─────────────────────────────────────────────────────

    [Fact]
    public async Task SendConnection_DeliversOnlyToTargetConnection()
    {
        var (conn1, recv1) = await ConnectAsync(_server1);
        var (conn2, recv2) = await ConnectAsync(_server2);

        // Target conn2 specifically from conn1.
        var conn2Id = await conn2.InvokeAsync<string>("GetConnectionId");
        await conn1.InvokeAsync("SendConnection", conn2Id, "direct-msg");

        Assert.Equal("direct-msg", await WaitAsync(recv2));

        await Task.Delay(500);
        Assert.False(recv1.Task.IsCompleted, "Non-targeted connection should not receive the message.");

        await Task.WhenAll(conn1.DisposeAsync().AsTask(), conn2.DisposeAsync().AsTask());
    }

    // ── SendConnections ────────────────────────────────────────────────────

    [Fact]
    public async Task SendConnections_DeliversToMultipleSpecificConnections()
    {
        var (conn1, recv1) = await ConnectAsync(_server1);
        var (conn2, recv2) = await ConnectAsync(_server2);
        var (conn3, recv3) = await ConnectAsync(_server1);

        var conn1Id = await conn1.InvokeAsync<string>("GetConnectionId");
        var conn3Id = await conn3.InvokeAsync<string>("GetConnectionId");

        // Target conn1 and conn3 only.
        await conn2.InvokeAsync("SendConnections", new[] { conn1Id, conn3Id }, "multi-direct");

        Assert.Equal("multi-direct", await WaitAsync(recv1));
        Assert.Equal("multi-direct", await WaitAsync(recv3));

        await Task.Delay(500);
        Assert.False(recv2.Task.IsCompleted, "Non-targeted connection should not receive the message.");

        await Task.WhenAll(
            conn1.DisposeAsync().AsTask(),
            conn2.DisposeAsync().AsTask(),
            conn3.DisposeAsync().AsTask());
    }

    // ── SendGroup ──────────────────────────────────────────────────────────

    [Fact]
    public async Task SendGroup_DeliversToGroupMembers_AcrossInstances()
    {
        var (conn1, recv1) = await ConnectAsync(_server1);
        var (conn2, recv2) = await ConnectAsync(_server2);
        var (conn3, recv3) = await ConnectAsync(_server2);

        const string group = "test-group";

        // conn1 (server 1) and conn2 (server 2) join the group; conn3 does not.
        await conn1.InvokeAsync("JoinGroup", group);
        await conn2.InvokeAsync("JoinGroup", group);

        await conn1.InvokeAsync("SendGroup", group, "group-hello");

        Assert.Equal("group-hello", await WaitAsync(recv1));
        Assert.Equal("group-hello", await WaitAsync(recv2));

        await Task.Delay(500);
        Assert.False(recv3.Task.IsCompleted, "Non-group member should not receive the message.");

        await Task.WhenAll(
            conn1.DisposeAsync().AsTask(),
            conn2.DisposeAsync().AsTask(),
            conn3.DisposeAsync().AsTask());
    }

    // ── SendGroupExcept ────────────────────────────────────────────────────

    [Fact]
    public async Task SendGroupExcept_ExcludesSpecifiedConnectionWithinGroup()
    {
        var (conn1, recv1) = await ConnectAsync(_server1);
        var (conn2, recv2) = await ConnectAsync(_server2);

        const string group = "except-group";
        await conn1.InvokeAsync("JoinGroup", group);
        await conn2.InvokeAsync("JoinGroup", group);

        var conn2Id = await conn2.InvokeAsync<string>("GetConnectionId");

        await conn1.InvokeAsync("SendGroupExcept", group, "group-except-msg", new[] { conn2Id });

        Assert.Equal("group-except-msg", await WaitAsync(recv1));

        await Task.Delay(500);
        Assert.False(recv2.Task.IsCompleted, "Excluded group member should not receive the message.");

        await Task.WhenAll(conn1.DisposeAsync().AsTask(), conn2.DisposeAsync().AsTask());
    }

    // ── SendGroups ─────────────────────────────────────────────────────────

    [Fact]
    public async Task SendGroups_DeliversToAllSpecifiedGroups()
    {
        var (conn1, recv1) = await ConnectAsync(_server1);
        var (conn2, recv2) = await ConnectAsync(_server2);
        var (conn3, recv3) = await ConnectAsync(_server1);

        await conn1.InvokeAsync("JoinGroup", "groups-a");
        await conn2.InvokeAsync("JoinGroup", "groups-b");
        // conn3 joins neither group.

        await conn3.InvokeAsync("SendGroups", new[] { "groups-a", "groups-b" }, "multi-group-msg");

        Assert.Equal("multi-group-msg", await WaitAsync(recv1));
        Assert.Equal("multi-group-msg", await WaitAsync(recv2));

        await Task.Delay(500);
        Assert.False(recv3.Task.IsCompleted, "Non-group member should not receive the message.");

        await Task.WhenAll(
            conn1.DisposeAsync().AsTask(),
            conn2.DisposeAsync().AsTask(),
            conn3.DisposeAsync().AsTask());
    }

    // ── SendUser / SendUsers ───────────────────────────────────────────────

    [Fact]
    public async Task SendUser_DeliversToAuthenticatedUser()
    {
        // User-targeting requires an authenticated user on the connection.
        // We use named-user factories to inject a specific user identity.
        await using var userFactory1 = new BackplaneWebApplicationFactory(_postgres.ConnectionString)
            .WithUserAuth("alice");
        await using var userFactory2 = new BackplaneWebApplicationFactory(_postgres.ConnectionString)
            .WithUserAuth("bob");

        var (aliceConn, aliceRecv) = await ConnectAsync(userFactory1);
        var (bobConn, bobRecv) = await ConnectAsync(userFactory2);

        // Send from bob's server to "alice" user.
        await bobConn.InvokeAsync("SendUser", "alice", "hey-alice");

        Assert.Equal("hey-alice", await WaitAsync(aliceRecv));

        await Task.Delay(500);
        Assert.False(bobRecv.Task.IsCompleted, "Non-targeted user should not receive the message.");

        await Task.WhenAll(aliceConn.DisposeAsync().AsTask(), bobConn.DisposeAsync().AsTask());
    }

    [Fact]
    public async Task SendUsers_DeliversToMultipleAuthenticatedUsers()
    {
        await using var aliceFactory = new BackplaneWebApplicationFactory(_postgres.ConnectionString)
            .WithUserAuth("alice");
        await using var bobFactory = new BackplaneWebApplicationFactory(_postgres.ConnectionString)
            .WithUserAuth("bob");
        await using var charlieFactory = new BackplaneWebApplicationFactory(_postgres.ConnectionString)
            .WithUserAuth("charlie");

        var (aliceConn, aliceRecv) = await ConnectAsync(aliceFactory);
        var (bobConn, bobRecv) = await ConnectAsync(bobFactory);
        var (charlieConn, charlieRecv) = await ConnectAsync(charlieFactory);

        // Target alice and bob but not charlie.
        await charlieConn.InvokeAsync("SendUsers", new[] { "alice", "bob" }, "hey-both");

        Assert.Equal("hey-both", await WaitAsync(aliceRecv));
        Assert.Equal("hey-both", await WaitAsync(bobRecv));

        await Task.Delay(500);
        Assert.False(charlieRecv.Task.IsCompleted, "Non-targeted user should not receive the message.");

        await Task.WhenAll(
            aliceConn.DisposeAsync().AsTask(),
            bobConn.DisposeAsync().AsTask(),
            charlieConn.DisposeAsync().AsTask());
    }
}
