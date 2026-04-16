using Microsoft.AspNetCore.SignalR.Protocol;

namespace Nodsoft.AspNetCore.SignalR.PostgreSQL.IntegrationTests;

/// <summary>
/// Integration tests for <see cref="PostgreSqlHubLifetimeManager{THub}"/> that exercise
/// the full LISTEN/NOTIFY round-trip using a real PostgreSQL instance provided by
/// <see cref="PostgreSqlContainerFixture"/>.
/// <para>
/// These tests verify:
/// <list type="bullet">
///   <item>Messages sent via <c>Send*</c> methods are delivered to the correct local connections.</item>
///   <item>Cross-instance delivery: a message published by one manager instance is received by another manager instance sharing the same channel.</item>
///   <item>Exclusion lists, group membership, and user routing all behave correctly end-to-end.</item>
/// </list>
/// </para>
/// </summary>
[Collection(nameof(PostgreSqlContainerFixture))]
public sealed class PostgreSqlBackplaneIntegrationTests(PostgreSqlContainerFixture fixture) : IAsyncDisposable
{
    /// <summary>
    /// Maximum time to wait for a PostgreSQL notification to be delivered to a local connection.
    /// In most environments notifications arrive within a few hundred milliseconds.
    /// </summary>
    private static readonly TimeSpan DeliveryTimeout = TimeSpan.FromSeconds(10);

    private readonly List<PostgreSqlHubLifetimeManager<ChatHub>> _managers = [];

    // ── Helpers ──────────────────────────────────────────────────────────────

    private PostgreSqlHubLifetimeManager<ChatHub> CreateManager()
    {
        var dataSource = fixture.CreateDataSource();
        var options = Options.Create(new PostgreSqlBackplaneOptions { DataSource = dataSource });
        var manager = new PostgreSqlHubLifetimeManager<ChatHub>(options, NullLogger<PostgreSqlHubLifetimeManager<ChatHub>>.Instance);
        _managers.Add(manager);
        return manager;
    }

    private static CancellationToken DeliveryToken(CancellationToken testCt)
        => CancellationTokenSource.CreateLinkedTokenSource(testCt,
               new CancellationTokenSource(DeliveryTimeout).Token).Token;

    // ── SendAllAsync ──────────────────────────────────────────────────────────

    [Fact]
    public async Task SendAllAsync_DeliversToAllLocalConnections()
    {
        var manager = CreateManager();
        var conn1 = new FakeHubConnectionContext("sa-1");
        var conn2 = new FakeHubConnectionContext("sa-2");

        await manager.OnConnectedAsync(conn1);
        await manager.OnConnectedAsync(conn2);

        await manager.SendAllAsync("broadcast", [], TestContext.Current.CancellationToken);

        var ct = DeliveryToken(TestContext.Current.CancellationToken);
        await conn1.WaitForMessageAsync(ct);
        await conn2.WaitForMessageAsync(ct);

        Assert.Single(conn1.ReceivedMessages);
        Assert.Single(conn2.ReceivedMessages);
        var inv = Assert.IsType<InvocationMessage>(conn1.ReceivedMessages.Single());
        Assert.Equal("broadcast", inv.Target);
    }

    [Fact]
    public async Task SendAllExceptAsync_ExcludesSpecifiedConnections()
    {
        var manager = CreateManager();
        var conn1 = new FakeHubConnectionContext("sae-1");
        var conn2 = new FakeHubConnectionContext("sae-2");
        var conn3 = new FakeHubConnectionContext("sae-3");

        await manager.OnConnectedAsync(conn1);
        await manager.OnConnectedAsync(conn2);
        await manager.OnConnectedAsync(conn3);

        await manager.SendAllExceptAsync("greet", [], ["sae-2"], TestContext.Current.CancellationToken);

        var ct = DeliveryToken(TestContext.Current.CancellationToken);
        await conn1.WaitForMessageAsync(ct);
        await conn3.WaitForMessageAsync(ct);

        Assert.Single(conn1.ReceivedMessages);
        Assert.Empty(conn2.ReceivedMessages);
        Assert.Single(conn3.ReceivedMessages);
    }

    // ── SendConnectionAsync ───────────────────────────────────────────────────

    [Fact]
    public async Task SendConnectionAsync_DeliversToSpecificConnection()
    {
        var manager = CreateManager();
        var target = new FakeHubConnectionContext("sc-target");
        var other = new FakeHubConnectionContext("sc-other");

        await manager.OnConnectedAsync(target);
        await manager.OnConnectedAsync(other);

        await manager.SendConnectionAsync("sc-target", "dm", ["hello"], TestContext.Current.CancellationToken);

        var ct = DeliveryToken(TestContext.Current.CancellationToken);
        await target.WaitForMessageAsync(ct);

        Assert.Single(target.ReceivedMessages);
        Assert.Empty(other.ReceivedMessages);
    }

    [Fact]
    public async Task SendConnectionsAsync_DeliversToMultipleSpecificConnections()
    {
        var manager = CreateManager();
        var conn1 = new FakeHubConnectionContext("scs-1");
        var conn2 = new FakeHubConnectionContext("scs-2");
        var conn3 = new FakeHubConnectionContext("scs-3");

        await manager.OnConnectedAsync(conn1);
        await manager.OnConnectedAsync(conn2);
        await manager.OnConnectedAsync(conn3);

        await manager.SendConnectionsAsync(["scs-1", "scs-3"], "ping", [], TestContext.Current.CancellationToken);

        var ct = DeliveryToken(TestContext.Current.CancellationToken);
        await conn1.WaitForMessageAsync(ct);
        await conn3.WaitForMessageAsync(ct);

        Assert.Single(conn1.ReceivedMessages);
        Assert.Empty(conn2.ReceivedMessages);
        Assert.Single(conn3.ReceivedMessages);
    }

    // ── SendGroupAsync ────────────────────────────────────────────────────────

    [Fact]
    public async Task SendGroupAsync_DeliversToGroupMembers()
    {
        var manager = CreateManager();
        var member1 = new FakeHubConnectionContext("sg-m1");
        var member2 = new FakeHubConnectionContext("sg-m2");
        var nonMember = new FakeHubConnectionContext("sg-nm");

        await manager.OnConnectedAsync(member1);
        await manager.OnConnectedAsync(member2);
        await manager.OnConnectedAsync(nonMember);
        await manager.AddToGroupAsync("sg-m1", "room", TestContext.Current.CancellationToken);
        await manager.AddToGroupAsync("sg-m2", "room", TestContext.Current.CancellationToken);

        await manager.SendGroupAsync("room", "groupMsg", [], TestContext.Current.CancellationToken);

        var ct = DeliveryToken(TestContext.Current.CancellationToken);
        await member1.WaitForMessageAsync(ct);
        await member2.WaitForMessageAsync(ct);

        Assert.Single(member1.ReceivedMessages);
        Assert.Single(member2.ReceivedMessages);
        Assert.Empty(nonMember.ReceivedMessages);
    }

    [Fact]
    public async Task SendGroupExceptAsync_ExcludesSpecifiedConnections()
    {
        var manager = CreateManager();
        var conn1 = new FakeHubConnectionContext("sge-1");
        var conn2 = new FakeHubConnectionContext("sge-2");
        var conn3 = new FakeHubConnectionContext("sge-3");

        await manager.OnConnectedAsync(conn1);
        await manager.OnConnectedAsync(conn2);
        await manager.OnConnectedAsync(conn3);
        await manager.AddToGroupAsync("sge-1", "room2", TestContext.Current.CancellationToken);
        await manager.AddToGroupAsync("sge-2", "room2", TestContext.Current.CancellationToken);
        await manager.AddToGroupAsync("sge-3", "room2", TestContext.Current.CancellationToken);

        await manager.SendGroupExceptAsync("room2", "groupMsg", [], ["sge-2"], TestContext.Current.CancellationToken);

        var ct = DeliveryToken(TestContext.Current.CancellationToken);
        await conn1.WaitForMessageAsync(ct);
        await conn3.WaitForMessageAsync(ct);

        Assert.Single(conn1.ReceivedMessages);
        Assert.Empty(conn2.ReceivedMessages);
        Assert.Single(conn3.ReceivedMessages);
    }

    [Fact]
    public async Task SendGroupsAsync_DeliversToMultipleGroups()
    {
        var manager = CreateManager();
        var connA = new FakeHubConnectionContext("sgs-a");
        var connB = new FakeHubConnectionContext("sgs-b");
        var connC = new FakeHubConnectionContext("sgs-c");

        await manager.OnConnectedAsync(connA);
        await manager.OnConnectedAsync(connB);
        await manager.OnConnectedAsync(connC);
        await manager.AddToGroupAsync("sgs-a", "alpha", TestContext.Current.CancellationToken);
        await manager.AddToGroupAsync("sgs-b", "beta", TestContext.Current.CancellationToken);

        await manager.SendGroupsAsync(["alpha", "beta"], "multiGroup", [], TestContext.Current.CancellationToken);

        var ct = DeliveryToken(TestContext.Current.CancellationToken);
        await connA.WaitForMessageAsync(ct);
        await connB.WaitForMessageAsync(ct);

        Assert.Single(connA.ReceivedMessages);
        Assert.Single(connB.ReceivedMessages);
        Assert.Empty(connC.ReceivedMessages);
    }

    // ── SendUserAsync ─────────────────────────────────────────────────────────

    [Fact]
    public async Task SendUserAsync_DeliversToAllConnectionsOfUser()
    {
        var manager = CreateManager();
        var alice1 = new FakeHubConnectionContext("su-alice-1", userId: "alice");
        var alice2 = new FakeHubConnectionContext("su-alice-2", userId: "alice");
        var bob = new FakeHubConnectionContext("su-bob", userId: "bob");

        await manager.OnConnectedAsync(alice1);
        await manager.OnConnectedAsync(alice2);
        await manager.OnConnectedAsync(bob);

        await manager.SendUserAsync("alice", "userMsg", [], TestContext.Current.CancellationToken);

        var ct = DeliveryToken(TestContext.Current.CancellationToken);
        await alice1.WaitForMessageAsync(ct);
        await alice2.WaitForMessageAsync(ct);

        Assert.Single(alice1.ReceivedMessages);
        Assert.Single(alice2.ReceivedMessages);
        Assert.Empty(bob.ReceivedMessages);
    }

    [Fact]
    public async Task SendUsersAsync_DeliversToMultipleUsers()
    {
        var manager = CreateManager();
        var alice = new FakeHubConnectionContext("sus-alice", userId: "alice");
        var bob = new FakeHubConnectionContext("sus-bob", userId: "bob");
        var charlie = new FakeHubConnectionContext("sus-charlie", userId: "charlie");

        await manager.OnConnectedAsync(alice);
        await manager.OnConnectedAsync(bob);
        await manager.OnConnectedAsync(charlie);

        await manager.SendUsersAsync(["alice", "bob"], "multiUser", [], TestContext.Current.CancellationToken);

        var ct = DeliveryToken(TestContext.Current.CancellationToken);
        await alice.WaitForMessageAsync(ct);
        await bob.WaitForMessageAsync(ct);

        Assert.Single(alice.ReceivedMessages);
        Assert.Single(bob.ReceivedMessages);
        Assert.Empty(charlie.ReceivedMessages);
    }

    // ── Cross-instance delivery ───────────────────────────────────────────────

    [Fact]
    public async Task CrossInstance_SendAllAsync_DeliversToBothInstances()
    {
        // Two independent manager instances sharing the same PostgreSQL channel.
        var sender = CreateManager();
        var receiver = CreateManager();

        var senderConn = new FakeHubConnectionContext("ci-sender-conn");
        var receiverConn = new FakeHubConnectionContext("ci-receiver-conn");

        await sender.OnConnectedAsync(senderConn);
        await receiver.OnConnectedAsync(receiverConn);

        // Allow LISTEN connections to establish before publishing.
        await Task.Delay(200, TestContext.Current.CancellationToken);

        await sender.SendAllAsync("crossBroadcast", [], TestContext.Current.CancellationToken);

        var ct = DeliveryToken(TestContext.Current.CancellationToken);

        // Both the sender's own connection and the receiver's connection should get the message,
        // because the backplane delivers through NOTIFY → LISTEN on each server instance.
        await senderConn.WaitForMessageAsync(ct);
        await receiverConn.WaitForMessageAsync(ct);

        Assert.Single(senderConn.ReceivedMessages);
        Assert.Single(receiverConn.ReceivedMessages);
    }

    [Fact]
    public async Task CrossInstance_SendConnectionAsync_OnlyDeliveredOnHoldingInstance()
    {
        // Instance A holds the connection; Instance B publishes a direct message.
        var instanceA = CreateManager();
        var instanceB = CreateManager();

        var conn = new FakeHubConnectionContext("ci-conn-1");
        await instanceA.OnConnectedAsync(conn);

        // Allow LISTEN connections to establish.
        await Task.Delay(200, TestContext.Current.CancellationToken);

        // Instance B sends directly to the connection that lives on Instance A.
        await instanceB.SendConnectionAsync("ci-conn-1", "remoteDm", ["from-B"], TestContext.Current.CancellationToken);

        var ct = DeliveryToken(TestContext.Current.CancellationToken);
        await conn.WaitForMessageAsync(ct);

        Assert.Single(conn.ReceivedMessages);
        var inv = Assert.IsType<InvocationMessage>(conn.ReceivedMessages.Single());
        Assert.Equal("remoteDm", inv.Target);
    }

    [Fact]
    public async Task CrossInstance_SendGroupAsync_DeliversToGroupOnBothInstances()
    {
        var instanceA = CreateManager();
        var instanceB = CreateManager();

        var connA = new FakeHubConnectionContext("cig-a");
        var connB = new FakeHubConnectionContext("cig-b");

        await instanceA.OnConnectedAsync(connA);
        await instanceA.AddToGroupAsync("cig-a", "shared-room", TestContext.Current.CancellationToken);

        await instanceB.OnConnectedAsync(connB);
        await instanceB.AddToGroupAsync("cig-b", "shared-room", TestContext.Current.CancellationToken);

        // Allow LISTEN connections to establish.
        await Task.Delay(200, TestContext.Current.CancellationToken);

        // Either instance sends to the group; both instances should deliver to their members.
        await instanceA.SendGroupAsync("shared-room", "roomMsg", [], TestContext.Current.CancellationToken);

        var ct = DeliveryToken(TestContext.Current.CancellationToken);
        await connA.WaitForMessageAsync(ct);
        await connB.WaitForMessageAsync(ct);

        Assert.Single(connA.ReceivedMessages);
        Assert.Single(connB.ReceivedMessages);
    }

    // ── Delivery content ──────────────────────────────────────────────────────

    [Fact]
    public async Task SendAllAsync_DeliveredMessage_CarriesCorrectMethodNameAndArgs()
    {
        var manager = CreateManager();
        var conn = new FakeHubConnectionContext("payload-conn");
        await manager.OnConnectedAsync(conn);

        await manager.SendAllAsync("greet", ["world", 42], TestContext.Current.CancellationToken);

        var ct = DeliveryToken(TestContext.Current.CancellationToken);
        var msg = await conn.WaitForMessageAsync(ct);

        var inv = Assert.IsType<InvocationMessage>(msg);
        Assert.Equal("greet", inv.Target);
        Assert.Equal(2, inv.Arguments.Length);
    }

    // ── Disposal ──────────────────────────────────────────────────────────────

    public async ValueTask DisposeAsync()
    {
        foreach (var m in _managers)
        {
            await m.DisposeAsync();
        }
    }
}
