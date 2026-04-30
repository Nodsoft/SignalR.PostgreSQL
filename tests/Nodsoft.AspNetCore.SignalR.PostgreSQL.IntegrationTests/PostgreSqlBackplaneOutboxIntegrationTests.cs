using Microsoft.AspNetCore.SignalR.Protocol;

namespace Nodsoft.AspNetCore.SignalR.PostgreSQL.IntegrationTests;

/// <summary>
/// Integration tests for the outbox-pattern code paths in
/// <see cref="PostgreSqlHubLifetimeManager{THub}"/>. These tests exercise the full
/// <c>INSERT</c> + <c>NOTIFY</c> → <c>SELECT</c> → dispatch round-trip against a real PostgreSQL
/// instance, and verify both same-instance and cross-instance delivery for payloads that
/// exceed the inline-NOTIFY threshold.
/// </summary>
[Collection(nameof(PostgreSqlContainerFixture))]
public sealed class PostgreSqlBackplaneOutboxIntegrationTests(PostgreSqlContainerFixture fixture) : IAsyncDisposable
{
    private static readonly TimeSpan DeliveryTimeout = TimeSpan.FromSeconds(15);

    private readonly List<PostgreSqlHubLifetimeManager<ChatHub>> _managers = [];

    private PostgreSqlHubLifetimeManager<ChatHub> CreateManager(Action<PostgreSqlBackplaneOptions>? configure = null)
    {
        NpgsqlDataSource dataSource = fixture.CreateDataSource();
        PostgreSqlBackplaneOptions opts = new() { DataSource = dataSource };
        configure?.Invoke(opts);
        IOptions<PostgreSqlBackplaneOptions> options = Options.Create(opts);
        PostgreSqlHubLifetimeManager<ChatHub> manager = new(options, NullLogger<PostgreSqlHubLifetimeManager<ChatHub>>.Instance);
        _managers.Add(manager);
        return manager;
    }

    private static CancellationToken DeliveryToken(CancellationToken testCt)
        => CancellationTokenSource.CreateLinkedTokenSource(testCt,
               new CancellationTokenSource(DeliveryTimeout).Token).Token;

    /// <summary>Builds an argument that, once serialized, will exceed the configured inline threshold.</summary>
    private static string BuildLargeArg(int approximateBytes) => new('x', approximateBytes);

    // ── Outbox table provisioning ────────────────────────────────────────────

    [Fact]
    public async Task OutboxTable_IsAutoProvisioned_OnManagerStartup()
    {
        var manager = CreateManager();

        // Allow the background provisioning task to complete.
        await Task.Delay(500, TestContext.Current.CancellationToken);

        await using NpgsqlConnection conn = await fixture.CreateDataSource().OpenConnectionAsync(TestContext.Current.CancellationToken);
        await using NpgsqlCommand cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT to_regclass('public.signalr_backplane_outbox')::text";
        object? result = await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken);

        Assert.NotNull(result);
        Assert.NotEqual(DBNull.Value, result);
        Assert.Equal("signalr_backplane_outbox", result);

        // Suppress unused variable warning — the manager exists to trigger provisioning.
        _ = manager;
    }

    // ── Inline path remains the no-DB-round-trip default ─────────────────────

    [Fact]
    public async Task SmallPayload_IsDeliveredInline_AndDoesNotInsertIntoOutbox()
    {
        // Use a unique table name so this test can verify "no rows inserted" in isolation
        // from any other test that may have run.
        string tableName = $"signalr_outbox_smallpath_{Guid.NewGuid():N}";
        var manager = CreateManager(o => o.OutboxTableName = tableName);
        var conn = new FakeHubConnectionContext("inline-conn");
        await manager.OnConnectedAsync(conn);

        // Allow LISTEN + outbox table provisioning to complete.
        await Task.Delay(300, TestContext.Current.CancellationToken);

        await manager.SendAllAsync("ping", ["small"], TestContext.Current.CancellationToken);

        var ct = DeliveryToken(TestContext.Current.CancellationToken);
        await conn.WaitForMessageAsync(ct);

        // Verify the message was delivered ...
        Assert.Single(conn.ReceivedMessages);
        // ... and that nothing was inserted into the outbox table for this small payload.
        await using NpgsqlConnection dbConn = await fixture.CreateDataSource().OpenConnectionAsync(TestContext.Current.CancellationToken);
        await using NpgsqlCommand cmd = dbConn.CreateCommand();
        cmd.CommandText = $"SELECT count(*) FROM {tableName}";
        long count = (long)(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken))!;
        Assert.Equal(0, count);
    }

    // ── Outbox round-trip ────────────────────────────────────────────────────

    [Fact]
    public async Task LargePayload_IsDeliveredViaOutbox_OnSameInstance()
    {
        // Force every message through the outbox by setting the inline threshold to 0.
        var manager = CreateManager(o => o.InlinePayloadThresholdBytes = 0);
        var conn = new FakeHubConnectionContext("outbox-conn-same");
        await manager.OnConnectedAsync(conn);

        await Task.Delay(300, TestContext.Current.CancellationToken);

        // Send a payload that comfortably exceeds the natural 8 KB NOTIFY limit.
        string bigArg = BuildLargeArg(20_000);
        await manager.SendAllAsync("largeBroadcast", [bigArg], TestContext.Current.CancellationToken);

        var ct = DeliveryToken(TestContext.Current.CancellationToken);
        var msg = await conn.WaitForMessageAsync(ct);

        var inv = Assert.IsType<InvocationMessage>(msg);
        Assert.Equal("largeBroadcast", inv.Target);
        Assert.Single(inv.Arguments);
    }

    [Fact]
    public async Task LargePayload_IsDeliveredViaOutbox_AcrossInstances()
    {
        // Both instances must use the same outbox table (the default) so the receiver can fetch the row.
        var sender = CreateManager(o => o.InlinePayloadThresholdBytes = 0);
        var receiver = CreateManager(o => o.InlinePayloadThresholdBytes = 0);

        var senderConn = new FakeHubConnectionContext("ob-sender");
        var receiverConn = new FakeHubConnectionContext("ob-receiver");
        await sender.OnConnectedAsync(senderConn);
        await receiver.OnConnectedAsync(receiverConn);

        // Allow both LISTEN connections + table provisioning to settle.
        await Task.Delay(500, TestContext.Current.CancellationToken);

        string bigArg = BuildLargeArg(20_000);
        await sender.SendAllAsync("crossOutbox", [bigArg], TestContext.Current.CancellationToken);

        var ct = DeliveryToken(TestContext.Current.CancellationToken);
        await senderConn.WaitForMessageAsync(ct);
        await receiverConn.WaitForMessageAsync(ct);

        Assert.Single(senderConn.ReceivedMessages);
        Assert.Single(receiverConn.ReceivedMessages);

        var inv = Assert.IsType<InvocationMessage>(receiverConn.ReceivedMessages.Single());
        Assert.Equal("crossOutbox", inv.Target);
    }

    [Fact]
    public async Task OutboxRow_IsCleanedUp_AfterExpiry()
    {
        // Short expiry so the test does not have to wait long.
        TimeSpan expiry = TimeSpan.FromMilliseconds(500);
        string tableName = $"signalr_outbox_cleanup_{Guid.NewGuid():N}";

        var manager = CreateManager(o =>
        {
            o.InlinePayloadThresholdBytes = 0;
            o.OutboxExpiry = expiry;
            o.OutboxTableName = tableName;
        });
        var conn = new FakeHubConnectionContext("cleanup-conn");
        await manager.OnConnectedAsync(conn);

        await Task.Delay(300, TestContext.Current.CancellationToken);

        await manager.SendAllAsync("toBeCleaned", ["payload"], TestContext.Current.CancellationToken);

        // Wait for delivery first.
        var ct = DeliveryToken(TestContext.Current.CancellationToken);
        await conn.WaitForMessageAsync(ct);

        // Wait for cleanup to run.
        await Task.Delay(expiry + TimeSpan.FromSeconds(1), TestContext.Current.CancellationToken);

        await using NpgsqlConnection dbConn = await fixture.CreateDataSource().OpenConnectionAsync(TestContext.Current.CancellationToken);
        await using NpgsqlCommand cmd = dbConn.CreateCommand();
        cmd.CommandText = $"SELECT count(*) FROM {tableName}";
        long count = (long)(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken))!;
        Assert.Equal(0, count);
    }

    [Fact]
    public async Task LargePayload_IsDropped_WhenOutboxIsDisabled()
    {
        var manager = CreateManager(o =>
        {
            o.UseOutbox = false;
            o.InlinePayloadThresholdBytes = 100; // ensure a moderately sized arg trips the limit
        });
        var conn = new FakeHubConnectionContext("dropped-conn");
        await manager.OnConnectedAsync(conn);

        await Task.Delay(300, TestContext.Current.CancellationToken);

        // This payload is far larger than the 100-byte threshold and the outbox is disabled,
        // so the message should be dropped silently. We assert by ensuring nothing arrives within
        // a short window.
        string bigArg = BuildLargeArg(2_000);
        await manager.SendAllAsync("droppedBroadcast", [bigArg], TestContext.Current.CancellationToken);

        await Task.Delay(500, TestContext.Current.CancellationToken);

        Assert.Empty(conn.ReceivedMessages);
    }

    // ── Disposal ─────────────────────────────────────────────────────────────

    public async ValueTask DisposeAsync()
    {
        foreach (var m in _managers)
        {
            await m.DisposeAsync();
        }
    }
}
