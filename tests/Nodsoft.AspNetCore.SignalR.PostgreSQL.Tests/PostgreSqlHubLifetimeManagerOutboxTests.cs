using System.Reflection;

namespace Nodsoft.AspNetCore.SignalR.PostgreSQL.Tests;

/// <summary>
/// Unit tests for the outbox-pattern code paths in
/// <see cref="PostgreSqlHubLifetimeManager{THub}"/> and <see cref="PostgreSqlBackplaneOptions"/>.
/// These tests verify constructor validation, option defaults, and the publish-path
/// branching logic without requiring a real PostgreSQL endpoint.
/// </summary>
public sealed class PostgreSqlHubLifetimeManagerOutboxTests : IAsyncDisposable
{
    private PostgreSqlHubLifetimeManager<TestHub>? _manager;

    // ── Option defaults ──────────────────────────────────────────────────────

    [Fact]
    public void Options_HaveSensibleDefaults_ForOutbox()
    {
        PostgreSqlBackplaneOptions opts = new();

        Assert.True(opts.UseOutbox);
        Assert.Equal(7500, opts.InlinePayloadThresholdBytes);
        Assert.Equal("signalr_backplane_outbox", opts.OutboxTableName);
        Assert.Equal(TimeSpan.FromSeconds(30), opts.OutboxExpiry);
    }

    // ── Constructor: outbox table name validation ────────────────────────────

    [Theory]
    [InlineData("BadName")]            // uppercase
    [InlineData("bad-name")]           // hyphen
    [InlineData("bad name")]           // space
    [InlineData("bad;name")]           // semicolon (SQL injection attempt)
    [InlineData("\"injected\"; DROP")] // outright injection attempt
    [InlineData("")]                   // empty
    [InlineData(" ")]                  // whitespace
    public void Constructor_ThrowsInvalidOperationException_WhenOutboxTableNameContainsUnsafeCharacters(string tableName)
    {
        IOptions<PostgreSqlBackplaneOptions> options = Options.Create(new PostgreSqlBackplaneOptions
        {
            ConnectionString = "Host=127.0.0.1;Port=9;Database=test;Timeout=1;",
            OutboxTableName = tableName,
        });

        InvalidOperationException ex = Assert.Throws<InvalidOperationException>(
            () => new PostgreSqlHubLifetimeManager<TestHub>(options, NullLogger<PostgreSqlHubLifetimeManager<TestHub>>.Instance));

        Assert.Contains("OutboxTableName", ex.Message);
    }

    [Theory]
    [InlineData("signalr_backplane_outbox")]
    [InlineData("custom_outbox_42")]
    [InlineData("a")]
    public void Constructor_AcceptsValidOutboxTableName(string tableName)
    {
        _manager = ManagerFactory.Create<TestHub>(configure: o => o.OutboxTableName = tableName);
        Assert.NotNull(_manager);
    }

    // ── PublishAsync routing: inline vs. outbox ──────────────────────────────

    [Fact]
    public async Task PublishAsync_DropsOversizedPayload_WhenOutboxIsDisabled()
    {
        // Arrange: very low threshold + outbox disabled → any non-trivial message exceeds the threshold
        // and should be silently dropped (no exception, no DB connection attempted before the size check).
        _manager = ManagerFactory.Create<TestHub>(configure: o =>
        {
            o.UseOutbox = false;
            o.InlinePayloadThresholdBytes = 10; // far smaller than any real serialized message
        });

        // Build the internal BackplaneMessage via reflection (it's internal to the production assembly).
        object message = ManagerFactory.CreateBackplaneMessage(
            serverInstanceId: "test-server",
            type: 0, // BackplaneMessageType.All
            methodName: "ping");

        // Act + Assert: should complete without throwing (the message is dropped silently).
        await ManagerFactory.InvokePublishAsync(_manager, message, TestContext.Current.CancellationToken);
    }

    // ── OnNotification: outbox marker recognition ────────────────────────────

    [Fact]
    public async Task OnNotification_DetectsOutboxMarker_AndDoesNotDeliverInline()
    {
        // Arrange: a connection that would receive any inline-dispatched message.
        _manager = ManagerFactory.Create<TestHub>();
        FakeHubConnectionContext conn = new("outbox-conn-1");
        await _manager.OnConnectedAsync(conn);

        // Send a payload that LOOKS like an outbox marker. Because no real outbox row exists,
        // and the data source points at an unreachable PG, the async fetch task fails silently;
        // critically, no inline dispatch should happen.
        ManagerFactory.InvokeOnNotification(_manager, """{"outboxId":"00000000000000000000000000000000"}""");

        // Give the fire-and-forget fetch a chance to run (it will fail, but must not crash the manager).
        await Task.Delay(50, TestContext.Current.CancellationToken);

        Assert.Empty(conn.ReceivedMessages);
    }

    [Fact]
    public async Task OnNotification_DispatchesInlinePayload_ToLocalConnection()
    {
        _manager = ManagerFactory.Create<TestHub>();
        FakeHubConnectionContext conn = new("inline-conn-1");
        await _manager.OnConnectedAsync(conn);

        // Construct a valid inline backplane message JSON manually (web/camelCase to match the manager).
        string payload = """
            {"serverInstanceId":"remote-server","type":0,"methodName":"ping","args":[]}
            """;

        ManagerFactory.InvokeOnNotification(_manager, payload);

        HubMessage delivered = Assert.Single(conn.ReceivedMessages);
        InvocationMessage invocation = Assert.IsType<InvocationMessage>(delivered);
        Assert.Equal("ping", invocation.Target);
    }

    [Fact]
    public void OnNotification_IgnoresMalformedJson_WithoutThrowing()
    {
        _manager = ManagerFactory.Create<TestHub>();
        // Should not throw.
        ManagerFactory.InvokeOnNotification(_manager, "not valid json {{{");
    }

    // ── Disposal ─────────────────────────────────────────────────────────────

    public async ValueTask DisposeAsync()
    {
        if (_manager is not null)
        {
            await _manager.DisposeAsync();
        }
    }
}

