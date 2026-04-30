using System.Collections.Concurrent;
using System.Text;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Protocol;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Nodsoft.AspNetCore.SignalR.PostgreSQL.Internal;

namespace Nodsoft.AspNetCore.SignalR.PostgreSQL;

using HubConnectionCtxDictionary = ConcurrentDictionary<string, HubConnectionContext>;

/// <summary>
/// A SignalR <see cref="HubLifetimeManager{THub}"/> that uses PostgreSQL LISTEN/NOTIFY
/// to broadcast messages across multiple server instances.
/// </summary>
/// <typeparam name="THub">The hub type this manager serves.</typeparam>
public sealed class PostgreSqlHubLifetimeManager<THub> : HubLifetimeManager<THub>, IAsyncDisposable
    where THub : Hub
{
    /// <summary>A unique identifier for this server instance, used to avoid self-delivery of backplane messages.</summary>
    private readonly string _serverInstanceId = Guid.NewGuid().ToString("N");

    /// <summary>The PostgreSQL LISTEN/NOTIFY channel name derived from the hub type (e.g. <c>signalr__chathub</c>).</summary>
    private readonly string _channelName;

    /// <summary>Resolved backplane options (cached at construction time).</summary>
    private readonly PostgreSqlBackplaneOptions _options;

    /// <summary>Validated outbox table identifier, ready to be interpolated into SQL.</summary>
    private readonly string _outboxTableName;

    /// <summary>Background task that ensures the outbox table exists. <see langword="null"/> when the outbox is disabled.</summary>
    private readonly Task? _outboxInitTask;

    /// <summary>All active connections on this server instance, keyed by connection ID.</summary>
    private readonly HubConnectionCtxDictionary _connections = new(StringComparer.Ordinal);

    /// <summary>Group membership map: group name → (connection ID → connection).</summary>
    private readonly ConcurrentDictionary<string, HubConnectionCtxDictionary> _groups = new(StringComparer.Ordinal);

    /// <summary>User connection map: user identifier → (connection ID → connection).</summary>
    private readonly ConcurrentDictionary<string, HubConnectionCtxDictionary> _users = new(StringComparer.Ordinal);

    /// <summary>Npgsql data source used for opening NOTIFY command connections and the LISTEN connection.</summary>
    private readonly NpgsqlDataSource _dataSource;

    /// <summary>Logger for diagnostic output.</summary>
    private readonly ILogger<PostgreSqlHubLifetimeManager<THub>> _logger;

    /// <summary>The dedicated connection used for the background LISTEN loop.</summary>
    private NpgsqlConnection? _listenConnection;

    /// <summary>The background task running the LISTEN loop.</summary>
    private Task? _listenTask;

    /// <summary>Cancellation source that shuts down the LISTEN loop on disposal.</summary>
    private readonly CancellationTokenSource _cts = new();

    /// <summary>Shared JSON serializer options used for both serializing and deserializing backplane payloads.</summary>
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    /// <summary>
    /// Initializes a new <see cref="PostgreSqlHubLifetimeManager{THub}"/>, validates configuration,
    /// and starts the background LISTEN loop.
    /// </summary>
    /// <param name="options">Backplane configuration (data source or connection string).</param>
    /// <param name="logger">Logger for diagnostics.</param>
    /// <exception cref="InvalidOperationException">
    /// Thrown when neither <see cref="PostgreSqlBackplaneOptions.DataSource"/> nor
    /// <see cref="PostgreSqlBackplaneOptions.ConnectionString"/> is configured,
    /// or when the hub type name contains characters that are unsafe for a PostgreSQL channel identifier.
    /// </exception>
    public PostgreSqlHubLifetimeManager(
        IOptions<PostgreSqlBackplaneOptions> options,
        ILogger<PostgreSqlHubLifetimeManager<THub>> logger)
    {
        _logger = logger;

        PostgreSqlBackplaneOptions opts = options.Value;
        _options = opts;
        _dataSource = opts.DataSource
            ?? (opts.ConnectionString is not null
                ? NpgsqlDataSource.Create(opts.ConnectionString)
                : throw new InvalidOperationException(
                    "Either DataSource or ConnectionString must be set in PostgreSqlBackplaneOptions."));

        // Validate the channel name to prevent SQL injection via interpolation.
        // Channel names are derived from hub type names and must only contain safe identifier characters.
        string rawHubName = typeof(THub).Name.ToLowerInvariant();
        if (!System.Text.RegularExpressions.Regex.IsMatch(rawHubName, @"^[a-z0-9_]+$"))
        {
            throw new InvalidOperationException(
                $"Hub type name '{typeof(THub).Name}' contains characters that are not allowed in a PostgreSQL channel name.");
        }

        _channelName = $"signalr__{rawHubName}";

        // Validate the outbox table name to prevent SQL injection via identifier interpolation.
        if (string.IsNullOrWhiteSpace(opts.OutboxTableName)
            || !System.Text.RegularExpressions.Regex.IsMatch(opts.OutboxTableName, @"^[a-z0-9_]+$"))
        {
            throw new InvalidOperationException(
                $"OutboxTableName '{opts.OutboxTableName}' is invalid. It must contain only lowercase letters, digits, and underscores.");
        }

        _outboxTableName = opts.OutboxTableName;

        if (opts.UseOutbox)
        {
            // Best-effort table provisioning. Failures are logged but do not block the manager;
            // PublishAsync will surface any issues at publish time.
            _outboxInitTask = EnsureOutboxTableAsync(_cts.Token);
        }

        _listenTask = StartListeningAsync(_cts.Token);
    }

    // ── Connection lifecycle ────────────────────────────────────────────────

    /// <inheritdoc/>
    public override Task OnConnectedAsync(HubConnectionContext connection)
    {
        _connections[connection.ConnectionId] = connection;

        if (connection.UserIdentifier is { } userId)
        {
            _users.GetOrAdd(userId, _ => new(StringComparer.Ordinal))
                [connection.ConnectionId] = connection;
        }

        _logger.LogInformation("Connection '{ConnectionId}' connected to hub '{HubName}'", connection.ConnectionId, typeof(THub).Name);
        
        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public override Task OnDisconnectedAsync(HubConnectionContext connection)
    {
        _connections.TryRemove(connection.ConnectionId, out _);

        if (connection.UserIdentifier is { } userId
            && _users.TryGetValue(userId, out HubConnectionCtxDictionary? userConnections))
        {
            userConnections.TryRemove(connection.ConnectionId, out _);
        }

        foreach (HubConnectionCtxDictionary group in _groups.Values)
        {
            group.TryRemove(connection.ConnectionId, out _);
        }

        return Task.CompletedTask;
    }

    // ── Group management ────────────────────────────────────────────────────

    /// <inheritdoc/>
    public override Task AddToGroupAsync(string connectionId, string groupName, CancellationToken cancellationToken = default)
    {
        if (_connections.TryGetValue(connectionId, out HubConnectionContext? connection))
        {
            _groups.GetOrAdd(groupName, _ => new(StringComparer.Ordinal))
                [connectionId] = connection;
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public override Task RemoveFromGroupAsync(string connectionId, string groupName, CancellationToken cancellationToken = default)
    {
        if (_groups.TryGetValue(groupName, out HubConnectionCtxDictionary? group))
        {
            group.TryRemove(connectionId, out _);
        }

        return Task.CompletedTask;
    }

    // ── Send methods ────────────────────────────────────────────────────────

    /// <inheritdoc/>
    public override Task SendAllAsync(string methodName, object?[] args, CancellationToken cancellationToken = default)
        => PublishAsync(new()
        {
            ServerInstanceId = _serverInstanceId,
            Type = BackplaneMessageType.All,
            MethodName = methodName,
            Args = SerializeArgs(args),
        }, cancellationToken);

    /// <inheritdoc/>
    public override Task SendAllExceptAsync(string methodName, object?[] args, IReadOnlyList<string> excludedConnectionIds, CancellationToken cancellationToken = default)
        => PublishAsync(new()
        {
            ServerInstanceId = _serverInstanceId,
            Type = BackplaneMessageType.AllExcept,
            MethodName = methodName,
            Args = SerializeArgs(args),
            ExcludedConnectionIds = [.. excludedConnectionIds],
        }, cancellationToken);

    /// <inheritdoc/>
    public override Task SendConnectionAsync(string connectionId, string methodName, object?[] args, CancellationToken cancellationToken = default)
        => PublishAsync(new()
        {
            ServerInstanceId = _serverInstanceId,
            Type = BackplaneMessageType.Connection,
            MethodName = methodName,
            Args = SerializeArgs(args),
            Filter = connectionId,
        }, cancellationToken);

    /// <inheritdoc/>
    public override Task SendConnectionsAsync(IReadOnlyList<string> connectionIds, string methodName, object?[] args, CancellationToken cancellationToken = default)
        => PublishAsync(new()
        {
            ServerInstanceId = _serverInstanceId,
            Type = BackplaneMessageType.Connections,
            MethodName = methodName,
            Args = SerializeArgs(args),
            Filters = [.. connectionIds],
        }, cancellationToken);

    /// <inheritdoc/>
    public override Task SendGroupAsync(string groupName, string methodName, object?[] args, CancellationToken cancellationToken = default)
        => PublishAsync(new()
        {
            ServerInstanceId = _serverInstanceId,
            Type = BackplaneMessageType.Group,
            MethodName = methodName,
            Args = SerializeArgs(args),
            Filter = groupName,
        }, cancellationToken);

    /// <inheritdoc/>
    public override Task SendGroupExceptAsync(string groupName, string methodName, object?[] args, IReadOnlyList<string> excludedConnectionIds, CancellationToken cancellationToken = default)
        => PublishAsync(new()
        {
            ServerInstanceId = _serverInstanceId,
            Type = BackplaneMessageType.GroupExcept,
            MethodName = methodName,
            Args = SerializeArgs(args),
            Filter = groupName,
            ExcludedConnectionIds = [.. excludedConnectionIds],
        }, cancellationToken);

    /// <inheritdoc/>
    public override Task SendGroupsAsync(IReadOnlyList<string> groupNames, string methodName, object?[] args, CancellationToken cancellationToken = default)
        => PublishAsync(new()
        {
            ServerInstanceId = _serverInstanceId,
            Type = BackplaneMessageType.Groups,
            MethodName = methodName,
            Args = SerializeArgs(args),
            Filters = [.. groupNames],
        }, cancellationToken);

    /// <inheritdoc/>
    public override Task SendUserAsync(string userId, string methodName, object?[] args, CancellationToken cancellationToken = default)
        => PublishAsync(new()
        {
            ServerInstanceId = _serverInstanceId,
            Type = BackplaneMessageType.User,
            MethodName = methodName,
            Args = SerializeArgs(args),
            Filter = userId,
        }, cancellationToken);

    /// <inheritdoc/>
    public override Task SendUsersAsync(IReadOnlyList<string> userIds, string methodName, object?[] args, CancellationToken cancellationToken = default)
        => PublishAsync(new()
        {
            ServerInstanceId = _serverInstanceId,
            Type = BackplaneMessageType.Users,
            MethodName = methodName,
            Args = SerializeArgs(args),
            Filters = [.. userIds],
        }, cancellationToken);

    // ── Internal helpers ────────────────────────────────────────────────────

    /// <summary>
    /// Serializes <paramref name="message"/> as a JSON payload and publishes it to the PostgreSQL
    /// notification channel via <c>pg_notify</c>.
    /// <para>
    /// Payloads whose serialized UTF-8 size is within
    /// <see cref="PostgreSqlBackplaneOptions.InlinePayloadThresholdBytes"/> are sent inline through
    /// <c>pg_notify</c> (no additional database round-trip). Larger payloads are routed through the
    /// outbox table when <see cref="PostgreSqlBackplaneOptions.UseOutbox"/> is enabled, or dropped with
    /// a warning otherwise.
    /// </para>
    /// </summary>
    private async Task PublishAsync(BackplaneMessage message, CancellationToken cancellationToken)
    {
        string payload = JsonSerializer.Serialize(message, JsonOptions);
        int payloadByteLength = Encoding.UTF8.GetByteCount(payload);

        if (payloadByteLength <= _options.InlinePayloadThresholdBytes)
        {
            await PublishInlineAsync(payload, cancellationToken);
            return;
        }

        if (!_options.UseOutbox)
        {
            _logger.LogWarning(
                "BackplaneMessage payload of {PayloadBytes} bytes exceeds the inline NOTIFY threshold of {ThresholdBytes} bytes, and the outbox is disabled; the message will not be delivered.",
                payloadByteLength,
                _options.InlinePayloadThresholdBytes);
            return;
        }

        await PublishViaOutboxAsync(payload, payloadByteLength, cancellationToken);
    }

    /// <summary>Issues a single <c>pg_notify</c> call carrying the full serialized payload inline.</summary>
    private async Task PublishInlineAsync(string payload, CancellationToken cancellationToken)
    {
        await using NpgsqlCommand cmd = _dataSource.CreateCommand($"SELECT pg_notify('{_channelName}', @payload)");
        cmd.Parameters.AddWithValue("payload", payload);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// Inserts the serialized payload into the outbox table and atomically issues a small
    /// <c>pg_notify</c> reference so peer instances can fetch it. Schedules a background
    /// cleanup to delete the row after <see cref="PostgreSqlBackplaneOptions.OutboxExpiry"/>.
    /// </summary>
    private async Task PublishViaOutboxAsync(string payload, int payloadByteLength, CancellationToken cancellationToken)
    {
        // Wait for the outbox table to be provisioned. The init task does its own retry/error logging.
        if (_outboxInitTask is not null)
        {
            try { await _outboxInitTask; }
            catch { /* surfaced below when the INSERT fails */ }
        }

        string outboxId = Guid.NewGuid().ToString("N");
        string marker = JsonSerializer.Serialize(new OutboxNotification { OutboxId = outboxId }, JsonOptions);

        // Single-statement transaction: INSERT then NOTIFY. The NOTIFY is queued during the implicit
        // transaction and only delivered to listeners on commit, by which point the row is visible.
        string sql =
            $"WITH inserted AS (" +
            $"  INSERT INTO {_outboxTableName} (id, channel, payload) VALUES (@id, @channel, @payload) RETURNING 1" +
            $") SELECT pg_notify('{_channelName}', @marker) FROM inserted";

        await using (NpgsqlCommand cmd = _dataSource.CreateCommand(sql))
        {
            cmd.Parameters.AddWithValue("id", outboxId);
            cmd.Parameters.AddWithValue("channel", _channelName);
            cmd.Parameters.AddWithValue("payload", payload);
            cmd.Parameters.AddWithValue("marker", marker);
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }

        _logger.LogDebug(
            "Published BackplaneMessage of {PayloadBytes} bytes to outbox row '{OutboxId}' on channel '{Channel}'.",
            payloadByteLength,
            outboxId,
            _channelName);

        // Fire-and-forget cleanup. We deliberately do NOT pass the manager's CTS token so that
        // a graceful shutdown does not leave an orphaned row behind on the publishing instance.
        _ = ScheduleOutboxRowCleanupAsync(outboxId);
    }

    /// <summary>
    /// Waits for <see cref="PostgreSqlBackplaneOptions.OutboxExpiry"/> and then best-effort deletes
    /// the outbox row created by <see cref="PublishViaOutboxAsync"/>. Errors are logged and swallowed.
    /// </summary>
    private async Task ScheduleOutboxRowCleanupAsync(string outboxId)
    {
        try
        {
            await Task.Delay(_options.OutboxExpiry, _cts.Token);
        }
        catch (OperationCanceledException)
        {
            // Manager is shutting down; attempt the delete immediately so the row does not linger.
        }

        try
        {
            await using NpgsqlCommand cmd = _dataSource.CreateCommand($"DELETE FROM {_outboxTableName} WHERE id = @id");
            cmd.Parameters.AddWithValue("id", outboxId);
            await cmd.ExecuteNonQueryAsync();
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Failed to delete outbox row '{OutboxId}'; it will be cleaned up by a future operation or operator-managed pruning.", outboxId);
        }
    }

    /// <summary>
    /// Idempotently provisions the outbox table and supporting index. Safe to call concurrently
    /// across multiple manager instances thanks to <c>CREATE TABLE IF NOT EXISTS</c>.
    /// </summary>
    private async Task EnsureOutboxTableAsync(CancellationToken cancellationToken)
    {
        try
        {
            string ddl =
                $"CREATE TABLE IF NOT EXISTS {_outboxTableName} (" +
                "  id text PRIMARY KEY," +
                "  channel text NOT NULL," +
                "  payload text NOT NULL," +
                "  created_at timestamptz NOT NULL DEFAULT now()" +
                ");" +
                $"CREATE INDEX IF NOT EXISTS ix_{_outboxTableName}_created_at ON {_outboxTableName} (created_at);";

            await using NpgsqlCommand cmd = _dataSource.CreateCommand(ddl);
            await cmd.ExecuteNonQueryAsync(cancellationToken);

            _logger.LogDebug("Outbox table '{Table}' is ready on channel '{Channel}'.", _outboxTableName, _channelName);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Manager disposed before the table could be provisioned.
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to provision outbox table '{Table}'. Outbox publishes will fail until the table exists.", _outboxTableName);
            throw;
        }
    }

    /// <summary>
    /// Reads the serialized <see cref="BackplaneMessage"/> for the given outbox row.
    /// Returns <see langword="null"/> if the row has already been cleaned up or was never written.
    /// </summary>
    private async Task<string?> ReadOutboxPayloadAsync(string outboxId, CancellationToken cancellationToken)
    {
        await using NpgsqlCommand cmd = _dataSource.CreateCommand($"SELECT payload FROM {_outboxTableName} WHERE id = @id");
        cmd.Parameters.AddWithValue("id", outboxId);
        object? result = await cmd.ExecuteScalarAsync(cancellationToken);
        return result is string s ? s : null;
    }

    /// <summary>Serializes the given argument array to <see cref="JsonElement"/> values suitable for JSON transport.</summary>
    private static JsonElement[] SerializeArgs(object?[] args)
        => args.Select(a => JsonSerializer.SerializeToElement(a, JsonOptions)).ToArray();

    /// <summary>Converts deserialized <see cref="JsonElement"/> values back to an object array for hub invocation.</summary>
    private static object?[] DeserializeArgs(JsonElement[] elements)
        => elements.Select(e => (object?)e).ToArray();

    /// <summary>
    /// Background loop that opens a dedicated PostgreSQL connection and issues a LISTEN command
    /// for the hub's notification channel. Automatically reconnects after transient failures.
    /// </summary>
    private async Task StartListeningAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                _listenConnection = await _dataSource.OpenConnectionAsync(cancellationToken);
                _listenConnection.Notification += OnNotification;

                await using (NpgsqlCommand cmd = _listenConnection.CreateCommand())
                {
                    cmd.CommandText = $"LISTEN \"{_channelName}\"";
                    await cmd.ExecuteNonQueryAsync(cancellationToken);
                }

                _logger.LogInformation("PostgreSQL backplane listening on channel '{Channel}'", _channelName);

                // Keep the connection alive, processing notifications.
                while (!cancellationToken.IsCancellationRequested)
                {
                    await _listenConnection.WaitAsync(cancellationToken);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "PostgreSQL backplane listener encountered an error. Reconnecting in 5 s");
                _listenConnection?.Dispose();
                _listenConnection = null;

                try { await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken); }
                catch (OperationCanceledException) { break; }
            }
        }
    }

    /// <summary>
    /// Handles a PostgreSQL notification event. Inline payloads are deserialized directly
    /// into a <see cref="BackplaneMessage"/>; outbox-reference payloads (containing an
    /// <c>outboxId</c>) trigger an asynchronous fetch from the outbox table before dispatch.
    /// </summary>
    private void OnNotification(object sender, NpgsqlNotificationEventArgs e)
    {
        // Peek at the payload once to determine whether it's an inline message or an outbox reference.
        JsonDocument doc;
        try
        {
            doc = JsonDocument.Parse(e.Payload);
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(ex, "Failed to parse backplane notification payload");
            return;
        }

        try
        {
            if (doc.RootElement.ValueKind == JsonValueKind.Object
                && doc.RootElement.TryGetProperty("outboxId", out JsonElement idEl)
                && idEl.ValueKind == JsonValueKind.String
                && idEl.GetString() is { Length: > 0 } outboxId)
            {
                // Outbox reference: fetch and dispatch asynchronously to avoid blocking the listener loop.
                _ = HandleOutboxNotificationAsync(outboxId);
                return;
            }

            BackplaneMessage? message;
            try
            {
                message = doc.RootElement.Deserialize<BackplaneMessage>(JsonOptions);
            }
            catch (JsonException ex)
            {
                _logger.LogWarning(ex, "Failed to deserialize backplane message");
                return;
            }

            if (message is not null)
            {
                Dispatch(message);
            }
        }
        finally
        {
            doc.Dispose();
        }
    }

    /// <summary>
    /// Fetches an outbox-staged payload by ID, deserializes it, and dispatches it to local connections.
    /// Tolerates missing rows (e.g. cleaned up between NOTIFY emission and processing).
    /// </summary>
    private async Task HandleOutboxNotificationAsync(string outboxId)
    {
        try
        {
            string? payload = await ReadOutboxPayloadAsync(outboxId, _cts.Token);
            if (payload is null)
            {
                _logger.LogWarning("Outbox row '{OutboxId}' on channel '{Channel}' was missing or already cleaned up before it could be dispatched.", outboxId, _channelName);
                return;
            }

            BackplaneMessage? message;
            try
            {
                message = JsonSerializer.Deserialize<BackplaneMessage>(payload, JsonOptions);
            }
            catch (JsonException ex)
            {
                _logger.LogWarning(ex, "Failed to deserialize outbox-staged backplane message '{OutboxId}'", outboxId);
                return;
            }

            if (message is not null)
            {
                Dispatch(message);
            }
        }
        catch (OperationCanceledException) when (_cts.IsCancellationRequested)
        {
            // Manager is shutting down.
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to process outbox notification '{OutboxId}' on channel '{Channel}'", outboxId, _channelName);
        }
    }

    /// <summary>Routes a deserialized <see cref="BackplaneMessage"/> to the appropriate local-connection delivery helpers.</summary>
    private void Dispatch(BackplaneMessage message)
    {
        object?[] args = DeserializeArgs(message.Args);
        string[] excluded = message.ExcludedConnectionIds ?? [];

        switch (message.Type)
        {
            case BackplaneMessageType.All:
                DeliverToAll(message.MethodName, args, []);
                break;

            case BackplaneMessageType.AllExcept:
                DeliverToAll(message.MethodName, args, excluded);
                break;

            case BackplaneMessageType.Connection when message is { Filter: { } connectionId, MethodName: var methodName }:
                DeliverToConnection(connectionId, methodName, args);
                break;

            case BackplaneMessageType.Connections when message is { Filters: { } connectionIds, MethodName: var methodName }:
                foreach (string id in connectionIds)
                {
                    DeliverToConnection(id, methodName, args);
                }

                break;

            case BackplaneMessageType.Group when message is { Filter: { } groupName, MethodName: var methodName }:
                DeliverToGroup(groupName, methodName, args, excluded);
                break;

            case BackplaneMessageType.GroupExcept when message is { Filter: { } groupName, MethodName: var methodName }:
                DeliverToGroup(groupName, methodName, args, excluded);
                break;

            case BackplaneMessageType.Groups when message is { Filters: { } groupNames, MethodName: var methodName }:
                foreach (string group in groupNames)
                {
                    DeliverToGroup(group, methodName, args, []);
                }

                break;

            case BackplaneMessageType.User when message is { Filter: { } userId, MethodName: var methodName }:
                DeliverToUser(userId, methodName, args);
                break;

            case BackplaneMessageType.Users when message is { Filters: { } userIds, MethodName: var methodName }:
                foreach (string userId in userIds)
                {
                    DeliverToUser(userId, methodName, args);
                }

                break;
        }
    }

    /// <summary>Delivers a hub method invocation to all locally tracked connections, optionally excluding some.</summary>
    private void DeliverToAll(string methodName, object?[] args, IReadOnlyList<string> excluded)
    {
        HashSet<string>? excludedSet = excluded.Count > 0 ? new HashSet<string>(excluded, StringComparer.Ordinal) : null;

        foreach ((string id, HubConnectionContext connection) in _connections)
        {
            if (excludedSet is null || !excludedSet.Contains(id))
            {
                _ = WriteToConnectionAsync(connection, methodName, args);
            }
        }
    }

    /// <summary>Delivers a hub method invocation to a single locally tracked connection by its ID.</summary>
    private void DeliverToConnection(string connectionId, string methodName, object?[] args)
    {
        if (_connections.TryGetValue(connectionId, out HubConnectionContext? connection))
        {
            _ = WriteToConnectionAsync(connection, methodName, args);
        }
    }

    /// <summary>Delivers a hub method invocation to all locally tracked connections in a group, optionally excluding some.</summary>
    private void DeliverToGroup(string groupName, string methodName, object?[] args, IReadOnlyList<string> excluded)
    {
        if (!_groups.TryGetValue(groupName, out HubConnectionCtxDictionary? group))
        {
            return;
        }

        HashSet<string>? excludedSet = excluded.Count > 0 ? new HashSet<string>(excluded, StringComparer.Ordinal) : null;

        foreach ((string id, HubConnectionContext connection) in group)
        {
            if (excludedSet is null || !excludedSet.Contains(id))
            {
                _ = WriteToConnectionAsync(connection, methodName, args);
            }
        }
    }

    /// <summary>Delivers a hub method invocation to all locally tracked connections belonging to a user.</summary>
    private void DeliverToUser(string userId, string methodName, object?[] args)
    {
        if (!_users.TryGetValue(userId, out HubConnectionCtxDictionary? userConnections))
        {
            return;
        }

        foreach (HubConnectionContext connection in userConnections.Values)
        {
            _ = WriteToConnectionAsync(connection, methodName, args);
        }
    }

    /// <summary>Writes a hub method invocation message directly to the given connection, suppressing non-fatal transport errors.</summary>
    private async Task WriteToConnectionAsync(HubConnectionContext connection, string methodName, object?[] args)
    {
        try
        {
            await connection.WriteAsync(new InvocationMessage(methodName, args));
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to write backplane message to connection '{ConnectionId}'", connection.ConnectionId);
        }
    }

    // ── Disposal ─────────────────────────────────────────────────────────────

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();

        if (_listenTask is not null)
        {
            try { await _listenTask; }
            catch (OperationCanceledException) { /* expected */ }
        }

        if (_outboxInitTask is not null)
        {
            try { await _outboxInitTask; }
            catch { /* already logged; do not block disposal */ }
        }

        if (_listenConnection is not null)
        {
            await _listenConnection.DisposeAsync();
        }

        _cts.Dispose();
    }
}
