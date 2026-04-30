using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.SignalR;
using Microsoft.AspNetCore.SignalR.Protocol;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Nodsoft.AspNetCore.SignalR.PostgreSQL.Internal;
using Npgsql;

namespace Nodsoft.AspNetCore.SignalR.PostgreSQL;

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

    /// <summary>All active connections on this server instance, keyed by connection ID.</summary>
    private readonly ConcurrentDictionary<string, HubConnectionContext> _connections = new(StringComparer.Ordinal);

    /// <summary>Group membership map: group name → (connection ID → connection).</summary>
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, HubConnectionContext>> _groups
        = new(StringComparer.Ordinal);

    /// <summary>User connection map: user identifier → (connection ID → connection).</summary>
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, HubConnectionContext>> _users
        = new(StringComparer.Ordinal);

    /// <summary>Npgsql data source used for opening NOTIFY command connections and the LISTEN connection.</summary>
    private readonly NpgsqlDataSource _dataSource;

    /// <summary>Logger for diagnostic output.</summary>
    private readonly ILogger<PostgreSqlHubLifetimeManager<THub>> _logger;

    /// <summary>The dedicated connection used for the background LISTEN loop.</summary>
    private NpgsqlConnection? _listenConnection;

    /// <summary>The background task running the LISTEN loop.</summary>
    private Task? _listenTask;

    /// <summary>The background task running the outbox cleanup loop, if enabled.</summary>
    private Task? _cleanupTask;

    /// <summary>Cancellation source that shuts down the LISTEN loop on disposal.</summary>
    private readonly CancellationTokenSource _cts = new();

    /// <summary>Shared JSON serializer options used for both serializing and deserializing backplane payloads.</summary>
    private static readonly JsonSerializerOptions _jsonOptions = new(JsonSerializerDefaults.Web);

    /// <summary>Effective backplane options captured at construction time.</summary>
    private readonly PostgreSqlBackplaneOptions _options;

    /// <summary>The validated, fully-qualified outbox table identifier safe for SQL interpolation.</summary>
    private readonly string _outboxTable;

    /// <summary>NOTIFY payload tag indicating the payload itself is the inline JSON message.</summary>
    private const string InlineTag = "I:";

    /// <summary>NOTIFY payload tag indicating the payload is a reference to an outbox row by UUID.</summary>
    private const string ReferenceTag = "R:";

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

        var opts = options.Value;
        _options = opts;
        _dataSource = opts.DataSource
            ?? (opts.ConnectionString is not null
                ? NpgsqlDataSource.Create(opts.ConnectionString)
                : throw new InvalidOperationException(
                    "Either DataSource or ConnectionString must be set in PostgreSqlBackplaneOptions."));

        // Validate the channel name to prevent SQL injection via interpolation.
        // Channel names are derived from hub type names and must only contain safe identifier characters.
        var rawHubName = typeof(THub).Name.ToLowerInvariant();
        if (!System.Text.RegularExpressions.Regex.IsMatch(rawHubName, @"^[a-z0-9_]+$"))
        {
            throw new InvalidOperationException(
                $"Hub type name '{typeof(THub).Name}' contains characters that are not allowed in a PostgreSQL channel name.");
        }

        _channelName = $"signalr__{rawHubName}";

        // Validate the outbox table identifier. A schema-qualified name is allowed,
        // but each component must match a safe identifier pattern to be used directly in SQL.
        var rawTable = (opts.OutboxTableName ?? string.Empty).Trim();
        if (!System.Text.RegularExpressions.Regex.IsMatch(rawTable, @"^[a-z_][a-z0-9_]*(\.[a-z_][a-z0-9_]*)?$"))
        {
            throw new InvalidOperationException(
                $"OutboxTableName '{opts.OutboxTableName}' is not a valid PostgreSQL identifier. " +
                "Use lowercase letters, digits, and underscores; an optional schema prefix is allowed.");
        }

        _outboxTable = rawTable;

        if (opts.OutboxThresholdBytes <= 0 || opts.OutboxThresholdBytes > 7_900)
        {
            throw new InvalidOperationException(
                "OutboxThresholdBytes must be between 1 and 7900 (PostgreSQL NOTIFY hard limit is 8000 bytes).");
        }

        _listenTask = StartListeningAsync(_cts.Token);

        if (opts.OutboxCleanupInterval > TimeSpan.Zero)
        {
            _cleanupTask = RunOutboxCleanupAsync(_cts.Token);
        }
    }

    // ── Connection lifecycle ────────────────────────────────────────────────

    /// <inheritdoc/>
    public override Task OnConnectedAsync(HubConnectionContext connection)
    {
        _connections[connection.ConnectionId] = connection;

        if (connection.UserIdentifier is { } userId)
        {
            _users.GetOrAdd(userId, _ => new ConcurrentDictionary<string, HubConnectionContext>(StringComparer.Ordinal))
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
            && _users.TryGetValue(userId, out var userConnections))
        {
            userConnections.TryRemove(connection.ConnectionId, out _);
        }

        foreach (var group in _groups.Values)
        {
            group.TryRemove(connection.ConnectionId, out _);
        }

        return Task.CompletedTask;
    }

    // ── Group management ────────────────────────────────────────────────────

    /// <inheritdoc/>
    public override Task AddToGroupAsync(string connectionId, string groupName, CancellationToken cancellationToken = default)
    {
        if (_connections.TryGetValue(connectionId, out var connection))
        {
            _groups.GetOrAdd(groupName, _ => new ConcurrentDictionary<string, HubConnectionContext>(StringComparer.Ordinal))
                [connectionId] = connection;
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc/>
    public override Task RemoveFromGroupAsync(string connectionId, string groupName, CancellationToken cancellationToken = default)
    {
        if (_groups.TryGetValue(groupName, out var group))
        {
            group.TryRemove(connectionId, out _);
        }

        return Task.CompletedTask;
    }

    // ── Send methods ────────────────────────────────────────────────────────

    /// <inheritdoc/>
    public override Task SendAllAsync(string methodName, object?[] args, CancellationToken cancellationToken = default)
        => PublishAsync(new BackplaneMessage
        {
            ServerInstanceId = _serverInstanceId,
            Type = BackplaneMessageType.All,
            MethodName = methodName,
            Args = SerializeArgs(args),
        }, cancellationToken);

    /// <inheritdoc/>
    public override Task SendAllExceptAsync(string methodName, object?[] args, IReadOnlyList<string> excludedConnectionIds, CancellationToken cancellationToken = default)
        => PublishAsync(new BackplaneMessage
        {
            ServerInstanceId = _serverInstanceId,
            Type = BackplaneMessageType.AllExcept,
            MethodName = methodName,
            Args = SerializeArgs(args),
            ExcludedConnectionIds = [.. excludedConnectionIds],
        }, cancellationToken);

    /// <inheritdoc/>
    public override Task SendConnectionAsync(string connectionId, string methodName, object?[] args, CancellationToken cancellationToken = default)
        => PublishAsync(new BackplaneMessage
        {
            ServerInstanceId = _serverInstanceId,
            Type = BackplaneMessageType.Connection,
            MethodName = methodName,
            Args = SerializeArgs(args),
            Filter = connectionId,
        }, cancellationToken);

    /// <inheritdoc/>
    public override Task SendConnectionsAsync(IReadOnlyList<string> connectionIds, string methodName, object?[] args, CancellationToken cancellationToken = default)
        => PublishAsync(new BackplaneMessage
        {
            ServerInstanceId = _serverInstanceId,
            Type = BackplaneMessageType.Connections,
            MethodName = methodName,
            Args = SerializeArgs(args),
            Filters = [.. connectionIds],
        }, cancellationToken);

    /// <inheritdoc/>
    public override Task SendGroupAsync(string groupName, string methodName, object?[] args, CancellationToken cancellationToken = default)
        => PublishAsync(new BackplaneMessage
        {
            ServerInstanceId = _serverInstanceId,
            Type = BackplaneMessageType.Group,
            MethodName = methodName,
            Args = SerializeArgs(args),
            Filter = groupName,
        }, cancellationToken);

    /// <inheritdoc/>
    public override Task SendGroupExceptAsync(string groupName, string methodName, object?[] args, IReadOnlyList<string> excludedConnectionIds, CancellationToken cancellationToken = default)
        => PublishAsync(new BackplaneMessage
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
        => PublishAsync(new BackplaneMessage
        {
            ServerInstanceId = _serverInstanceId,
            Type = BackplaneMessageType.Groups,
            MethodName = methodName,
            Args = SerializeArgs(args),
            Filters = [.. groupNames],
        }, cancellationToken);

    /// <inheritdoc/>
    public override Task SendUserAsync(string userId, string methodName, object?[] args, CancellationToken cancellationToken = default)
        => PublishAsync(new BackplaneMessage
        {
            ServerInstanceId = _serverInstanceId,
            Type = BackplaneMessageType.User,
            MethodName = methodName,
            Args = SerializeArgs(args),
            Filter = userId,
        }, cancellationToken);

    /// <inheritdoc/>
    public override Task SendUsersAsync(IReadOnlyList<string> userIds, string methodName, object?[] args, CancellationToken cancellationToken = default)
        => PublishAsync(new BackplaneMessage
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
    /// notification channel. Payloads whose UTF-8 byte size is at or below
    /// <see cref="PostgreSqlBackplaneOptions.OutboxThresholdBytes"/> are sent inline via <c>NOTIFY</c>
    /// (no DB write). Larger payloads are persisted to the outbox table and a small reference
    /// (<c>R:&lt;uuid&gt;</c>) is published via <c>NOTIFY</c> instead, allowing arbitrarily large
    /// messages to traverse the backplane without hitting the 8 KB <c>NOTIFY</c> limit.
    /// </summary>
    private async Task PublishAsync(BackplaneMessage message, CancellationToken cancellationToken)
    {
        var payload = JsonSerializer.Serialize(message, _jsonOptions);
        var payloadByteCount = Encoding.UTF8.GetByteCount(payload);

        if (payloadByteCount <= _options.OutboxThresholdBytes)
        {
            // Fast path: send the payload directly via NOTIFY, no DB write.
            await using var cmd = _dataSource.CreateCommand($"SELECT pg_notify('{_channelName}', @payload)");
            cmd.Parameters.AddWithValue("payload", InlineTag + payload);
            await cmd.ExecuteNonQueryAsync(cancellationToken);
            return;
        }

        // Outbox path: persist the payload and publish a reference. Done in a single round-trip
        // via a CTE so the INSERT and pg_notify are issued in one statement.
        var sql =
            $"""
             WITH ins AS (
                 INSERT INTO {_outboxTable} (id, payload)
                 VALUES (gen_random_uuid(), @payload)
                 RETURNING id
             )
             SELECT pg_notify('{_channelName}', '{ReferenceTag}' || id::text) FROM ins
             """;

        await using var outboxCmd = _dataSource.CreateCommand(sql);
        outboxCmd.Parameters.AddWithValue("payload", payload);
        await outboxCmd.ExecuteNonQueryAsync(cancellationToken);

        _logger.LogDebug(
            "Backplane payload of {Bytes} bytes exceeded inline threshold ({Threshold}); persisted to outbox '{Table}'.",
            payloadByteCount, _options.OutboxThresholdBytes, _outboxTable);
    }

    /// <summary>Serializes the given argument array to <see cref="JsonElement"/> values suitable for JSON transport.</summary>
    private static JsonElement[] SerializeArgs(object?[] args)
        => args.Select(a => JsonSerializer.SerializeToElement(a, _jsonOptions)).ToArray();

    /// <summary>Converts deserialized <see cref="JsonElement"/> values back to an object array for hub invocation.</summary>
    private static object?[] DeserializeArgs(JsonElement[] elements)
        => elements.Select(e => (object?)e).ToArray();

    /// <summary>
    /// Background loop that opens a dedicated PostgreSQL connection and issues a LISTEN command
    /// for the hub's notification channel. Automatically reconnects after transient failures.
    /// </summary>
    private async Task StartListeningAsync(CancellationToken cancellationToken)
    {
        // Ensure the outbox table exists once before opening the listen connection.
        // The CREATE TABLE/INDEX statements are idempotent, so retrying on reconnect is harmless,
        // but we only need to do this once on initial startup.
        var schemaInitialized = false;

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                if (!schemaInitialized)
                {
                    await EnsureOutboxTableAsync(cancellationToken);
                    schemaInitialized = true;
                }

                _listenConnection = await _dataSource.OpenConnectionAsync(cancellationToken);
                _listenConnection.Notification += OnNotification;

                await using (var cmd = _listenConnection.CreateCommand())
                {
                    cmd.CommandText = $"LISTEN \"{_channelName}\"";
                    await cmd.ExecuteNonQueryAsync(cancellationToken);
                }

                _logger.LogInformation("PostgreSQL backplane listening on channel '{Channel}'.", _channelName);

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
                _logger.LogError(ex, "PostgreSQL backplane listener encountered an error. Reconnecting in 5 s.");
                _listenConnection?.Dispose();
                _listenConnection = null;

                try { await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken); }
                catch (OperationCanceledException) { break; }
            }
        }
    }

    /// <summary>
    /// Handles a PostgreSQL notification event. The payload is tagged with one of:
    /// <list type="bullet">
    /// <item><c>I:&lt;json&gt;</c> — the JSON body follows directly and is processed inline.</item>
    /// <item><c>R:&lt;uuid&gt;</c> — the body is a reference to a row in the outbox table whose payload must be fetched.</item>
    /// </list>
    /// Reference resolution is dispatched to a fire-and-forget task because the Npgsql notification
    /// callback is synchronous and must not block the LISTEN connection.
    /// </summary>
    private void OnNotification(object sender, NpgsqlNotificationEventArgs e)
    {
        var raw = e.Payload;

        if (raw.StartsWith(InlineTag, StringComparison.Ordinal))
        {
            ProcessPayload(raw.AsSpan(InlineTag.Length).ToString());
            return;
        }

        if (raw.StartsWith(ReferenceTag, StringComparison.Ordinal))
        {
            var idText = raw[ReferenceTag.Length..];
            if (!Guid.TryParse(idText, out var id))
            {
                _logger.LogWarning("Received outbox reference notification with invalid UUID '{Id}'.", idText);
                return;
            }

            _ = ResolveAndProcessOutboxAsync(id, _cts.Token);
            return;
        }

        _logger.LogWarning("Received backplane notification with unrecognized tag; dropping.");
    }

    /// <summary>
    /// Fetches an outbox payload by id and routes it through <see cref="ProcessPayload"/>.
    /// Logs and swallows any failures; the cleanup loop will reclaim the row on its TTL.
    /// </summary>
    private async Task ResolveAndProcessOutboxAsync(Guid id, CancellationToken cancellationToken)
    {
        try
        {
            await using var cmd = _dataSource.CreateCommand($"SELECT payload FROM {_outboxTable} WHERE id = @id");
            cmd.Parameters.AddWithValue("id", id);

            var result = await cmd.ExecuteScalarAsync(cancellationToken);
            if (result is string payload)
            {
                ProcessPayload(payload);
            }
            else
            {
                _logger.LogWarning(
                    "Outbox row '{Id}' was not found when resolving a backplane reference. " +
                    "It may have been purged by the cleanup loop before delivery completed.",
                    id);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // shutdown
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to resolve outbox payload for id '{Id}'.", id);
        }
    }

    /// <summary>
    /// Deserializes a backplane payload JSON string and routes it to the appropriate local connections.
    /// </summary>
    private void ProcessPayload(string payload)
    {
        BackplaneMessage? message;

        try
        {
            message = JsonSerializer.Deserialize<BackplaneMessage>(payload, _jsonOptions);
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(ex, "Failed to deserialize backplane message.");
            return;
        }

        if (message is null)
        {
            return;
        }

        var args = DeserializeArgs(message.Args);
        var excluded = message.ExcludedConnectionIds ?? [];

        switch (message.Type)
        {
            case BackplaneMessageType.All:
                DeliverToAll(message.MethodName, args, []);
                break;

            case BackplaneMessageType.AllExcept:
                DeliverToAll(message.MethodName, args, excluded);
                break;

            case BackplaneMessageType.Connection when message.Filter is not null:
                DeliverToConnection(message.Filter, message.MethodName, args);
                break;

            case BackplaneMessageType.Connections when message.Filters is not null:
                foreach (var id in message.Filters)
                {
                    DeliverToConnection(id, message.MethodName, args);
                }

                break;

            case BackplaneMessageType.Group when message.Filter is not null:
                DeliverToGroup(message.Filter, message.MethodName, args, []);
                break;

            case BackplaneMessageType.GroupExcept when message.Filter is not null:
                DeliverToGroup(message.Filter, message.MethodName, args, excluded);
                break;

            case BackplaneMessageType.Groups when message.Filters is not null:
                foreach (var group in message.Filters)
                {
                    DeliverToGroup(group, message.MethodName, args, []);
                }

                break;

            case BackplaneMessageType.User when message.Filter is not null:
                DeliverToUser(message.Filter, message.MethodName, args);
                break;

            case BackplaneMessageType.Users when message.Filters is not null:
                foreach (var userId in message.Filters)
                {
                    DeliverToUser(userId, message.MethodName, args);
                }

                break;
        }
    }

    /// <summary>
    /// Idempotently creates the outbox table and its supporting index if they do not yet exist.
    /// Invoked once on startup; safe for concurrent execution by multiple server instances.
    /// </summary>
    private async Task EnsureOutboxTableAsync(CancellationToken cancellationToken)
    {
        // Index name must not be schema-qualified, so derive it from the unqualified table name.
        // The validation regex permits at most one dot, so a single IndexOf is sufficient.
        var unqualifiedTable = _outboxTable.Contains('.', StringComparison.Ordinal)
            ? _outboxTable[(_outboxTable.IndexOf('.', StringComparison.Ordinal) + 1)..]
            : _outboxTable;

        var sql =
            $"""
             CREATE TABLE IF NOT EXISTS {_outboxTable} (
                 id UUID PRIMARY KEY,
                 payload TEXT NOT NULL,
                 created_at TIMESTAMPTZ NOT NULL DEFAULT now()
             );
             CREATE INDEX IF NOT EXISTS {unqualifiedTable}_created_at_idx ON {_outboxTable} (created_at);
             """;

        await using var cmd = _dataSource.CreateCommand(sql);
        await cmd.ExecuteNonQueryAsync(cancellationToken);

        _logger.LogInformation("PostgreSQL backplane outbox table '{Table}' is ready.", _outboxTable);
    }

    /// <summary>
    /// Background loop that periodically deletes outbox rows older than
    /// <see cref="PostgreSqlBackplaneOptions.OutboxRetention"/>. Errors are logged and the loop continues.
    /// </summary>
    private async Task RunOutboxCleanupAsync(CancellationToken cancellationToken)
    {
        var interval = _options.OutboxCleanupInterval;
        var retention = _options.OutboxRetention;

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(interval, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            try
            {
                // Compute cutoff server-side using PostgreSQL's now() so the threshold reflects the
                // actual execution time rather than the moment the command was constructed.
                await using var cmd = _dataSource.CreateCommand(
                    $"DELETE FROM {_outboxTable} WHERE created_at < now() - @retention");
                cmd.Parameters.AddWithValue("retention", retention);

                var deleted = await cmd.ExecuteNonQueryAsync(cancellationToken);
                if (deleted > 0)
                {
                    _logger.LogDebug("Outbox cleanup deleted {Count} expired row(s) from '{Table}'.", deleted, _outboxTable);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Outbox cleanup iteration failed; will retry after {Interval}.", interval);
            }
        }
    }

    /// <summary>Delivers a hub method invocation to all locally tracked connections, optionally excluding some.</summary>
    private void DeliverToAll(string methodName, object?[] args, IReadOnlyList<string> excluded)
    {
        var excludedSet = excluded.Count > 0 ? new HashSet<string>(excluded, StringComparer.Ordinal) : null;

        foreach (var (id, connection) in _connections)
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
        if (_connections.TryGetValue(connectionId, out var connection))
        {
            _ = WriteToConnectionAsync(connection, methodName, args);
        }
    }

    /// <summary>Delivers a hub method invocation to all locally tracked connections in a group, optionally excluding some.</summary>
    private void DeliverToGroup(string groupName, string methodName, object?[] args, IReadOnlyList<string> excluded)
    {
        if (_groups.TryGetValue(groupName, out var group))
        {
            var excludedSet = excluded.Count > 0 ? new HashSet<string>(excluded, StringComparer.Ordinal) : null;

            foreach (var (id, connection) in group)
            {
                if (excludedSet is null || !excludedSet.Contains(id))
                {
                    _ = WriteToConnectionAsync(connection, methodName, args);
                }
            }
        }
    }

    /// <summary>Delivers a hub method invocation to all locally tracked connections belonging to a user.</summary>
    private void DeliverToUser(string userId, string methodName, object?[] args)
    {
        if (_users.TryGetValue(userId, out var userConnections))
        {
            foreach (var connection in userConnections.Values)
            {
                _ = WriteToConnectionAsync(connection, methodName, args);
            }
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
            _logger.LogWarning(ex, "Failed to write backplane message to connection '{ConnectionId}'.", connection.ConnectionId);
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

        if (_cleanupTask is not null)
        {
            try { await _cleanupTask; }
            catch (OperationCanceledException) { /* expected */ }
        }

        if (_listenConnection is not null)
        {
            await _listenConnection.DisposeAsync();
        }

        _cts.Dispose();
    }
}
