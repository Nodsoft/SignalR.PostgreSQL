using System.Collections.Concurrent;
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

    /// <summary>Cancellation source that shuts down the LISTEN loop on disposal.</summary>
    private readonly CancellationTokenSource _cts = new();

    /// <summary>Shared JSON serializer options used for both serializing and deserializing backplane payloads.</summary>
    private static readonly JsonSerializerOptions _jsonOptions = new(JsonSerializerDefaults.Web);

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

        _listenTask = StartListeningAsync(_cts.Token);
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
    /// Serializes <paramref name="message"/> as a JSON payload and publishes it
    /// to the PostgreSQL notification channel via <c>pg_notify</c>.
    /// Payloads exceeding the PostgreSQL 8 KB limit are dropped with a warning.
    /// </summary>
    private async Task PublishAsync(BackplaneMessage message, CancellationToken cancellationToken)
    {
        var payload = JsonSerializer.Serialize(message, _jsonOptions);

        // PostgreSQL NOTIFY payload is limited to ~8 KB.
        if (payload.Length > 8000)
        {
            _logger.LogWarning("BackplaneMessage payload exceeds 8 KB and will not be delivered via NOTIFY.");
            return;
        }

        await using var cmd = _dataSource.CreateCommand($"SELECT pg_notify('{_channelName}', @payload)");
        cmd.Parameters.AddWithValue("payload", payload);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
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
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
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
    /// Handles a PostgreSQL notification event by deserializing the payload into a
    /// <see cref="BackplaneMessage"/> and routing it to the appropriate local connections.
    /// </summary>
    private void OnNotification(object sender, NpgsqlNotificationEventArgs e)
    {
        BackplaneMessage? message;

        try
        {
            message = JsonSerializer.Deserialize<BackplaneMessage>(e.Payload, _jsonOptions);
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

        if (_listenConnection is not null)
        {
            await _listenConnection.DisposeAsync();
        }

        _cts.Dispose();
    }
}
