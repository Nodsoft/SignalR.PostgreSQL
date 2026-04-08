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
    private readonly string _serverInstanceId = Guid.NewGuid().ToString("N");
    private readonly string _channelName;

    private readonly ConcurrentDictionary<string, HubConnectionContext> _connections = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, HubConnectionContext>> _groups
        = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, HubConnectionContext>> _users
        = new(StringComparer.Ordinal);

    private readonly NpgsqlDataSource _dataSource;
    private readonly ILogger<PostgreSqlHubLifetimeManager<THub>> _logger;

    private NpgsqlConnection? _listenConnection;
    private Task? _listenTask;
    private readonly CancellationTokenSource _cts = new();

    private static readonly JsonSerializerOptions _jsonOptions = new(JsonSerializerDefaults.Web);

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

    public override Task AddToGroupAsync(string connectionId, string groupName, CancellationToken cancellationToken = default)
    {
        if (_connections.TryGetValue(connectionId, out var connection))
        {
            _groups.GetOrAdd(groupName, _ => new ConcurrentDictionary<string, HubConnectionContext>(StringComparer.Ordinal))
                [connectionId] = connection;
        }

        return Task.CompletedTask;
    }

    public override Task RemoveFromGroupAsync(string connectionId, string groupName, CancellationToken cancellationToken = default)
    {
        if (_groups.TryGetValue(groupName, out var group))
        {
            group.TryRemove(connectionId, out _);
        }

        return Task.CompletedTask;
    }

    // ── Send methods ────────────────────────────────────────────────────────

    public override Task SendAllAsync(string methodName, object?[] args, CancellationToken cancellationToken = default)
        => PublishAsync(new BackplaneMessage
        {
            ServerInstanceId = _serverInstanceId,
            Type = BackplaneMessageType.All,
            MethodName = methodName,
            Args = SerializeArgs(args),
        }, cancellationToken);

    public override Task SendAllExceptAsync(string methodName, object?[] args, IReadOnlyList<string> excludedConnectionIds, CancellationToken cancellationToken = default)
        => PublishAsync(new BackplaneMessage
        {
            ServerInstanceId = _serverInstanceId,
            Type = BackplaneMessageType.AllExcept,
            MethodName = methodName,
            Args = SerializeArgs(args),
            ExcludedConnectionIds = [.. excludedConnectionIds],
        }, cancellationToken);

    public override Task SendConnectionAsync(string connectionId, string methodName, object?[] args, CancellationToken cancellationToken = default)
        => PublishAsync(new BackplaneMessage
        {
            ServerInstanceId = _serverInstanceId,
            Type = BackplaneMessageType.Connection,
            MethodName = methodName,
            Args = SerializeArgs(args),
            Filter = connectionId,
        }, cancellationToken);

    public override Task SendConnectionsAsync(IReadOnlyList<string> connectionIds, string methodName, object?[] args, CancellationToken cancellationToken = default)
        => PublishAsync(new BackplaneMessage
        {
            ServerInstanceId = _serverInstanceId,
            Type = BackplaneMessageType.Connections,
            MethodName = methodName,
            Args = SerializeArgs(args),
            Filters = [.. connectionIds],
        }, cancellationToken);

    public override Task SendGroupAsync(string groupName, string methodName, object?[] args, CancellationToken cancellationToken = default)
        => PublishAsync(new BackplaneMessage
        {
            ServerInstanceId = _serverInstanceId,
            Type = BackplaneMessageType.Group,
            MethodName = methodName,
            Args = SerializeArgs(args),
            Filter = groupName,
        }, cancellationToken);

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

    public override Task SendGroupsAsync(IReadOnlyList<string> groupNames, string methodName, object?[] args, CancellationToken cancellationToken = default)
        => PublishAsync(new BackplaneMessage
        {
            ServerInstanceId = _serverInstanceId,
            Type = BackplaneMessageType.Groups,
            MethodName = methodName,
            Args = SerializeArgs(args),
            Filters = [.. groupNames],
        }, cancellationToken);

    public override Task SendUserAsync(string userId, string methodName, object?[] args, CancellationToken cancellationToken = default)
        => PublishAsync(new BackplaneMessage
        {
            ServerInstanceId = _serverInstanceId,
            Type = BackplaneMessageType.User,
            MethodName = methodName,
            Args = SerializeArgs(args),
            Filter = userId,
        }, cancellationToken);

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

    private static JsonElement[] SerializeArgs(object?[] args)
        => args.Select(a => JsonSerializer.SerializeToElement(a, _jsonOptions)).ToArray();

    private static object?[] DeserializeArgs(JsonElement[] elements)
        => elements.Select(e => (object?)e).ToArray();

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

    private void DeliverToConnection(string connectionId, string methodName, object?[] args)
    {
        if (_connections.TryGetValue(connectionId, out var connection))
        {
            _ = WriteToConnectionAsync(connection, methodName, args);
        }
    }

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
