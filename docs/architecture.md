# Architecture

This document describes the internal design of `Nodsoft.AspNetCore.SignalR.PostgreSQL` and explains how PostgreSQL LISTEN/NOTIFY is used to implement a SignalR backplane.

&nbsp;

## Background: what is a SignalR backplane?

ASP.NET Core SignalR maintains connection state entirely in process. Each server instance holds an in-memory map of active `HubConnectionContext` objects. When you call `Clients.All.MyMethod(args)`, the default `DefaultHubLifetimeManager<THub>` iterates that map and writes directly to each connection.

In a single-server deployment this is fine. When you scale horizontally — multiple instances behind a load balancer — a client on server A can only receive messages dispatched **by server A**. Calling `Clients.All.MyMethod` on server B never reaches server A's clients.

A **backplane** fixes this by replacing the `HubLifetimeManager<THub>` with a distributed implementation that publishes outbound messages to a shared bus and subscribes to incoming messages from all peers.

```
Without backplane                   With PostgreSQL backplane

Server A ──► Client A1              Server A ──► Client A1
Server A ──► Client A2              Server A ──► Client A2
                                    Server A ─── pg_notify ──► Server B ──► Client B1
Server B ──► Client B1              Server A ─── pg_notify ──► Server B ──► Client B2
Server B ──► Client B2
```

&nbsp;

## Component overview

```
Nodsoft.AspNetCore.SignalR.PostgreSQL
├── PostgreSqlHubLifetimeManager<THub>         (public) – replaces DefaultHubLifetimeManager
├── PostgreSqlBackplaneOptions                 (public) – configuration
├── PostgreSqlSignalRBuilderExtensions         (public) – ISignalRServerBuilder extensions
└── Internal/
    ├── BackplaneMessage                       (internal record) – wire format
    └── BackplaneMessageType                   (internal enum)   – routing strategy
```

### `PostgreSqlHubLifetimeManager<THub>`

The central class. Registered as a singleton `HubLifetimeManager<THub>` by `AddPostgreSqlBackplane`. It:

1. Manages in-memory state for locally connected clients (connections, groups, users).
2. Publishes every outbound `Send*` call to PostgreSQL via `pg_notify`.
3. Receives inbound notifications from PostgreSQL and dispatches them to local connections.

### `BackplaneMessage`

The JSON wire format. All fields are serialised with `JsonSerializerDefaults.Web` (camelCase property names).

| Field | Type | Description |
|---|---|---|
| `serverInstanceId` | `string` | GUID of the originating server instance. Receivers skip messages whose `serverInstanceId` matches their own. |
| `type` | `BackplaneMessageType` (byte) | Routing strategy (see below). |
| `methodName` | `string` | Hub client method to invoke. |
| `args` | `JsonElement[]` | Serialised arguments for the method. |
| `filter` | `string?` | Single-value routing target: connection ID, group name, or user ID (depends on `type`). |
| `excludedConnectionIds` | `string[]?` | Connections to skip. Used for `AllExcept` and `GroupExcept`. |
| `filters` | `string[]?` | Multi-value routing targets: connection IDs, group names, or user IDs (depends on `type`). |

### `BackplaneMessageType`

Maps one-to-one to the `Send*` methods on `HubLifetimeManager<THub>`:

| Enum value | `HubLifetimeManager` method | Routing |
|---|---|---|
| `All` | `SendAllAsync` | Every local connection |
| `AllExcept` | `SendAllExceptAsync` | Every local connection not in `excludedConnectionIds` |
| `Connection` | `SendConnectionAsync` | Single connection matching `filter` |
| `Connections` | `SendConnectionsAsync` | All connections whose IDs are in `filters` |
| `Group` | `SendGroupAsync` | All local connections in the group named by `filter` |
| `GroupExcept` | `SendGroupExceptAsync` | Group connections not in `excludedConnectionIds` |
| `Groups` | `SendGroupsAsync` | All local connections in any group listed in `filters` |
| `User` | `SendUserAsync` | All local connections whose user identifier matches `filter` |
| `Users` | `SendUsersAsync` | All local connections whose user identifier is in `filters` |

&nbsp;

## In-memory state

Each `PostgreSqlHubLifetimeManager<THub>` instance maintains three `ConcurrentDictionary` structures:

```
_connections  : connectionId  → HubConnectionContext
_groups       : groupName     → { connectionId → HubConnectionContext }
_users        : userId        → { connectionId → HubConnectionContext }
```

These are populated/cleaned up by the standard `OnConnectedAsync`, `OnDisconnectedAsync`, `AddToGroupAsync`, and `RemoveFromGroupAsync` lifecycle hooks. The state is **local only** — it reflects only the clients connected to the current server instance.

> **Implication:** group membership is not shared across server instances. A `SendGroupAsync` from server A only delivers to the clients in that group that happen to be connected to server A. This is expected and correct behaviour when sticky sessions are used; see [Limitations](../README.md#limitations).

&nbsp;

## Message flow

### Publish path

```
Hub method (e.g. Clients.All.Chat("hello"))
  │
  ▼
HubLifetimeManager<THub>.SendAllAsync(...)
  │
  ▼
PostgreSqlHubLifetimeManager<THub>.SendAllAsync(...)
  │  builds BackplaneMessage { Type=All, MethodName="Chat", Args=[...] }
  ▼
PublishAsync(message, ct)
  │  JsonSerializer.Serialize → payload string
  │  guard: payload.Length > 8000 → LogWarning + return
  ▼
NpgsqlDataSource.CreateCommand("SELECT pg_notify(@channel, @payload)")
  │  execute non-query
  ▼
PostgreSQL broadcasts NOTIFY to all LISTEN subscribers
```

The publisher opens a **new short-lived connection** per `pg_notify` call (using the pooled `NpgsqlDataSource`). This keeps the LISTEN connection free for receiving.

### Subscribe / receive path

```
StartListeningAsync (background Task, started in ctor)
  │
  ▼
_dataSource.OpenConnectionAsync()   ← dedicated long-lived connection
  │
  ▼
LISTEN "signalr__{hubname}"
  │
  ├── NpgsqlConnection.WaitAsync(ct)   ← blocks until notification or cancellation
  │
  ▼
NpgsqlConnection.Notification event fires
  │
  ▼
OnNotification(sender, NpgsqlNotificationEventArgs e)
  │  JsonSerializer.Deserialize<BackplaneMessage>(e.Payload)
  │  guard: message.ServerInstanceId == _serverInstanceId → skip
  ▼
Route by message.Type → DeliverTo*(methodName, args, excluded)
  │
  ▼
WriteToConnectionAsync(connection, methodName, args)
  │  connection.WriteAsync(new InvocationMessage(methodName, args))
  ▼
Client receives the hub method call
```

&nbsp;

## Channel naming

The PostgreSQL notification channel is derived from the hub type name:

```
channel = "signalr__" + typeof(THub).Name.ToLowerInvariant()
```

Examples:

| Hub class | Channel name |
|---|---|
| `ChatHub` | `signalr__chathub` |
| `NotificationHub` | `signalr__notificationhub` |
| `MyApp.Hubs.GameHub` | `signalr__gamehub` |

Only characters in `[a-z0-9_]` are permitted. A hub whose lower-cased name contains any other character (e.g. a generic hub with angle brackets) causes an `InvalidOperationException` at startup. This validation prevents channel name injection through the `pg_notify` call.

&nbsp;

## Reconnection

The LISTEN loop is wrapped in a `try/catch` with automatic reconnection:

```
while (!cancellationToken.IsCancellationRequested)
{
    try
    {
        open connection → LISTEN → WaitAsync loop
    }
    catch (OperationCanceledException) when (ct.IsCancellationRequested)
    {
        break;   // clean shutdown
    }
    catch (Exception ex)
    {
        LogError(ex, "Reconnecting in 5 s");
        dispose connection;
        await Task.Delay(5s, ct);
        // loop → retry
    }
}
```

Any transient PostgreSQL disconnection causes a 5-second pause then a reconnect attempt. Notifications published during the reconnect window are **lost** (PostgreSQL does not buffer NOTIFY for disconnected listeners).

&nbsp;

## Disposal

`PostgreSqlHubLifetimeManager<THub>` implements `IAsyncDisposable`. On disposal:

1. `CancellationTokenSource.CancelAsync()` — signals the LISTEN loop to stop.
2. Awaits `_listenTask` (catching `OperationCanceledException`).
3. Disposes the dedicated LISTEN connection.
4. Disposes the `CancellationTokenSource`.

The `NpgsqlDataSource` is **not** disposed by the manager — its lifetime is controlled by the DI container that owns it.

&nbsp;

## Thread safety

- All three in-memory dictionaries are `ConcurrentDictionary` instances, making reads and writes from connection lifecycle callbacks thread-safe.
- `OnNotification` is invoked on the thread that calls `NpgsqlConnection.WaitAsync`. Delivery (`WriteToConnectionAsync`) is fire-and-forget (`_ = WriteToConnectionAsync(...)`), meaning individual message writes do not block the listener loop.
- Concurrent `Send*` calls each acquire a new pooled connection from `NpgsqlDataSource`, so there is no contention on the LISTEN connection.

&nbsp;

## Dependency diagram

```
Nodsoft.AspNetCore.SignalR.PostgreSQL
  ├── Microsoft.AspNetCore.App (FrameworkReference)
  │     └── Microsoft.AspNetCore.SignalR.Core
  └── Npgsql (10.x)
```

No other runtime dependencies.
