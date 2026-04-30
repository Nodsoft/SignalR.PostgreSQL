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
    ├── BackplaneMessageType                   (internal enum)   – routing strategy
    └── OutboxNotification                     (internal record) – small reference payload sent through NOTIFY when a message is staged in the outbox table
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
  │  JsonSerializer.Serialize → payload
  │  Encoding.UTF8.GetByteCount(payload) → byteLen
  │
  ├── byteLen ≤ InlinePayloadThresholdBytes  → PublishInlineAsync
  │       │
  │       ▼
  │     NpgsqlDataSource.CreateCommand("SELECT pg_notify(@channel, @payload)")
  │       │  execute non-query  ← single round-trip, no extra writes
  │       ▼
  │     PostgreSQL broadcasts NOTIFY to all LISTEN subscribers
  │
  └── byteLen > InlinePayloadThresholdBytes  → PublishViaOutboxAsync   (when UseOutbox = true)
          │                                          (else: LogWarning + drop)
          ▼
        WITH ins AS (
          INSERT INTO {OutboxTableName}(id, channel, payload) VALUES (@id, @channel, @payload) RETURNING 1
        ) SELECT pg_notify(@channel, @marker) FROM ins;
          │  the INSERT and NOTIFY share one transaction so listeners see the row
          │  by the time their NOTIFY fires
          │
          ▼
        Schedule fire-and-forget DELETE after OutboxExpiry → row cleanup
```

The publisher opens a **new short-lived connection** per `pg_notify` (or combined `INSERT`+`NOTIFY`) call from the pooled `NpgsqlDataSource`. This keeps the LISTEN connection free for receiving.

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
  │  JsonDocument.Parse(e.Payload)   ← single parse
  │
  ├── root has "outboxId" string property → HandleOutboxNotificationAsync(outboxId)
  │       │  SELECT payload FROM {OutboxTableName} WHERE id = @id
  │       │  JsonSerializer.Deserialize<BackplaneMessage>(payload)
  │       ▼
  │     Dispatch(message)
  │
  └── otherwise → JsonElement.Deserialize<BackplaneMessage>(...) → Dispatch(message)
  │
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

## Outbox pattern for large payloads

PostgreSQL caps each `NOTIFY` payload at 8000 bytes; messages exceeding that are rejected by the server. Rather than dropping such messages, the manager stages them in an **outbox table** and sends only a tiny reference through `NOTIFY`.

### Threshold

The split is governed by `PostgreSqlBackplaneOptions.InlinePayloadThresholdBytes` (default `7500` UTF-8 bytes — slightly under the hard limit to leave a safety margin). Messages whose serialized payload fits within the threshold travel inline through `pg_notify` with **no additional database round-trip**, preserving the hot-path latency of the existing implementation.

### Wire format

When a message is staged, the `NOTIFY` carries a small JSON envelope:

```json
{ "outboxId": "0c8a4f4e3b2d4a83a9d3a2b3c4d5e6f7" }
```

Receivers detect the marker and fetch the full `BackplaneMessage` by ID from the outbox table.

### Outbox table

Auto-created on startup via `CREATE TABLE IF NOT EXISTS`:

```sql
CREATE TABLE IF NOT EXISTS {OutboxTableName} (
    id         text        PRIMARY KEY,
    channel    text        NOT NULL,
    payload    text        NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_{OutboxTableName}_created_at
    ON {OutboxTableName} (created_at);
```

Both `OutboxTableName` (default `signalr_backplane_outbox`) and the channel name are validated against `^[a-z0-9_]+$` at construction time to prevent SQL identifier injection.

### Atomicity

The publisher uses a single CTE statement to keep `INSERT` and `NOTIFY` in one implicit transaction:

```sql
WITH inserted AS (
    INSERT INTO {OutboxTableName}(id, channel, payload) VALUES (@id, @channel, @payload) RETURNING 1
)
SELECT pg_notify(@channel, @marker) FROM inserted;
```

PostgreSQL only delivers `NOTIFY` events to listeners on transaction commit, so by the time a peer's `OnNotification` fires, the inserted row is committed and visible to the receiver's `SELECT`.

### Cleanup

After publishing, the manager schedules a fire-and-forget `DELETE` for the new row after `OutboxExpiry` (default 30 s). Receivers do **not** delete rows themselves — multiple instances may receive the same `NOTIFY`, and only one read attempt is needed per instance.

If the publishing process crashes between `INSERT` and the scheduled `DELETE`, the row will linger; operators can add a periodic prune (`DELETE FROM signalr_backplane_outbox WHERE created_at < now() - interval '1 hour'`) for defence-in-depth.

### Disabling the outbox

Setting `UseOutbox = false` reverts to the legacy "drop oversized" behaviour: messages exceeding `InlinePayloadThresholdBytes` are dropped with a `LogWarning` and never delivered.

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
