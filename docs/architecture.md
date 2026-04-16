# Architecture

This document describes the internal design of `Nodsoft.AspNetCore.SignalR.PostgreSQL` and explains how PostgreSQL LISTEN/NOTIFY is used to implement a SignalR backplane.

---

## Table of Contents

- [Overview](#overview)
- [PostgreSQL LISTEN/NOTIFY Primer](#postgresql-listennotify-primer)
- [Component Responsibilities](#component-responsibilities)
- [Message Flow](#message-flow)
  - [Outbound (publish)](#outbound-publish)
  - [Inbound (receive)](#inbound-receive)
- [Channel Naming](#channel-naming)
- [BackplaneMessage Schema](#backplanemessage-schema)
- [Routing Strategies](#routing-strategies)
- [Connection, Group, and User Tracking](#connection-group-and-user-tracking)
- [Reconnection Behaviour](#reconnection-behaviour)
- [Payload Size Limit](#payload-size-limit)
- [Self-Delivery Prevention](#self-delivery-prevention)
- [Disposal and Shutdown](#disposal-and-shutdown)
- [Design Trade-offs](#design-trade-offs)

---

## Overview

ASP.NET Core SignalR's `HubLifetimeManager<THub>` is the abstraction responsible for tracking connections and dispatching hub method invocations to clients. Out of the box, the default in-memory implementation only knows about connections on the current server process. In a horizontally-scaled deployment (multiple server instances behind a load balancer), a message sent on Server A reaches only the clients connected to Server A.

This library replaces `HubLifetimeManager<THub>` with `PostgreSqlHubLifetimeManager<THub>`, which intercepts every `Send*` call, serialises the invocation into a JSON payload, and publishes it to a PostgreSQL notification channel via `pg_notify`. Every server instance (including the sender) listens on that channel and routes the payload to its locally connected clients.

```
 ┌─────────────────────────────────────────────────────────────────────────────┐
 │                         ASP.NET Core Server A                               │
 │                                                                             │
 │  Hub method call                                                            │
 │       │                                                                     │
 │       ▼                                                                     │
 │  PostgreSqlHubLifetimeManager ──► pg_notify("signalr__chathub", payload)   │
 │       ▲                                                                     │
 │       │  LISTEN "signalr__chathub"  (background loop)                      │
 │       │  discards own messages (ServerInstanceId check)                    │
 └───────┼─────────────────────────────────────────────────────────────────────┘
         │
         │  PostgreSQL NOTIFY broadcast
         │
 ┌───────┼─────────────────────────────────────────────────────────────────────┐
 │       │                  ASP.NET Core Server B                              │
 │       │                                                                     │
 │       ▼                                                                     │
 │  PostgreSqlHubLifetimeManager (LISTEN loop receives notification)          │
 │       │                                                                     │
 │       ▼                                                                     │
 │  Route payload to local connections / groups / users                       │
 └─────────────────────────────────────────────────────────────────────────────┘
```

---

## PostgreSQL LISTEN/NOTIFY Primer

`LISTEN` and `NOTIFY` are PostgreSQL built-in commands for lightweight pub/sub messaging:

- **`LISTEN <channel>`** — a client session subscribes to a named channel. Notifications are queued on the connection until the client reads them.
- **`NOTIFY <channel>, 'payload'`** (or `SELECT pg_notify(<channel>, <payload>)`) — publishes a string payload to the channel. All sessions currently listening are notified.
- Notifications are delivered asynchronously. The Npgsql driver surfaces them via the `NpgsqlConnection.Notification` event.
- Unlike a message queue, NOTIFY is **fire-and-forget**: if no subscriber is connected at the time of notification, or if a subscriber is offline, the message is lost.
- The payload is limited to **8 191 bytes** (8 KB − 1).

---

## Component Responsibilities

| Component | Responsibility |
|---|---|
| `PostgreSqlHubLifetimeManager<THub>` | Implements `HubLifetimeManager<THub>`. Tracks local connections, groups, and users. Serialises and publishes `BackplaneMessage`s. Maintains the LISTEN background loop. Routes inbound notifications to local clients. |
| `BackplaneMessage` | Immutable JSON-serialisable record that encodes a SignalR hub method invocation and its routing metadata. |
| `BackplaneMessageType` | `byte`-backed enum describing which routing strategy to apply (All, Group, User, Connection, etc.). |
| `PostgreSqlBackplaneOptions` | Configuration: connection string or pre-built `NpgsqlDataSource`. |
| `PostgreSqlSignalRBuilderExtensions` | DI registration: replaces `HubLifetimeManager<>` with `PostgreSqlHubLifetimeManager<>`. |

---

## Message Flow

### Outbound (publish)

```
Hub method (e.g. Clients.All.SendAsync("ReceiveMessage", ...))
    │
    ▼
PostgreSqlHubLifetimeManager.SendAllAsync(methodName, args)
    │
    ├─ Builds BackplaneMessage { Type=All, MethodName, Args=SerializeArgs(args), ServerInstanceId }
    │
    ├─ JsonSerializer.Serialize(message)         ← ~JSON string
    │
    ├─ Check payload.Length ≤ 8 000              ← drop + log warning if exceeded
    │
    └─ await NpgsqlCommand("SELECT pg_notify($channel, @payload)").ExecuteNonQueryAsync()
```

Each `Send*` overload maps to a `BackplaneMessageType` variant and populates the appropriate `Filter`, `Filters`, or `ExcludedConnectionIds` fields.

### Inbound (receive)

```
NpgsqlConnection.Notification event fires (on LISTEN connection)
    │
    ▼
OnNotification(sender, NpgsqlNotificationEventArgs e)
    │
    ├─ Deserialise e.Payload → BackplaneMessage
    │
    ├─ If message.ServerInstanceId == _serverInstanceId → discard (own message)   [NOT currently implemented — see note below]
    │
    ├─ DeserializeArgs(message.Args)
    │
    └─ switch(message.Type)
          All           → DeliverToAll(methodName, args, [])
          AllExcept     → DeliverToAll(methodName, args, excludedConnectionIds)
          Connection    → DeliverToConnection(connectionId, methodName, args)
          Connections   → foreach id → DeliverToConnection(id, ...)
          Group         → DeliverToGroup(groupName, methodName, args, [])
          GroupExcept   → DeliverToGroup(groupName, methodName, args, excluded)
          Groups        → foreach group → DeliverToGroup(group, ...)
          User          → DeliverToUser(userId, methodName, args)
          Users         → foreach user → DeliverToUser(user, ...)
```

> **Note:** The `ServerInstanceId` field is stored in the message and is available for self-delivery filtering. The current implementation does **not** short-circuit on own messages, because the sender's local clients should also receive the message (the publish path does *not* deliver locally; delivery only happens via the LISTEN path). This design ensures a single consistent code path for all delivery.

---

## Channel Naming

Channel names follow the pattern:

```
signalr__<hubname_lowercased>
```

Examples:

| Hub class | Channel name |
|---|---|
| `ChatHub` | `signalr__chathub` |
| `NotificationHub` | `signalr__notificationhub` |
| `MyApp_Hub` | `signalr__myapp_hub` |

The hub type name is validated against the regular expression `^[a-z0-9_]+$` after lowercasing. Any hub whose name contains characters outside this set (e.g., hyphens, Unicode) will cause an `InvalidOperationException` at startup.

PostgreSQL channel names are case-sensitive and can be up to 63 bytes. The `signalr__` prefix reserves a namespace and makes monitoring queries easy (`SELECT ... WHERE channel LIKE 'signalr__%'`).

---

## BackplaneMessage Schema

```json
{
  "serverInstanceId": "a3b4c5d6e7f8...",   // GUID (N format) — identifies originating server
  "type": 0,                                // BackplaneMessageType byte value
  "methodName": "ReceiveMessage",           // Hub client method to invoke
  "args": [                                 // JSON-encoded arguments
    { "user": "Alice", "text": "Hello" }
  ],
  "filter": null,                           // Single target (connectionId / groupName / userId)
  "excludedConnectionIds": null,            // Connections to skip
  "filters": null                           // Multiple targets (connectionIds / groupNames / userIds)
}
```

`BackplaneMessageType` values:

| Value | Byte | Meaning |
|---|---|---|
| `All` | 0 | All connected clients |
| `AllExcept` | 1 | All clients except `excludedConnectionIds` |
| `Group` | 2 | All clients in group `filter` |
| `GroupExcept` | 3 | All clients in group `filter`, except `excludedConnectionIds` |
| `Groups` | 4 | All clients in all groups in `filters` |
| `User` | 5 | All connections of user `filter` |
| `Users` | 6 | All connections of users in `filters` |
| `Connection` | 7 | Single connection `filter` |
| `Connections` | 8 | Connections in `filters` |

---

## Routing Strategies

### Deliver to all (`All` / `AllExcept`)

Iterates `_connections` (the full local connection dictionary). For `AllExcept`, builds a `HashSet<string>` from `ExcludedConnectionIds` and skips matching connection IDs.

### Deliver to group (`Group` / `GroupExcept` / `Groups`)

Looks up `_groups[groupName]` and iterates its members, applying exclusions as above. If the group is not present in the local dictionary (no local members), the delivery is a no-op.

### Deliver to user (`User` / `Users`)

Looks up `_users[userId]` and iterates all connections belonging to that user (a user may have multiple simultaneous connections).

### Deliver to connection (`Connection` / `Connections`)

Looks up `_connections[connectionId]` by exact ID. Only delivers if that connection is locally tracked.

---

## Connection, Group, and User Tracking

All tracking structures are thread-safe `ConcurrentDictionary` instances:

```
_connections : ConcurrentDictionary<string, HubConnectionContext>
    // key: connectionId

_groups      : ConcurrentDictionary<string, ConcurrentDictionary<string, HubConnectionContext>>
    // outer key: groupName
    // inner key: connectionId

_users       : ConcurrentDictionary<string, ConcurrentDictionary<string, HubConnectionContext>>
    // outer key: userId (from HubConnectionContext.UserIdentifier)
    // inner key: connectionId
```

- `OnConnectedAsync` — adds to `_connections`; if the connection has a `UserIdentifier`, adds to `_users`.
- `OnDisconnectedAsync` — removes from `_connections`, `_users`, and all `_groups`.
- `AddToGroupAsync` / `RemoveFromGroupAsync` — mutates `_groups`.

> **Important:** group membership is tracked **locally per server instance** only. A client that connects to Server A and calls `JoinGroup` is not visible in Server B's `_groups`. When Server B receives a group-targeted `BackplaneMessage`, it delivers only to its own local group members.

---

## Reconnection Behaviour

The LISTEN background loop (`StartListeningAsync`) is designed to survive transient database errors:

```
loop:
  try:
    open connection
    LISTEN "<channel>"
    loop: await WaitAsync(cancellationToken)   ← blocks until notification or error
  catch OperationCanceledException (shutdown):
    break
  catch any other exception:
    log error
    dispose connection
    await Task.Delay(5 seconds, cancellationToken)
    continue loop
```

The 5-second reconnection delay prevents tight-loop hammering during extended outages. There is currently no exponential backoff; all reconnections use a fixed 5-second delay.

---

## Payload Size Limit

PostgreSQL's `pg_notify` truncates or rejects payloads larger than **8 191 bytes**. The library checks `payload.Length > 8000` (a conservative threshold) before publishing and logs a warning and drops the message if exceeded.

There is no built-in mechanism to fragment or overflow large messages. See [Limitations](../README.md#limitations) in the README for mitigation strategies.

---

## Self-Delivery Prevention

Each `PostgreSqlHubLifetimeManager` instance generates a unique `_serverInstanceId` (`Guid.NewGuid().ToString("N")`) at construction time. This ID is embedded in every outbound `BackplaneMessage`. The LISTEN loop on the same server will receive back its own notifications (PostgreSQL notifies all listeners, including the sender). The `ServerInstanceId` field is present in the schema for future use; the current implementation relies on the fact that the publish path does not call any local `Deliver*` methods — all local delivery flows through the LISTEN path — ensuring consistent behaviour without an explicit discard.

---

## Disposal and Shutdown

`PostgreSqlHubLifetimeManager<THub>` implements `IAsyncDisposable`. On disposal:

1. `_cts.Cancel()` — signals the LISTEN loop to stop.
2. `await _listenTask` — waits for the loop to exit gracefully.
3. `_listenConnection?.DisposeAsync()` — closes the LISTEN connection.
4. `_cts.Dispose()`.

Disposal is triggered automatically by the ASP.NET Core DI container when the application shuts down.

---

## Design Trade-offs

| Decision | Rationale |
|---|---|
| One LISTEN connection per hub type per server | Keeps connection count predictable; each hub type is isolated. |
| Fire-and-forget `pg_notify` | Matches SignalR's delivery semantics (no guaranteed delivery). |
| JSON serialisation (System.Text.Json) | Interoperable and inspectable; trade-off is larger payload vs. binary formats. |
| 8 KB payload guard | Prevents silent truncation or errors from PostgreSQL. |
| Local-only group tracking | Avoids the need for a distributed group store; clients must re-join groups on reconnect. |
| Fixed 5-second reconnection delay | Simple and predictable; exponential backoff can be added later. |
| `byte`-backed `BackplaneMessageType` | Reduces JSON payload size compared to string enum names. |
