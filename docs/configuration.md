# Configuration Reference

This page documents all configuration options, extension method overloads, and DI registration details for `Nodsoft.AspNetCore.SignalR.PostgreSQL`.

&nbsp;

## `PostgreSqlBackplaneOptions`

Namespace: `Nodsoft.AspNetCore.SignalR.PostgreSQL`

```csharp
public sealed class PostgreSqlBackplaneOptions
{
    public string? ConnectionString { get; set; }
    public NpgsqlDataSource? DataSource { get; set; }

    public int InlinePayloadThresholdBytes { get; set; } = 7500;
    public bool UseOutbox { get; set; } = true;
    public string OutboxTableName { get; set; } = "signalr_backplane_outbox";
    public TimeSpan OutboxExpiry { get; set; } = TimeSpan.FromSeconds(30);
}
```

Exactly one of `ConnectionString` / `DataSource` must be set. If neither is provided, `PostgreSqlHubLifetimeManager<THub>` throws an `InvalidOperationException` when it is first instantiated (at DI resolution time, not at registration time).

### `ConnectionString`

A standard [Npgsql connection string](https://www.npgsql.org/doc/connection-string-parameters.html).

```csharp
options.ConnectionString = "Host=localhost;Port=5432;Database=myapp;Username=app;Password=secret";
```

When `ConnectionString` is used, the manager creates a default `NpgsqlDataSource` internally via `NpgsqlDataSource.Create(connectionString)`. This data source uses Npgsql's built-in connection pool and is owned by the manager (not exposed to the rest of the application).

### `DataSource`

An externally created `NpgsqlDataSource`. **Takes precedence** over `ConnectionString` when both are set.

```csharp
options.DataSource = NpgsqlDataSource.Create(connectionString);
// or
options.DataSource = new NpgsqlDataSourceBuilder(connectionString)
    .EnableDynamicJson()
    .Build();
```

Prefer this option when you need full control over the data source (e.g. custom type mappings, specific pool settings) or when you want to share the same data source between the backplane and the rest of your application.

> **Note:** The backplane does **not** call `Dispose` on the `NpgsqlDataSource` it receives. Lifetime management is the caller's responsibility.

&nbsp;

## Outbox options

The library uses an **outbox pattern** to relay messages whose serialized JSON exceeds the PostgreSQL `NOTIFY` payload limit (8 KB). Small messages still travel inline through `pg_notify` with no extra database round-trip; large messages are stored in a small table and a short reference is sent through `NOTIFY` instead. See [docs/architecture.md](architecture.md#outbox-pattern-for-large-payloads) for the full description.

### `InlinePayloadThresholdBytes`

The maximum UTF-8 byte size of a serialized backplane message that will be sent inline. Defaults to `7500` (the PostgreSQL hard limit is 8000 bytes; the default leaves a safety margin).

```csharp
options.InlinePayloadThresholdBytes = 7500;
```

Setting this value to `0` forces every message through the outbox — useful for testing the outbox path or when you want to keep `NOTIFY` payloads as small as possible.

### `UseOutbox`

When `true` (the default), oversized messages are transparently routed through the outbox table. When `false`, oversized messages are dropped with a warning log entry — useful for legacy behaviour or environments where you do not want the backplane to issue `INSERT`/`DELETE` statements.

```csharp
options.UseOutbox = false; // legacy "drop oversized" behaviour
```

### `OutboxTableName`

Name of the outbox table. Must contain only lowercase letters, digits, and underscores (regex `^[a-z0-9_]+$`) to prevent SQL identifier injection. Defaults to `signalr_backplane_outbox`. The table is auto-created on first use via `CREATE TABLE IF NOT EXISTS`, so you do not have to provision it manually. Schema:

```sql
CREATE TABLE IF NOT EXISTS signalr_backplane_outbox (
    id         text        PRIMARY KEY,
    channel    text        NOT NULL,
    payload    text        NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_signalr_backplane_outbox_created_at
    ON signalr_backplane_outbox (created_at);
```

If you need to share the outbox across multiple deployments or isolate it per environment, override the name:

```csharp
options.OutboxTableName = "myapp_signalr_outbox";
```

### `OutboxExpiry`

How long an outbox row lives before it is cleaned up. Defaults to `30 seconds`. After publishing, the manager schedules a fire-and-forget `DELETE` after this delay, giving all listeners a generous window to fetch the payload.

```csharp
options.OutboxExpiry = TimeSpan.FromMinutes(1);
```

> **Operational note:** If a publisher crashes between `INSERT` and the scheduled `DELETE`, an outbox row may linger. Operators are encouraged to add a periodic cleanup job (e.g. `DELETE FROM signalr_backplane_outbox WHERE created_at < now() - interval '1 hour'`) for defence-in-depth.

&nbsp;

## `AddPostgreSqlBackplane` extension methods

Namespace: `Nodsoft.AspNetCore.SignalR.PostgreSQL`  
Extends: `Microsoft.AspNetCore.SignalR.ISignalRServerBuilder`

All overloads register `PostgreSqlHubLifetimeManager<>` as the singleton `HubLifetimeManager<>` for every hub type used in the application.

---

### `AddPostgreSqlBackplane(NpgsqlDataSource)`

```csharp
public static ISignalRServerBuilder AddPostgreSqlBackplane(
    this ISignalRServerBuilder builder,
    NpgsqlDataSource dataSource)
```

Directly supplies a pre-built data source.

```csharp
NpgsqlDataSource ds = NpgsqlDataSource.Create(connectionString);

builder.Services.AddSignalR()
    .AddPostgreSqlBackplane(ds);
```

**Validation:** Both `builder` and `dataSource` must be non-null (`ArgumentNullException` thrown otherwise).

---

### `AddPostgreSqlBackplane(string)`

```csharp
public static ISignalRServerBuilder AddPostgreSqlBackplane(
    this ISignalRServerBuilder builder,
    string connectionString)
```

Supplies a raw connection string. A default `NpgsqlDataSource` is created internally.

```csharp
builder.Services.AddSignalR()
    .AddPostgreSqlBackplane(
        builder.Configuration.GetConnectionString("signalr")!);
```

**Validation:** `connectionString` must be non-null and non-whitespace (`ArgumentException` thrown otherwise).

---

### `AddPostgreSqlBackplane(Action<PostgreSqlBackplaneOptions>)`

```csharp
public static ISignalRServerBuilder AddPostgreSqlBackplane(
    this ISignalRServerBuilder builder,
    Action<PostgreSqlBackplaneOptions> configureOptions)
```

Accepts a configuration delegate, which is registered via `IOptions<PostgreSqlBackplaneOptions>`. Use this overload when you want to configure options from `IConfiguration` or other DI-resolved values.

```csharp
builder.Services.AddSignalR()
    .AddPostgreSqlBackplane(options =>
    {
        options.ConnectionString = builder.Configuration.GetConnectionString("signalr");
    });
```

**Validation:** Both `builder` and `configureOptions` must be non-null.

&nbsp;

## Integration with Npgsql DI extensions

### Aspire / `Npgsql.DependencyInjection`

When using [.NET Aspire](https://learn.microsoft.com/en-us/dotnet/aspire/) or the `Npgsql.DependencyInjection` NuGet package you typically register `NpgsqlDataSource` in DI:

```csharp
// Aspire
builder.AddNpgsqlDataSource("signalr");

// Npgsql.DependencyInjection
builder.Services.AddNpgsqlDataSource(connectionString);
```

You can then use the options delegate overload and resolve the data source yourself:

```csharp
builder.Services.AddSignalR()
    .AddPostgreSqlBackplane(options =>
    {
        // The options delegate runs before the DI container is built,
        // so retrieve the data source via PostConfigure instead.
    });

builder.Services.AddOptions<PostgreSqlBackplaneOptions>()
    .PostConfigure<NpgsqlDataSource>((opts, ds) => opts.DataSource = ds);
```

Or, if you have direct access to the `NpgsqlDataSource` instance at registration time, use the `AddPostgreSqlBackplane(NpgsqlDataSource)` overload directly.

&nbsp;

## Configuration via `appsettings.json`

The options system is built on `IOptions<PostgreSqlBackplaneOptions>`, so the connection string can be bound from any configuration source.

_appsettings.json_

```json
{
  "ConnectionStrings": {
    "signalr": "Host=localhost;Database=myapp;Username=app;Password=secret"
  }
}
```

_Program.cs_

```csharp
builder.Services.AddSignalR()
    .AddPostgreSqlBackplane(options =>
    {
        options.ConnectionString = builder.Configuration.GetConnectionString("signalr");
    });
```

&nbsp;

## Environment variables

Because `GetConnectionString` reads from `ConnectionStrings:{name}`, you can override it with the environment variable:

```
ConnectionStrings__signalr=Host=db.example.com;Database=myapp;Username=app;Password=secret
```

&nbsp;

## Multiple hubs

Each hub type gets its own PostgreSQL channel and its own `PostgreSqlHubLifetimeManager<THub>` singleton. You do not need to call `AddPostgreSqlBackplane` more than once:

```csharp
builder.Services.AddSignalR()
    .AddPostgreSqlBackplane(connectionString)  // applies to ALL hubs
    .AddHub<ChatHub>()
    .AddHub<NotificationHub>();
```

Both `ChatHub` and `NotificationHub` will use the PostgreSQL backplane, listening on `signalr__chathub` and `signalr__notificationhub` respectively.

&nbsp;

## Logging

`PostgreSqlHubLifetimeManager<THub>` logs to the standard `ILogger<PostgreSqlHubLifetimeManager<THub>>` category. The following events are emitted:

| Level | Event |
|---|---|
| `Information` | A client connected or disconnected. |
| `Information` | LISTEN loop started on a channel. |
| `Debug` | A message was published through the outbox table (includes outbox row ID). |
| `Debug` | The outbox table was successfully provisioned. |
| `Debug` | A scheduled outbox cleanup `DELETE` failed (best-effort, harmless). |
| `Warning` | A backplane message exceeded the inline NOTIFY threshold and the outbox is disabled (the message is dropped). |
| `Warning` | An outbox row referenced by a notification was missing or already cleaned up before delivery. |
| `Warning` | Writing a hub method invocation to a specific connection failed. |
| `Warning` | Failed to parse or deserialize a backplane notification payload. |
| `Error` | The LISTEN loop encountered an error (includes reconnect notice). |
| `Error` | The outbox table could not be provisioned. |

To suppress verbose connection logs in production, increase the minimum level for this category in `appsettings.json`:

```json
{
  "Logging": {
    "LogLevel": {
      "Nodsoft.AspNetCore.SignalR.PostgreSQL.PostgreSqlHubLifetimeManager": "Warning"
    }
  }
}
```
