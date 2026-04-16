# Configuration Reference

This document provides a complete reference for all configuration options exposed by `Nodsoft.AspNetCore.SignalR.PostgreSQL`.

---

## `PostgreSqlBackplaneOptions`

Namespace: `Nodsoft.AspNetCore.SignalR.PostgreSQL`

Configure the backplane by passing an `Action<PostgreSqlBackplaneOptions>` delegate, an `NpgsqlDataSource`, or a connection string to one of the `AddPostgreSqlBackplane` extension methods.

### Properties

#### `ConnectionString`

```csharp
public string? ConnectionString { get; set; }
```

An [Npgsql connection string](https://www.npgsql.org/doc/connection-string-parameters.html).

Used to create an internal `NpgsqlDataSource` when `DataSource` is not provided. Only one of `ConnectionString` or `DataSource` must be set; providing both is allowed (but `DataSource` takes precedence).

**Example:**

```
Host=localhost;Port=5432;Database=myapp;Username=signalr_user;Password=s3cr3t
```

#### `DataSource`

```csharp
public NpgsqlDataSource? DataSource { get; set; }
```

A pre-configured [`NpgsqlDataSource`](https://www.npgsql.org/doc/api/Npgsql.NpgsqlDataSource.html). Takes precedence over `ConnectionString` when set.

Use this option when your application already manages an `NpgsqlDataSource` (e.g., via Aspire's `AddNpgsqlDataSource` or a shared data source builder), to avoid creating a separate connection pool.

---

## Registration Methods

All methods are extension methods on `ISignalRServerBuilder` in the `Nodsoft.AspNetCore.SignalR.PostgreSQL` namespace.

### `AddPostgreSqlBackplane(NpgsqlDataSource dataSource)`

```csharp
public static ISignalRServerBuilder AddPostgreSqlBackplane(
    this ISignalRServerBuilder builder,
    NpgsqlDataSource dataSource)
```

Registers the backplane using an already-constructed `NpgsqlDataSource`.

**Usage:**

```csharp
var dataSource = NpgsqlDataSource.Create("Host=localhost;Database=myapp;Username=u;Password=p");

builder.Services.AddSignalR()
    .AddPostgreSqlBackplane(dataSource);
```

**Throws:** `ArgumentNullException` if `builder` or `dataSource` is `null`.

---

### `AddPostgreSqlBackplane(string connectionString)`

```csharp
public static ISignalRServerBuilder AddPostgreSqlBackplane(
    this ISignalRServerBuilder builder,
    string connectionString)
```

Registers the backplane using a raw Npgsql connection string. An `NpgsqlDataSource` is created internally.

**Usage:**

```csharp
builder.Services.AddSignalR()
    .AddPostgreSqlBackplane("Host=localhost;Database=myapp;Username=u;Password=p");
```

**Throws:** `ArgumentNullException` if `builder` is `null`. `ArgumentException` if `connectionString` is `null` or whitespace.

---

### `AddPostgreSqlBackplane(Action<PostgreSqlBackplaneOptions> configureOptions)`

```csharp
public static ISignalRServerBuilder AddPostgreSqlBackplane(
    this ISignalRServerBuilder builder,
    Action<PostgreSqlBackplaneOptions> configureOptions)
```

Registers the backplane using a configuration delegate. Useful when consuming connection strings from `IConfiguration` or when combining with other options setup patterns.

**Usage:**

```csharp
builder.Services.AddSignalR()
    .AddPostgreSqlBackplane(options =>
    {
        options.ConnectionString = builder.Configuration.GetConnectionString("SignalR")
            ?? throw new InvalidOperationException("Missing 'SignalR' connection string.");
    });
```

**Throws:** `ArgumentNullException` if `builder` or `configureOptions` is `null`.

---

## Connection String Parameters

The following Npgsql connection string parameters are relevant to the backplane's behaviour. For a complete reference, see the [Npgsql documentation](https://www.npgsql.org/doc/connection-string-parameters.html).

| Parameter | Recommended value | Notes |
|---|---|---|
| `Host` | Your PostgreSQL host | Required. |
| `Port` | `5432` (default) | Optional. |
| `Database` | Your application's database | Required. |
| `Username` / `Password` | Service account credentials | Required unless using peer auth. |
| `Minimum Pool Size` | `1` | Ensures at least one connection for the LISTEN loop. |
| `Maximum Pool Size` | `10`+ | Tune based on expected `pg_notify` throughput. |
| `Keepalive` | `30` | Seconds between TCP keep-alive probes. Helps detect stale LISTEN connections. |
| `TCP Keepalives Idle` | `60` | OS-level TCP keepalive idle time. |
| `Application Name` | `signalr-backplane` | Useful for identifying connections in `pg_stat_activity`. |

**Example with recommended parameters:**

```
Host=db.example.com;Database=myapp;Username=signalr;Password=s3cr3t;
Minimum Pool Size=1;Maximum Pool Size=10;Keepalive=30;Application Name=signalr-backplane
```

---

## PostgreSQL User Permissions

The database user used by the backplane requires only the following privileges:

```sql
-- No table-level permissions are needed.
-- pg_notify is available to all users by default.
GRANT CONNECT ON DATABASE myapp TO signalr_user;
```

The backplane does **not** create or modify any tables. All operations use `SELECT pg_notify(...)` and `LISTEN`.

---

## Multi-Hub Configuration

Each hub type registers its own `PostgreSqlHubLifetimeManager<THub>` instance. All hubs share the same `PostgreSqlBackplaneOptions`. If you need different connection strings per hub, register options using named options:

```csharp
services.AddOptions<PostgreSqlBackplaneOptions>("ChatHub")
    .Configure(o => o.ConnectionString = "...");

services.AddOptions<PostgreSqlBackplaneOptions>("NotificationHub")
    .Configure(o => o.ConnectionString = "...");
```

> **Note:** Named options support requires customising the lifetime manager constructor to inject `IOptionsMonitor<PostgreSqlBackplaneOptions>` instead of `IOptions<PostgreSqlBackplaneOptions>`. This is not supported out of the box in the current release.

---

## Integration with .NET Aspire

When using .NET Aspire, register the data source using the Aspire PostgreSQL component and pass it to the backplane:

```csharp
// AppHost
var postgres = builder.AddPostgres("postgres")
    .AddDatabase("signalr");

var server = builder.AddProject<Projects.MyServer>("server")
    .WithReference(postgres);
```

```csharp
// Server Program.cs
builder.AddNpgsqlDataSource("signalr");

builder.Services.AddSignalR()
    .AddPostgreSqlBackplane(options =>
    {
        // Resolved by DI at runtime via PostConfigure
    });

builder.Services.AddOptions<PostgreSqlBackplaneOptions>()
    .PostConfigure<NpgsqlDataSource>((o, ds) => o.DataSource = ds);
```

---

## Startup Validation

The `PostgreSqlHubLifetimeManager<THub>` constructor validates configuration eagerly at startup:

| Condition | Exception |
|---|---|
| Neither `ConnectionString` nor `DataSource` is set | `InvalidOperationException` |
| Hub type name contains characters outside `[a-z0-9_]` | `InvalidOperationException` |

Failed validation prevents the application from starting, providing fast feedback on misconfiguration.
