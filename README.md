# Nodsoft.AspNetCore.SignalR.PostgreSQL

### PostgreSQL LISTEN/NOTIFY Backplane for ASP.NET Core SignalR

[![NuGet](https://img.shields.io/nuget/v/Nodsoft.AspNetCore.SignalR.PostgreSQL?style=flat-square&logo=nuget)](https://www.nuget.org/packages/Nodsoft.AspNetCore.SignalR.PostgreSQL)
[![NuGet Downloads](https://img.shields.io/nuget/dt/Nodsoft.AspNetCore.SignalR.PostgreSQL?style=flat-square&logo=nuget)](https://www.nuget.org/packages/Nodsoft.AspNetCore.SignalR.PostgreSQL)
[![License](https://img.shields.io/github/license/Nodsoft/SignalR.PostgreSQL?style=flat-square)](LICENSE)
[![.NET](https://img.shields.io/badge/.NET-10%2B-blueviolet?style=flat-square&logo=dotnet)](https://dotnet.microsoft.com/)

&nbsp;

## Premise

`Nodsoft.AspNetCore.SignalR.PostgreSQL` provides a drop-in SignalR backplane backed by **PostgreSQL LISTEN/NOTIFY** — no Redis, no additional infrastructure required.

When you scale your ASP.NET Core application horizontally across multiple server instances, each instance holds its own in-memory map of active SignalR connections. Without a backplane, a message sent on server A cannot reach clients connected to server B. This library solves that by using the PostgreSQL database you already have as a lightweight pub/sub bus.

Each call to a `Send*` method (e.g. `SendAllAsync`, `SendGroupAsync`) serialises the invocation as a JSON payload and issues a `pg_notify` call on a per-hub PostgreSQL channel. All other server instances receive the notification via a persistent `LISTEN` connection and dispatch it to their locally tracked connections.

**Why PostgreSQL instead of Redis?**

- You already run PostgreSQL in most stacks — no extra service to operate.
- LISTEN/NOTIFY is a first-class PostgreSQL feature, stable and well-understood.
- Suitable for low-to-medium throughput real-time workloads where a dedicated message broker is overkill.

> [!NOTE]  
> For very high throughput or guaranteed message delivery, consider a dedicated message broker (e.g. Redis, RabbitMQ). See [Limitations](#limitations).

&nbsp;

## Quick Start

### 1. Install the package

```bash
dotnet add package Nodsoft.AspNetCore.SignalR.PostgreSQL
```

### 2. Register the backplane

Call `AddPostgreSqlBackplane` on your `ISignalRServerBuilder` in `Program.cs` (or `Startup.cs`):

**Option A — connection string**

```csharp
builder.Services.AddSignalR()
    .AddPostgreSqlBackplane("Host=localhost;Database=myapp;Username=postgres;Password=secret");
```

**Option B — `NpgsqlDataSource` (recommended)**

Provide a pre-configured `NpgsqlDataSource`, for example one registered via the Npgsql Aspire integration or another DI extension. Because the `AddPostgreSqlBackplane` overloads do not accept a DI factory delegate, use `PostConfigure` to wire the DI-registered data source into the backplane options after registration:

```csharp
// Register the data source (e.g. via Npgsql.DependencyInjection or Aspire)
builder.AddNpgsqlDataSource("signalr");

// Register the backplane (connection string acts as a placeholder; PostConfigure overrides it)
builder.Services.AddSignalR()
    .AddPostgreSqlBackplane(options => { /* connection string or data source set below */ });

// Override options with the DI-registered NpgsqlDataSource
builder.Services.AddOptions<PostgreSqlBackplaneOptions>()
    .PostConfigure<NpgsqlDataSource>((opts, ds) => opts.DataSource = ds);
```

Or, if you have the `NpgsqlDataSource` instance at hand, pass it directly:

```csharp
NpgsqlDataSource dataSource = NpgsqlDataSource.Create(connectionString);

builder.Services.AddSignalR()
    .AddPostgreSqlBackplane(options => options.DataSource = dataSource);
```

**Option C — options delegate**

```csharp
builder.Services.AddSignalR()
    .AddPostgreSqlBackplane(options =>
    {
        options.ConnectionString = builder.Configuration.GetConnectionString("signalr");
    });
```

### 3. Map your hubs as normal

No changes required to your `Hub` classes or how you map them:

```csharp
app.MapHub<ChatHub>("/hubs/chat");
```

That's it. Every server instance that starts with this configuration will participate in the shared PostgreSQL channel for each hub type.

&nbsp;

## Configuration Reference

All backplane configuration lives in `PostgreSqlBackplaneOptions`:

| Property | Type | Description |
|---|---|---|
| `ConnectionString` | `string?` | A standard Npgsql connection string. Used to create an `NpgsqlDataSource` internally. |
| `DataSource` | `NpgsqlDataSource?` | A pre-configured Npgsql data source. **Takes precedence** over `ConnectionString` when both are set. |

Exactly one of `ConnectionString` or `DataSource` must be provided. Omitting both throws an `InvalidOperationException` at startup.

### Extension method overloads

```csharp
// Pass a pre-built NpgsqlDataSource directly
ISignalRServerBuilder AddPostgreSqlBackplane(this ISignalRServerBuilder builder, NpgsqlDataSource dataSource);

// Pass a connection string
ISignalRServerBuilder AddPostgreSqlBackplane(this ISignalRServerBuilder builder, string connectionString);

// Provide a configuration delegate
ISignalRServerBuilder AddPostgreSqlBackplane(this ISignalRServerBuilder builder, Action<PostgreSqlBackplaneOptions> configureOptions);
```

&nbsp;

## How It Works

```
┌─────────────────────────┐           ┌─────────────────────────┐
│    Server Instance A    │           │    Server Instance B    │
│                         │           │                         │
│  Hub.SendAllAsync(...)  │ ─NOTIFY─> │  pg_notify recv         │
│                         │           │  → dispatch to clients  │
│  LISTEN signalr__hub    │ <─NOTIFY─ │  Hub.SendAllAsync(...)  │
│  → dispatch to clients  │           │                         │
└────────────┬────────────┘           └─────────────┬───────────┘
             │                                      │
             └────────────────┬─────────────────────┘
                              │
                      ┌───────▼───────┐
                      │  PostgreSQL   │
                      │ LISTEN/NOTIFY │
                      └───────────────┘
```

1. **On startup**, `PostgreSqlHubLifetimeManager<THub>` opens a dedicated long-lived PostgreSQL connection and issues `LISTEN "signalr__{hubname}"`.
2. **On `Send*`**, the manager serialises the method name, arguments, and routing target into a JSON payload and executes `SELECT pg_notify('signalr__{hubname}', @payload)`.
3. **All listening instances** (including the publisher) receive the notification. The publisher skips its own messages by comparing `ServerInstanceId`.
4. **Each receiver** dispatches the invocation to locally tracked connections that match the routing target.

For a deeper technical description see [docs/architecture.md](docs/architecture.md).

&nbsp;

## Routing Support

The backplane supports every routing target exposed by `HubLifetimeManager<THub>`:

| SignalR method | Routing |
|---|---|
| `SendAllAsync` | All connected clients |
| `SendAllExceptAsync` | All clients except specified connection IDs |
| `SendConnectionAsync` | A single connection by ID |
| `SendConnectionsAsync` | Multiple connections by ID |
| `SendGroupAsync` | All clients in a named group |
| `SendGroupExceptAsync` | Group clients, excluding specified connection IDs |
| `SendGroupsAsync` | All clients across multiple named groups |
| `SendUserAsync` | All connections belonging to a user |
| `SendUsersAsync` | All connections belonging to multiple users |

&nbsp;

## Limitations

| Constraint | Detail |
|---|---|
| **Payload size** | PostgreSQL NOTIFY payloads are capped at **8 KB**. Messages that exceed this limit are dropped with a warning log (`LogWarning`). Keep hub method arguments small, or consider offloading large data to a shared store (database, blob storage) and sending only a reference. |
| **Group membership is local** | Each server instance only tracks the groups that its own locally connected clients have joined. A `SendGroupAsync` from server A reaches only the clients in that group that are connected to server A. This matches the default in-process SignalR behaviour and is sufficient when the load balancer uses sticky sessions. Sticky sessions are **strongly recommended**. |
| **No message persistence** | NOTIFY is fire-and-forget. Messages published before a LISTEN connection is established — or during a reconnect gap — are silently lost. |
| **Throughput** | LISTEN/NOTIFY is suitable for low-to-medium fan-out scenarios. For high-volume, high-frequency message streams consider a dedicated broker. |

&nbsp;

## Requirements

- **.NET 10** or later
- **PostgreSQL 10** or later (LISTEN/NOTIFY has been available since PostgreSQL 9, but Npgsql 10.x requires PG 10+)
- **Npgsql 10.x** (pulled in transitively; no explicit installation required)

&nbsp;

## Spike / Demo Application

The [`spike/`](spike/) directory in this repository contains a full .NET Aspire-hosted sample application demonstrating the backplane in action with a real-time chat interface:

| Project | Role |
|---|---|
| `Spike.AppHost` | .NET Aspire orchestrator — starts PostgreSQL, the server, and the Blazor client |
| `Spike.Server` | ASP.NET Core WebAPI + SignalR server with the PostgreSQL backplane |
| `Spike.Client` | Blazor WebAssembly chat client |
| `Spike.Common` | Shared contracts (`IChatClient`, `ChatMessage`) |
| `Spike.ServiceDefaults` | Standard Aspire service defaults |

To run the spike locally:

```bash
cd spike/Spike.AppHost
dotnet run
```

The Aspire dashboard will open and show all services. The Blazor client connects to the SignalR server; you can open multiple browser tabs to simulate clients on different logical "server instances" once the server is scaled.

&nbsp;

## Contributing

Contributions, bug reports, and feature requests are welcome. Please open an issue or pull request on [GitHub](https://github.com/Nodsoft/SignalR.PostgreSQL).

&nbsp;

## License

This project is licensed under the [MIT License](LICENSE).  
© 2026 [Nodsoft Systems](https://github.com/Nodsoft) — authored by [Sakura Akeno Isayeki](https://github.com/SakuraIsayeki).
