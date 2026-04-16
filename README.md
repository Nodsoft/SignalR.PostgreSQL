# Nodsoft.AspNetCore.SignalR.PostgreSQL

[![NuGet](https://img.shields.io/nuget/v/Nodsoft.AspNetCore.SignalR.PostgreSQL.svg)](https://www.nuget.org/packages/Nodsoft.AspNetCore.SignalR.PostgreSQL)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Build](https://github.com/Nodsoft/SignalR.PostgreSQL/actions/workflows/build.yml/badge.svg)](https://github.com/Nodsoft/SignalR.PostgreSQL/actions)

A **PostgreSQL LISTEN/NOTIFY backplane** for ASP.NET Core SignalR.

When running multiple instances of an ASP.NET Core application that hosts SignalR hubs (e.g., behind a load balancer), a *backplane* is required so that a message sent on one server instance is forwarded to connections on all other instances. This library implements that backplane using PostgreSQL's built-in [`LISTEN` / `NOTIFY`](https://www.postgresql.org/docs/current/sql-notify.html) mechanism — no Redis, no Azure Service Bus, no additional infrastructure required beyond the PostgreSQL database you probably already have.

---

## Table of Contents

- [Features](#features)
- [How It Works](#how-it-works)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Limitations](#limitations)
- [Running the Spike](#running-the-spike)
- [Testing](#testing)
- [Contributing](#contributing)
- [License](#license)

---

## Features

- **Zero extra infrastructure** — uses only the PostgreSQL database your app already depends on.
- **Full SignalR routing support** — all `Send*` targeting strategies are supported:
  `SendAllAsync`, `SendAllExceptAsync`, `SendConnectionAsync`, `SendConnectionsAsync`,
  `SendGroupAsync`, `SendGroupExceptAsync`, `SendGroupsAsync`,
  `SendUserAsync`, `SendUsersAsync`.
- **Automatic reconnection** — the background LISTEN loop reconnects after transient database errors.
- **Self-delivery prevention** — a per-process server instance ID ensures a server never echoes its own outbound messages back to itself.
- **ASP.NET Core DI integration** — registers via a single `AddPostgreSqlBackplane(...)` call on the `ISignalRServerBuilder`.
- **Targets .NET 10** and uses `Npgsql` 10.x.

---

## How It Works

Each server instance runs a dedicated background connection that issues `LISTEN "signalr__<hubname>"`. When a hub method calls any `Send*` method, the manager serializes a `BackplaneMessage` to JSON and publishes it via `SELECT pg_notify(...)`. Every listening server instance (including the sender, which then discards its own messages via the `ServerInstanceId` field) receives the notification and routes the payload to its locally connected clients.

```
┌──────────────┐    pg_notify     ┌──────────────────────┐    LISTEN/NOTIFY    ┌──────────────┐
│  Server A    │ ──────────────► │  PostgreSQL Database  │ ──────────────────► │  Server B    │
│              │                 │  (NOTIFY channel)     │                     │              │
│  Client 1   │                 └──────────────────────┘                     │  Client 2   │
│  Client 3   │                                                               │  Client 4   │
└──────────────┘                                                               └──────────────┘
```

See [`docs/architecture.md`](docs/architecture.md) for a full technical deep-dive.

---

## Requirements

| Requirement | Minimum Version |
|---|---|
| .NET | 10.0 |
| ASP.NET Core | 10.0 |
| PostgreSQL | 10+ |
| Npgsql | 10.x |

---

## Installation

```bash
dotnet add package Nodsoft.AspNetCore.SignalR.PostgreSQL
```

Or via the NuGet Package Manager in Visual Studio / Rider.

---

## Quick Start

### 1. Register the backplane with a connection string

```csharp
// Program.cs
using Nodsoft.AspNetCore.SignalR.PostgreSQL;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSignalR()
    .AddPostgreSqlBackplane("Host=localhost;Database=myapp;Username=myuser;Password=secret");

var app = builder.Build();
app.MapHub<ChatHub>("/hubs/chat");
app.Run();
```

### 2. Register the backplane with an `NpgsqlDataSource`

If your application already configures an `NpgsqlDataSource` (e.g. via Aspire or `AddNpgsqlDataSource`), pass it directly to avoid opening a second connection pool:

```csharp
// Program.cs
using Nodsoft.AspNetCore.SignalR.PostgreSQL;

var builder = WebApplication.CreateBuilder(args);

builder.AddNpgsqlDataSource("signalr");   // Aspire-style registration

builder.Services.AddSignalR()
    .AddPostgreSqlBackplane(sp => sp.GetRequiredService<NpgsqlDataSource>());

var app = builder.Build();
app.MapHub<ChatHub>("/hubs/chat");
app.Run();
```

### 3. Register the backplane via options delegate

```csharp
builder.Services.AddSignalR()
    .AddPostgreSqlBackplane(options =>
    {
        options.ConnectionString = builder.Configuration.GetConnectionString("SignalR");
    });
```

### 4. Define your hub as normal

```csharp
// Hubs/ChatHub.cs
using Microsoft.AspNetCore.SignalR;

public class ChatHub : Hub
{
    public Task SendMessage(string user, string message)
        => Clients.All.SendAsync("ReceiveMessage", user, message);
}
```

No hub-specific code changes are needed — the backplane is transparent.

---

## Configuration

The backplane is configured through `PostgreSqlBackplaneOptions`:

| Property | Type | Description |
|---|---|---|
| `ConnectionString` | `string?` | An Npgsql connection string. Used when `DataSource` is not set. |
| `DataSource` | `NpgsqlDataSource?` | A pre-built data source. Takes precedence over `ConnectionString`. |

Exactly one of `ConnectionString` or `DataSource` must be provided; an `InvalidOperationException` is thrown at startup otherwise.

For a full reference, see [`docs/configuration.md`](docs/configuration.md).

---

## Limitations

### 8 KB payload limit

PostgreSQL's `NOTIFY` payloads are limited to **8 191 bytes**. The backplane checks payload size before publishing and drops messages that exceed this threshold with a warning log entry:

```
WARN  BackplaneMessage payload exceeds 8 KB and will not be delivered via NOTIFY
```

If your application sends large method arguments, consider one of the following:
- Reduce argument sizes (e.g., send IDs and let clients fetch data).
- Store large payloads in a shared table and send only a reference via NOTIFY.
- Use a different backplane technology (Redis, Azure Service Bus) for large-payload scenarios.

### Group membership is local only

Group membership (`AddToGroupAsync` / `RemoveFromGroupAsync`) is tracked **per server instance**. When a client connects to Server A and joins a group, Server B has no record of that membership. Messages sent to a group are broadcast to all servers via NOTIFY, but each server only delivers to its locally known group members. If a client reconnects to a different server, it must re-join its groups.

### No persistent message delivery

NOTIFY messages are ephemeral. If a server is offline when a notification is published, it will miss that message. This backplane is suitable for live, real-time scenarios, not guaranteed delivery.

### Channel name restrictions

Channel names are derived from the hub's type name (lowercased) in the format `signalr__<hubname>`. Hub type names must contain only lowercase alphanumeric characters and underscores. A hub named `ChatHub` → channel `signalr__chathub`. Hub names containing other characters (e.g., hyphens, Unicode) will cause an `InvalidOperationException` at startup.

---

## Running the Spike

A full working example is provided in the `spike/` directory. It includes:

- **`Spike.AppHost`** — .NET Aspire orchestration that starts PostgreSQL, the server, and the client.
- **`Spike.Server`** — ASP.NET Core server with `ChatHub` and the PostgreSQL backplane.
- **`Spike.Client`** — Blazor WebAssembly frontend demonstrating broadcast, group, and direct messaging.

### Prerequisites

- [.NET Aspire workload](https://learn.microsoft.com/en-us/dotnet/aspire/fundamentals/setup-tooling): `dotnet workload install aspire`
- Docker (for the PostgreSQL container started by Aspire)

### Run

```bash
cd spike/Spike.AppHost
dotnet run
```

Aspire will open the dashboard in your browser, start PostgreSQL in Docker, and launch the server and client. Navigate to the client URL shown in the dashboard.

---

## Testing

The solution contains two test projects:

| Project | Description |
|---|---|
| `Nodsoft.AspNetCore.SignalR.PostgreSQL.Tests` | Unit tests — mocked Npgsql, no database required. |
| `Nodsoft.AspNetCore.SignalR.PostgreSQL.IntegrationTests` | Integration tests — spins up a real PostgreSQL container via Testcontainers. |

### Run all tests

```bash
dotnet test
```

### Run only unit tests

```bash
dotnet test tests/Nodsoft.AspNetCore.SignalR.PostgreSQL.Tests
```

### Run integration tests

Integration tests require Docker:

```bash
dotnet test tests/Nodsoft.AspNetCore.SignalR.PostgreSQL.IntegrationTests
```

---

## Contributing

Contributions are very welcome! Please read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a pull request.

---

## License

This project is licensed under the **Apache License 2.0**. See [LICENSE](LICENSE) for details.

Copyright © [Nodsoft Systems](https://github.com/Nodsoft) — [Sakura Akeno Isayeki](https://github.com/SakuraIsayeki)
