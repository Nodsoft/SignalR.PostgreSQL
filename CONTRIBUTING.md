# Contributing to Nodsoft.AspNetCore.SignalR.PostgreSQL

Thank you for your interest in contributing! This document explains how to set up the development environment, the project structure, the branching model, code style expectations, and the pull-request process.

---

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Prerequisites](#prerequisites)
- [Project Structure](#project-structure)
- [Branching Strategy](#branching-strategy)
- [Building](#building)
- [Testing](#testing)
- [Code Style](#code-style)
- [Submitting a Pull Request](#submitting-a-pull-request)
- [Reporting Issues](#reporting-issues)

---

## Code of Conduct

Please be respectful and constructive in all interactions. We follow the [Contributor Covenant](https://www.contributor-covenant.org/) code of conduct. Harassment, personal attacks, and discriminatory language are not tolerated.

---

## Prerequisites

| Tool | Version | Notes |
|---|---|---|
| .NET SDK | 10.0+ | [Download](https://dotnet.microsoft.com/download) |
| Docker | any recent version | Required for integration tests (Testcontainers) and the spike (Aspire) |
| .NET Aspire workload | latest | For running the `spike/` demo: `dotnet workload install aspire` |

A PostgreSQL server is **not** required locally; integration tests provision one automatically via [Testcontainers](https://dotnet.testcontainers.org/).

---

## Project Structure

```
SignalR.PostgreSQL/
├── src/
│   └── Nodsoft.AspNetCore.SignalR.PostgreSQL/       # Main library
│       ├── Internal/
│       │   └── BackplaneMessage.cs                  # Backplane message record + routing enum
│       ├── PostgreSqlBackplaneOptions.cs            # Configuration options
│       ├── PostgreSqlHubLifetimeManager.cs          # Core HubLifetimeManager implementation
│       └── PostgreSqlSignalRBuilderExtensions.cs    # DI extension methods
├── tests/
│   ├── Nodsoft.AspNetCore.SignalR.PostgreSQL.Tests/ # Unit tests (no database)
│   └── Nodsoft.AspNetCore.SignalR.PostgreSQL.IntegrationTests/  # Integration tests (Testcontainers)
├── spike/                                           # End-to-end demo with .NET Aspire
│   ├── Spike.AppHost/                               # Aspire orchestration
│   ├── Spike.Client/                                # Blazor WebAssembly client
│   ├── Spike.Common/                                # Shared models / hub contracts
│   ├── Spike.Server/                                # ASP.NET Core server
│   └── Spike.ServiceDefaults/                       # Shared Aspire service defaults
├── Directory.Build.props                            # Shared MSBuild properties and package metadata
├── SignalR.PostgreSQL.slnx                          # Solution file
└── LICENSE
```

### Key files to understand

| File | Purpose |
|---|---|
| `PostgreSqlHubLifetimeManager.cs` | The heart of the library: tracks connections/groups/users, publishes via `pg_notify`, and routes incoming notifications. |
| `BackplaneMessage.cs` | The JSON-serializable message record exchanged between server instances. |
| `PostgreSqlSignalRBuilderExtensions.cs` | Extension methods that wire the lifetime manager into the ASP.NET Core DI container. |
| `PostgreSqlBackplaneOptions.cs` | The options class consumed by the lifetime manager. |

---

## Branching Strategy

| Branch | Purpose |
|---|---|
| `main` | Stable, release-ready code. Protected. Direct pushes not allowed. |
| `develop` | Integration branch for features. All PRs should target `develop`. |
| `feature/<name>` | Feature branches, branched from and merged back into `develop`. |
| `fix/<name>` | Bug-fix branches. |
| `docs/<name>` | Documentation-only branches. |

When in doubt, branch from `develop` and open your PR against `develop`.

---

## Building

```bash
# Restore dependencies
dotnet restore

# Build the entire solution
dotnet build

# Build only the library
dotnet build src/Nodsoft.AspNetCore.SignalR.PostgreSQL
```

The solution targets **net10.0**. Ensure your `dotnet --version` is at least `10.0.x`.

---

## Testing

### Unit tests (no external dependencies)

```bash
dotnet test tests/Nodsoft.AspNetCore.SignalR.PostgreSQL.Tests
```

These tests mock Npgsql and do not require a running database.

### Integration tests (requires Docker)

```bash
dotnet test tests/Nodsoft.AspNetCore.SignalR.PostgreSQL.IntegrationTests
```

[Testcontainers](https://dotnet.testcontainers.org/) automatically pulls and starts a PostgreSQL Docker image. Make sure Docker is running before executing these tests.

### All tests

```bash
dotnet test
```

### Test coverage

If you add new functionality, please include corresponding tests:

- Pure logic (serialization, routing decisions, DI registration) → unit test.
- Actual LISTEN/NOTIFY behavior across two server instances → integration test.

---

## Code Style

The project enforces code style via `.editorconfig` / Roslyn analyzer rules baked into `Directory.Build.props` (`EnforceCodeStyleInBuild=True`, `AnalysisLevel=latest`). The build will fail if style violations are introduced.

General guidelines:

- **Nullable reference types** are enabled. Annotate all public and internal APIs correctly.
- **`var` vs explicit types** — use explicit types for local variables where the type is not immediately obvious from the right-hand side; use `var` only when the type is evident (e.g., `var cmd = connection.CreateCommand()`).
- **Target-typed `new`** — use `new()` when the type is already declared on the left.
- **Pattern matching** — prefer `is { Property: var x }` patterns over null checks followed by property access.
- **XML documentation** — all `public` and `internal` members that are part of the library's API surface must have `<summary>` documentation.
- **Logging** — use structured logging (`LogInformation("Listening on '{Channel}'", channelName)`) rather than string interpolation in log calls.
- **No magic strings** — channel names, JSON property names, etc. should be derived programmatically or declared as `private const`.

---

## Submitting a Pull Request

1. **Fork** the repository and create your branch from `develop`:
   ```bash
   git checkout -b feature/my-feature develop
   ```

2. **Make your changes**, keeping commits focused and atomic. Write descriptive commit messages following [Conventional Commits](https://www.conventionalcommits.org/) where possible:
   ```
   feat(backplane): add configurable reconnection delay
   fix(channel): reject hub names containing non-ASCII characters
   docs: update configuration reference
   ```

3. **Write or update tests** for any changed behavior.

4. **Run the full test suite** and ensure everything passes:
   ```bash
   dotnet test
   ```

5. **Open a pull request** against `develop`. Fill in the PR template completely, including:
   - A description of *what* changed and *why*.
   - Links to any related issues.
   - Notes on any breaking changes.

6. **Address review feedback**. A maintainer will review the PR and may request changes. Once approved, it will be squash-merged into `develop`.

### PR checklist

- [ ] New or updated unit/integration tests for changed behaviour
- [ ] XML documentation updated for any new or changed public/internal API
- [ ] `dotnet build` passes with zero warnings
- [ ] `dotnet test` passes locally
- [ ] Commit messages follow Conventional Commits style
- [ ] No unrelated changes are included

---

## Reporting Issues

Please use the [GitHub Issues](https://github.com/Nodsoft/SignalR.PostgreSQL/issues) tracker.

- For **bugs**, use the [Bug Report](.github/ISSUE_TEMPLATE/bug_report.md) template. Include the library version, .NET version, PostgreSQL version, and a minimal reproducible example.
- For **feature requests**, use the [Feature Request](.github/ISSUE_TEMPLATE/feature_request.md) template. Describe the use case, not just the desired API change.
- For **questions**, open a Discussion rather than an Issue.

---

## License

By contributing, you agree that your contributions will be licensed under the [Apache License 2.0](LICENSE).
