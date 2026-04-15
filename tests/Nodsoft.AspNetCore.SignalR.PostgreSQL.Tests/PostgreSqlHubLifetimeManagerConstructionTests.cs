using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Npgsql;

namespace Nodsoft.AspNetCore.SignalR.PostgreSQL.Tests;

/// <summary>
/// Validates constructor-time guards in <see cref="PostgreSqlHubLifetimeManager{THub}"/>:
/// hub-name character validation and mandatory-options checks.
/// </summary>
public sealed class PostgreSqlHubLifetimeManagerConstructionTests
{
    // A hub whose lowercase name passes the regex: ^[a-z0-9_]+$
    private sealed class ValidHub : Hub;

    // Hub names that must be rejected at construction time.
    // The raw type name is lower-cased before the regex check, so any non-[a-z0-9_]
    // character (including hyphens, dots, or Unicode) triggers the guard.
    private sealed class Hub_With_Hyphens : Hub;      // lowercase: "hub_with_hyphens" — actually valid
    private sealed class HubWithDotInName : Hub;      // lowercase: "hubwithdotinname" — valid, used for option tests

    // A type whose *lowercase* name contains a character outside [a-z0-9_].
    // We can trigger this by nesting a type whose mangled name contains '+' or '<'.
    // The simplest approach is a generic hub — its name is e.g. "validhub`1".
    private sealed class GenericHub<T> : Hub;         // name contains backtick → invalid

    // ─────────────────────────────────────────────────────────────────────────

    private static PostgreSqlHubLifetimeManager<THub> CreateManager<THub>(
        string? connectionString = "Host=localhost;Database=test;Username=u;Password=p",
        NpgsqlDataSource? dataSource = null)
        where THub : Hub
    {
        var opts = new PostgreSqlBackplaneOptions();

        if (dataSource is not null)
        {
            opts.DataSource = dataSource;
        }
        else if (connectionString is not null)
        {
            opts.ConnectionString = connectionString;
        }

        var options = Options.Create(opts);
        var logger = NullLogger<PostgreSqlHubLifetimeManager<THub>>.Instance;

        return new PostgreSqlHubLifetimeManager<THub>(options, logger);
    }

    // ── Missing-options guard ──────────────────────────────────────────────

    [Fact]
    public void Constructor_Throws_WhenNeitherConnectionStringNorDataSourceIsSet()
    {
        var options = Options.Create(new PostgreSqlBackplaneOptions());
        var logger = NullLogger<PostgreSqlHubLifetimeManager<ValidHub>>.Instance;

        var ex = Assert.Throws<InvalidOperationException>(
            () => new PostgreSqlHubLifetimeManager<ValidHub>(options, logger));

        Assert.Contains("DataSource", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ConnectionString", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    // ── Hub-name character validation ──────────────────────────────────────

    [Fact]
    public void Constructor_Throws_WhenHubTypeNameContainsInvalidCharacters()
    {
        // GenericHub<T> has a raw name like "GenericHub`1" which lower-cases to "generichub`1"
        // The backtick fails the ^[a-z0-9_]+$ regex.
        var ex = Assert.Throws<InvalidOperationException>(
            () => CreateManager<GenericHub<int>>());

        Assert.Contains("GenericHub`1", ex.Message);
    }

    [Fact]
    public async Task Constructor_Succeeds_WhenHubTypeNameIsAlphanumeric()
    {
        // Should not throw — name is "validhub" after lowercasing.
        await using var manager = CreateManager<ValidHub>();
        Assert.NotNull(manager);
    }

    // ── DataSource overrides ConnectionString ──────────────────────────────

    [Fact]
    public async Task Constructor_Succeeds_WhenOnlyDataSourceIsProvided()
    {
        var ds = NpgsqlDataSource.Create("Host=localhost;Database=test;Username=u;Password=p");
        await using var manager = CreateManager<ValidHub>(connectionString: null, dataSource: ds);
        Assert.NotNull(manager);
    }

    [Fact]
    public async Task Constructor_Succeeds_WhenOnlyConnectionStringIsProvided()
    {
        await using var manager = CreateManager<ValidHub>(
            connectionString: "Host=localhost;Database=test;Username=u;Password=p");
        Assert.NotNull(manager);
    }

    // ── IAsyncDisposable ──────────────────────────────────────────────────

    [Fact]
    public async Task DisposeAsync_CompletesWithoutException()
    {
        var manager = CreateManager<ValidHub>();
        await manager.DisposeAsync();
        // No assertion needed — just verify it doesn't throw.
    }
}
