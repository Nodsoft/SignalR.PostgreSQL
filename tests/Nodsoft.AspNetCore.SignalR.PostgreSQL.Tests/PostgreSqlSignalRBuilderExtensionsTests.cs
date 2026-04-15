using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Npgsql;

namespace Nodsoft.AspNetCore.SignalR.PostgreSQL.Tests;

/// <summary>
/// Verifies that the <c>AddPostgreSqlBackplane</c> extension methods register
/// <see cref="PostgreSqlHubLifetimeManager{THub}"/> as the
/// <see cref="HubLifetimeManager{THub}"/> singleton and surface the provided
/// options via <see cref="IOptions{PostgreSqlBackplaneOptions}"/>.
/// </summary>
public sealed class PostgreSqlSignalRBuilderExtensionsTests
{
    private sealed class SampleHub : Hub;

    // ── AddPostgreSqlBackplane(connectionString) ───────────────────────────

    [Fact]
    public void AddPostgreSqlBackplane_ConnectionString_RegistersHubLifetimeManager()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSignalR().AddPostgreSqlBackplane("Host=localhost;Database=test;Username=u;Password=p");

        // Verify via descriptor to avoid resolving the manager
        // (which starts a background listener and only implements IAsyncDisposable).
        Assert.Contains(services, sd =>
            sd.ServiceType == typeof(HubLifetimeManager<>)
            && sd.ImplementationType == typeof(PostgreSqlHubLifetimeManager<>));
    }

    [Fact]
    public void AddPostgreSqlBackplane_ConnectionString_SetsConnectionStringInOptions()
    {
        const string cs = "Host=localhost;Database=test;Username=u;Password=p";
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSignalR().AddPostgreSqlBackplane(cs);

        // IOptions is safe to resolve; it does not instantiate HubLifetimeManager.
        using var sp = services.BuildServiceProvider();

        var opts = sp.GetRequiredService<IOptions<PostgreSqlBackplaneOptions>>().Value;
        Assert.Equal(cs, opts.ConnectionString);
        Assert.Null(opts.DataSource);
    }

    [Fact]
    public void AddPostgreSqlBackplane_NullConnectionString_Throws()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        Assert.Throws<ArgumentNullException>(() =>
            services.AddSignalR().AddPostgreSqlBackplane((string)null!));
    }

    [Fact]
    public void AddPostgreSqlBackplane_EmptyConnectionString_Throws()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        Assert.Throws<ArgumentException>(() =>
            services.AddSignalR().AddPostgreSqlBackplane(""));
    }

    [Fact]
    public void AddPostgreSqlBackplane_WhitespaceConnectionString_Throws()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        Assert.Throws<ArgumentException>(() =>
            services.AddSignalR().AddPostgreSqlBackplane("   "));
    }

    // ── AddPostgreSqlBackplane(NpgsqlDataSource) ──────────────────────────

    [Fact]
    public void AddPostgreSqlBackplane_DataSource_RegistersHubLifetimeManager()
    {
        using var ds = NpgsqlDataSource.Create("Host=localhost;Database=test;Username=u;Password=p");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSignalR().AddPostgreSqlBackplane(ds);

        Assert.Contains(services, sd =>
            sd.ServiceType == typeof(HubLifetimeManager<>)
            && sd.ImplementationType == typeof(PostgreSqlHubLifetimeManager<>));
    }

    [Fact]
    public void AddPostgreSqlBackplane_DataSource_SetsDataSourceInOptions()
    {
        using var ds = NpgsqlDataSource.Create("Host=localhost;Database=test;Username=u;Password=p");
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSignalR().AddPostgreSqlBackplane(ds);

        using var sp = services.BuildServiceProvider();

        var opts = sp.GetRequiredService<IOptions<PostgreSqlBackplaneOptions>>().Value;
        Assert.Same(ds, opts.DataSource);
    }

    [Fact]
    public void AddPostgreSqlBackplane_NullDataSource_Throws()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        Assert.Throws<ArgumentNullException>(() =>
            services.AddSignalR().AddPostgreSqlBackplane((NpgsqlDataSource)null!));
    }

    // ── AddPostgreSqlBackplane(Action<PostgreSqlBackplaneOptions>) ─────────

    [Fact]
    public void AddPostgreSqlBackplane_ConfigureDelegate_RegistersHubLifetimeManager()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSignalR().AddPostgreSqlBackplane(o =>
            o.ConnectionString = "Host=localhost;Database=test;Username=u;Password=p");

        Assert.Contains(services, sd =>
            sd.ServiceType == typeof(HubLifetimeManager<>)
            && sd.ImplementationType == typeof(PostgreSqlHubLifetimeManager<>));
    }

    [Fact]
    public void AddPostgreSqlBackplane_ConfigureDelegate_SetsOptions()
    {
        const string cs = "Host=localhost;Database=test;Username=u;Password=p";
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSignalR().AddPostgreSqlBackplane(o => o.ConnectionString = cs);

        using var sp = services.BuildServiceProvider();

        var opts = sp.GetRequiredService<IOptions<PostgreSqlBackplaneOptions>>().Value;
        Assert.Equal(cs, opts.ConnectionString);
    }

    [Fact]
    public void AddPostgreSqlBackplane_NullDelegate_Throws()
    {
        var services = new ServiceCollection();
        services.AddLogging();

        Assert.Throws<ArgumentNullException>(() =>
            services.AddSignalR().AddPostgreSqlBackplane((Action<PostgreSqlBackplaneOptions>)null!));
    }

    // ── Null builder guard ─────────────────────────────────────────────────

    [Fact]
    public void AddPostgreSqlBackplane_NullBuilder_WithConnectionString_Throws()
    {
        Assert.Throws<ArgumentNullException>(() =>
            PostgreSqlSignalRBuilderExtensions.AddPostgreSqlBackplane(
                null!, "Host=localhost;Database=test;Username=u;Password=p"));
    }

    [Fact]
    public void AddPostgreSqlBackplane_NullBuilder_WithDataSource_Throws()
    {
        using var ds = NpgsqlDataSource.Create("Host=localhost;Database=test;Username=u;Password=p");
        Assert.Throws<ArgumentNullException>(() =>
            PostgreSqlSignalRBuilderExtensions.AddPostgreSqlBackplane(null!, ds));
    }

    [Fact]
    public void AddPostgreSqlBackplane_NullBuilder_WithDelegate_Throws()
    {
        Assert.Throws<ArgumentNullException>(() =>
            PostgreSqlSignalRBuilderExtensions.AddPostgreSqlBackplane(
                null!, (Action<PostgreSqlBackplaneOptions>)(_ => { })));
    }

    // ── Singleton lifetime ─────────────────────────────────────────────────

    [Fact]
    public void HubLifetimeManager_IsRegisteredAsSingleton()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSignalR().AddPostgreSqlBackplane("Host=localhost;Database=test;Username=u;Password=p");

        // The backplane registers an open-generic singleton:
        // services.AddSingleton(typeof(HubLifetimeManager<>), typeof(PostgreSqlHubLifetimeManager<>))
        var descriptor = services.First(sd =>
            sd.ServiceType == typeof(HubLifetimeManager<>)
            && sd.ImplementationType == typeof(PostgreSqlHubLifetimeManager<>));

        Assert.Equal(ServiceLifetime.Singleton, descriptor.Lifetime);
    }
}
