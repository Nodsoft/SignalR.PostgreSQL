using Microsoft.Extensions.DependencyInjection;

namespace Nodsoft.AspNetCore.SignalR.PostgreSQL.Tests;

/// <summary>
/// Unit tests for <see cref="PostgreSqlSignalRBuilderExtensions"/>.
/// Verifies guard clauses and that the correct <see cref="HubLifetimeManager{THub}"/>
/// implementation is registered in the DI container.
/// </summary>
public sealed class PostgreSqlSignalRBuilderExtensionsTests
{
    // ── AddPostgreSqlBackplane(dataSource) ────────────────────────────────────

    [Fact]
    public void AddPostgreSqlBackplane_DataSource_Throws_WhenBuilderIsNull()
    {
        ISignalRServerBuilder builder = null!;
        var dataSource = NpgsqlDataSource.Create("Host=localhost");

        Assert.Throws<ArgumentNullException>(() => builder.AddPostgreSqlBackplane(dataSource));
    }

    [Fact]
    public void AddPostgreSqlBackplane_DataSource_Throws_WhenDataSourceIsNull()
    {
        var services = new ServiceCollection();
        var builder = services.AddSignalR();

        Assert.Throws<ArgumentNullException>(() => builder.AddPostgreSqlBackplane((NpgsqlDataSource)null!));
    }

    [Fact]
    public void AddPostgreSqlBackplane_DataSource_RegistersHubLifetimeManager()
    {
        var services = new ServiceCollection();
        var dataSource = NpgsqlDataSource.Create("Host=localhost");
        services.AddSignalR().AddPostgreSqlBackplane(dataSource);

        // The backplane appends an open-generic descriptor; LastOrDefault resolves
        // the one that the DI container will actually use.
        var descriptor = services.LastOrDefault(
            d => d.ServiceType == typeof(HubLifetimeManager<>));

        Assert.NotNull(descriptor);
        Assert.Equal(ServiceLifetime.Singleton, descriptor.Lifetime);
        Assert.Equal(typeof(PostgreSqlHubLifetimeManager<>), descriptor.ImplementationType);
    }

    // ── AddPostgreSqlBackplane(connectionString) ──────────────────────────────

    [Fact]
    public void AddPostgreSqlBackplane_ConnectionString_Throws_WhenBuilderIsNull()
    {
        ISignalRServerBuilder builder = null!;

        Assert.Throws<ArgumentNullException>(() => builder.AddPostgreSqlBackplane("Host=localhost"));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void AddPostgreSqlBackplane_ConnectionString_Throws_WhenConnectionStringIsNullOrWhiteSpace(string? cs)
    {
        var services = new ServiceCollection();
        var builder = services.AddSignalR();

        // null produces ArgumentNullException (subtype); empty/whitespace produces ArgumentException.
        Assert.ThrowsAny<ArgumentException>(() => builder.AddPostgreSqlBackplane(cs!));
    }

    [Fact]
    public void AddPostgreSqlBackplane_ConnectionString_RegistersHubLifetimeManager()
    {
        var services = new ServiceCollection();
        services.AddSignalR().AddPostgreSqlBackplane("Host=localhost;Database=test");

        var descriptor = services.LastOrDefault(
            d => d.ServiceType == typeof(HubLifetimeManager<>));

        Assert.NotNull(descriptor);
        Assert.Equal(ServiceLifetime.Singleton, descriptor.Lifetime);
        Assert.Equal(typeof(PostgreSqlHubLifetimeManager<>), descriptor.ImplementationType);
    }

    // ── AddPostgreSqlBackplane(Action<options>) ───────────────────────────────

    [Fact]
    public void AddPostgreSqlBackplane_Action_Throws_WhenBuilderIsNull()
    {
        ISignalRServerBuilder builder = null!;

        Assert.Throws<ArgumentNullException>(() => builder.AddPostgreSqlBackplane(_ => { }));
    }

    [Fact]
    public void AddPostgreSqlBackplane_Action_Throws_WhenActionIsNull()
    {
        var services = new ServiceCollection();
        var builder = services.AddSignalR();

        Assert.Throws<ArgumentNullException>(() => builder.AddPostgreSqlBackplane((Action<PostgreSqlBackplaneOptions>)null!));
    }

    [Fact]
    public void AddPostgreSqlBackplane_Action_RegistersHubLifetimeManager()
    {
        var services = new ServiceCollection();
        services.AddSignalR().AddPostgreSqlBackplane(o => o.ConnectionString = "Host=localhost;Database=test");

        var descriptor = services.LastOrDefault(
            d => d.ServiceType == typeof(HubLifetimeManager<>));

        Assert.NotNull(descriptor);
        Assert.Equal(ServiceLifetime.Singleton, descriptor.Lifetime);
        Assert.Equal(typeof(PostgreSqlHubLifetimeManager<>), descriptor.ImplementationType);
    }

    // ── Open generic registration ─────────────────────────────────────────────

    [Fact]
    public void AddPostgreSqlBackplane_RegistersOpenGenericLifetimeManager()
    {
        var services = new ServiceCollection();
        services.AddSignalR().AddPostgreSqlBackplane("Host=localhost;Database=test");

        // There should be at least one descriptor for HubLifetimeManager<>
        // that points to PostgreSqlHubLifetimeManager<>.
        var descriptor = services.LastOrDefault(
            d => d.ServiceType == typeof(HubLifetimeManager<>));

        Assert.NotNull(descriptor);
        Assert.Equal(ServiceLifetime.Singleton, descriptor.Lifetime);
        Assert.Equal(typeof(PostgreSqlHubLifetimeManager<>), descriptor.ImplementationType);
    }
}
