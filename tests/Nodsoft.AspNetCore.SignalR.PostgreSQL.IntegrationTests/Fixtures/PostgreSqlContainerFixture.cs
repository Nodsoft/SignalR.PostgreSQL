using Testcontainers.PostgreSql;

namespace Nodsoft.AspNetCore.SignalR.PostgreSQL.IntegrationTests.Fixtures;

/// <summary>
/// xUnit v3 class fixture that starts a PostgreSQL container before the first test in
/// the collection and stops it after the last test.  Tests obtain a connection string
/// via <see cref="ConnectionString"/> and create <see cref="NpgsqlDataSource"/> instances
/// from it.
/// </summary>
public sealed class PostgreSqlContainerFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer _container = new PostgreSqlBuilder("postgres:16-alpine")
        .WithDatabase("signalr_test")
        .WithUsername("test")
        .WithPassword("test")
        .Build();

    /// <summary>Gets the connection string for the running PostgreSQL container.</summary>
    public string ConnectionString => _container.GetConnectionString();

    /// <summary>Creates a new <see cref="NpgsqlDataSource"/> backed by the test container.</summary>
    public NpgsqlDataSource CreateDataSource() => NpgsqlDataSource.Create(ConnectionString);

    public async ValueTask InitializeAsync() => await _container.StartAsync();

    public async ValueTask DisposeAsync() => await _container.DisposeAsync();
}
