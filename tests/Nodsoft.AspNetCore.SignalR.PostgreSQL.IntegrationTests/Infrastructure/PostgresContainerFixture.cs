using Aspire.Hosting;
using Aspire.Hosting.Testing;

namespace Nodsoft.AspNetCore.SignalR.PostgreSQL.IntegrationTests.Infrastructure;

/// <summary>
/// An xUnit class fixture that provisions a PostgreSQL container via .NET Aspire
/// and exposes its connection string for use in integration tests.
/// The container is started once per test class and torn down after all tests complete.
/// </summary>
public sealed class PostgresContainerFixture : IAsyncLifetime
{
    private DistributedApplication? _app;

    /// <summary>
    /// Gets the ADO.NET connection string for the provisioned PostgreSQL database.
    /// Available after <see cref="InitializeAsync"/> completes.
    /// </summary>
    public string ConnectionString { get; private set; } = string.Empty;

    /// <inheritdoc />
    public async ValueTask InitializeAsync()
    {
        // Build a minimal Aspire distributed application that only provisions Postgres.
        // DistributedApplication.CreateBuilder() does not require a separate AppHost project.
        var builder = DistributedApplication.CreateBuilder([]);
        builder.AddPostgres("postgres")
               .AddDatabase("signalr");

        _app = builder.Build();
        await _app.StartAsync();

        // GetConnectionStringAsync is an Aspire.Hosting.Testing extension that resolves
        // the runtime connection string from the started resource.
        ConnectionString = await _app.GetConnectionStringAsync("signalr")
            ?? throw new InvalidOperationException("Could not retrieve connection string for 'signalr' database.");
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_app is not null)
        {
            await _app.StopAsync();
            await _app.DisposeAsync();
        }
    }
}
