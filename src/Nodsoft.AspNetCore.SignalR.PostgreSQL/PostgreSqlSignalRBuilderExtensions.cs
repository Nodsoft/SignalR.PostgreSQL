using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Npgsql;

namespace Nodsoft.AspNetCore.SignalR.PostgreSQL;

/// <summary>
/// Extension methods for configuring the PostgreSQL SignalR backplane.
/// </summary>
public static class PostgreSqlSignalRBuilderExtensions
{
    /// <summary>
    /// Adds the PostgreSQL backplane to SignalR using an <see cref="NpgsqlDataSource"/>.
    /// </summary>
    public static ISignalRServerBuilder AddPostgreSqlBackplane(
        this ISignalRServerBuilder builder,
        NpgsqlDataSource dataSource)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(dataSource);

        builder.Services.Configure<PostgreSqlBackplaneOptions>(o => o.DataSource = dataSource);
        RegisterBackplane(builder.Services);
        return builder;
    }

    /// <summary>
    /// Adds the PostgreSQL backplane to SignalR using a connection string.
    /// </summary>
    public static ISignalRServerBuilder AddPostgreSqlBackplane(
        this ISignalRServerBuilder builder,
        string connectionString)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        builder.Services.Configure<PostgreSqlBackplaneOptions>(o => o.ConnectionString = connectionString);
        RegisterBackplane(builder.Services);
        return builder;
    }

    /// <summary>
    /// Adds the PostgreSQL backplane to SignalR using an <see cref="Action{PostgreSqlBackplaneOptions}"/> delegate.
    /// </summary>
    public static ISignalRServerBuilder AddPostgreSqlBackplane(
        this ISignalRServerBuilder builder,
        Action<PostgreSqlBackplaneOptions> configureOptions)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configureOptions);

        builder.Services.Configure(configureOptions);
        RegisterBackplane(builder.Services);
        return builder;
    }

    private static void RegisterBackplane(IServiceCollection services)
    {
        services.TryAddSingleton(typeof(HubLifetimeManager<>), typeof(PostgreSqlHubLifetimeManager<>));
    }
}
