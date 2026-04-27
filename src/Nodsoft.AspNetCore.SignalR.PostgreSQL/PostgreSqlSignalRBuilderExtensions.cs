using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.DependencyInjection;

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
        builder.Services.RegisterBackplane();
        return builder;
    }

    private static void RegisterBackplane(this IServiceCollection services)
    {
        services.AddSingleton(typeof(HubLifetimeManager<>), typeof(PostgreSqlHubLifetimeManager<>));
    }
}
