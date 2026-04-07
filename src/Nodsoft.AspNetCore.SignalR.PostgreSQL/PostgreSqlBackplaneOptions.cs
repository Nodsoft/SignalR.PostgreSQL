namespace Nodsoft.AspNetCore.SignalR.PostgreSQL;

/// <summary>
/// Options for configuring the PostgreSQL SignalR backplane.
/// </summary>
public sealed class PostgreSqlBackplaneOptions
{
    /// <summary>
    /// Gets or sets the connection string for the PostgreSQL database.
    /// Either <see cref="ConnectionString"/> or <see cref="DataSource"/> must be provided.
    /// </summary>
    public string? ConnectionString { get; set; }

    /// <summary>
    /// Gets or sets an <see cref="NpgsqlDataSource"/> to use for backplane connections.
    /// Takes precedence over <see cref="ConnectionString"/> when set.
    /// </summary>
    public NpgsqlDataSource? DataSource { get; set; }
}
