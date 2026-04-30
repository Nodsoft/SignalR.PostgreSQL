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

    /// <summary>
    /// Gets or sets the maximum UTF-8 byte size of a serialized backplane payload that may be sent
    /// inline via <c>NOTIFY</c>. Payloads larger than this are persisted to the outbox table
    /// and a small reference is sent via <c>NOTIFY</c> instead.
    /// </summary>
    /// <remarks>
    /// PostgreSQL imposes a hard 8,000-byte limit on <c>NOTIFY</c> payloads. The actual on-the-wire
    /// payload is the serialized JSON prefixed by a 2-byte routing tag (<c>I:</c>). The default of
    /// 7,500 bytes therefore leaves a ~498-byte safety margin to absorb the tag and any minor
    /// encoding overhead. The reference path uses a fixed 38-byte payload (<c>R:&lt;uuid&gt;</c>)
    /// which always fits comfortably.
    /// </remarks>
    public int OutboxThresholdBytes { get; set; } = 7_500;

    /// <summary>
    /// Gets or sets the name of the table used to persist outbox payloads that exceed
    /// <see cref="OutboxThresholdBytes"/>. May be schema-qualified (e.g. <c>"myschema.signalr_outbox"</c>).
    /// Identifiers must match <c>^[a-z_][a-z0-9_]*(\.[a-z_][a-z0-9_]*)?$</c>.
    /// </summary>
    public string OutboxTableName { get; set; } = "signalr_outbox";

    /// <summary>
    /// Gets or sets how long an outbox row is retained before being eligible for deletion by the
    /// background cleanup loop. Must be greater than the longest expected processing lag between
    /// a publisher writing an outbox row and all subscribers reading it.
    /// </summary>
    public TimeSpan OutboxRetention { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>
    /// Gets or sets how often the background cleanup loop runs to delete outbox rows older than
    /// <see cref="OutboxRetention"/>. Set to <see cref="TimeSpan.Zero"/> to disable cleanup
    /// (in which case the application is responsible for purging rows externally).
    /// </summary>
    public TimeSpan OutboxCleanupInterval { get; set; } = TimeSpan.FromSeconds(30);
}
