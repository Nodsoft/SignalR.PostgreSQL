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
    /// Gets or sets the maximum size, in UTF-8 bytes, of a serialized backplane message
    /// that will be transmitted inline through <c>pg_notify</c> (no extra database round-trip).
    /// Messages whose serialized payload exceeds this threshold are routed through the
    /// outbox table when <see cref="UseOutbox"/> is <see langword="true"/>, or dropped otherwise.
    /// <para>
    /// PostgreSQL caps NOTIFY payloads at 8000 bytes; the default value of <c>7500</c> leaves
    /// a small safety margin. Values above 8000 are not honoured by PostgreSQL.
    /// </para>
    /// </summary>
    public int InlinePayloadThresholdBytes { get; set; } = 7500;

    /// <summary>
    /// Gets or sets a value indicating whether messages exceeding the
    /// <see cref="InlinePayloadThresholdBytes"/> threshold should be transparently delivered
    /// through an outbox table (<c>INSERT</c> + <c>NOTIFY</c> reference, then <c>SELECT</c> on the receiver side).
    /// When disabled, oversized payloads are dropped with a warning and never delivered.
    /// Defaults to <see langword="true"/>.
    /// </summary>
    public bool UseOutbox { get; set; } = true;

    /// <summary>
    /// Gets or sets the name of the PostgreSQL table used to stage outbox payloads.
    /// Must contain only lowercase letters, digits, and underscores (matching <c>^[a-z0-9_]+$</c>)
    /// to prevent SQL injection via identifier interpolation.
    /// Defaults to <c>signalr_backplane_outbox</c>.
    /// </summary>
    public string OutboxTableName { get; set; } = "signalr_backplane_outbox";

    /// <summary>
    /// Gets or sets the lifetime of an outbox row before it is cleaned up by the publisher.
    /// The row is created at <c>Send*Async</c> time and deleted by a scheduled background task
    /// after this delay, giving all listeners a chance to fetch the payload.
    /// Defaults to <c>30 seconds</c>.
    /// </summary>
    public TimeSpan OutboxExpiry { get; set; } = TimeSpan.FromSeconds(30);
}
