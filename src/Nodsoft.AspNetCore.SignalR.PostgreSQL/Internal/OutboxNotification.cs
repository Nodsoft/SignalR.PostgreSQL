namespace Nodsoft.AspNetCore.SignalR.PostgreSQL.Internal;

/// <summary>
/// Wire format for a NOTIFY payload that references a row in the backplane outbox table.
/// Used when the serialized <see cref="BackplaneMessage"/> exceeds the configured inline payload threshold
/// (PostgreSQL NOTIFY payloads are capped at ~8 KB), and the full message is stored in the outbox table
/// while only this small reference is sent through <c>pg_notify</c>.
/// </summary>
internal sealed record OutboxNotification
{
    /// <summary>
    /// Gets the identifier of the outbox row containing the serialized <see cref="BackplaneMessage"/> payload.
    /// </summary>
    public required string OutboxId { get; init; }
}
