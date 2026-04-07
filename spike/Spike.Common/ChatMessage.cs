namespace Spike.Common;

/// <summary>A chat message exchanged via the hub.</summary>
/// <param name="Username">Display name of the sender.</param>
/// <param name="Content">Message body.</param>
/// <param name="Timestamp">UTC time the message was sent.</param>
/// <param name="GroupName">Optional group name when the message targets a specific group.</param>
public record ChatMessage(
    string Username,
    string Content,
    DateTimeOffset Timestamp,
    string? GroupName = null);
