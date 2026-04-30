using System.Reflection;

namespace Nodsoft.AspNetCore.SignalR.PostgreSQL.Tests.Helpers;

// ── Test hub types ───────────────────────────────────────────────────────────

/// <summary>A valid hub type whose name contains only lowercase alphanumerics.</summary>
public sealed class TestHub : Hub;

/// <summary>
/// A hub whose type name contains a hyphen-like character (via Unicode), which makes
/// it invalid for a PostgreSQL channel identifier.
/// </summary>
// Note: C# identifiers cannot contain hyphens, so we use a name that lowercases
// to something containing an underscore (always valid) – actual invalid character
// tests are performed using a custom manager subclass or via reflection.
public sealed class ValidHub2 : Hub;

// ── Manager factory helpers ──────────────────────────────────────────────────

/// <summary>
/// Provides factory methods for creating <see cref="PostgreSqlHubLifetimeManager{THub}"/>
/// instances wired to an unreachable PostgreSQL endpoint so that the background LISTEN task
/// starts but fails immediately and can be cleanly cancelled on disposal.
/// </summary>
internal static class ManagerFactory
{
    /// <summary>
    /// Connection string pointing to a port that is unlikely to be open locally.
    /// The LISTEN task fails within ~1 s and then enters a 5-second retry delay that
    /// is cancelled immediately when the manager is disposed.
    /// </summary>
    private const string BogusConnectionString =
        "Host=127.0.0.1;Port=9;Database=test;Timeout=1;Command Timeout=1;";

    /// <summary>Creates a <see cref="PostgreSqlHubLifetimeManager{THub}"/> backed by an unreachable data source.</summary>
    public static PostgreSqlHubLifetimeManager<THub> Create<THub>(
        NpgsqlDataSource? dataSource = null,
        Action<PostgreSqlBackplaneOptions>? configure = null)
        where THub : Hub
    {
        dataSource ??= NpgsqlDataSource.Create(BogusConnectionString);
        PostgreSqlBackplaneOptions opts = new() { DataSource = dataSource };
        configure?.Invoke(opts);
        IOptions<PostgreSqlBackplaneOptions> options = Options.Create(opts);
        return new(options, NullLogger<PostgreSqlHubLifetimeManager<THub>>.Instance);
    }

    /// <summary>
    /// Uses reflection to invoke the private delivery helper method with the given arguments.
    /// This is used to unit-test internal routing logic without requiring a real PostgreSQL connection.
    /// </summary>
    public static void InvokeDeliverToAll<THub>(
        PostgreSqlHubLifetimeManager<THub> manager,
        string methodName,
        object?[] args,
        IReadOnlyList<string> excluded)
        where THub : Hub
        => GetDeliveryMethod<THub>("DeliverToAll").Invoke(manager, [methodName, args, excluded]);

    /// <summary>Invokes the private <c>DeliverToConnection</c> helper via reflection.</summary>
    public static void InvokeDeliverToConnection<THub>(
        PostgreSqlHubLifetimeManager<THub> manager,
        string connectionId,
        string methodName,
        object?[] args)
        where THub : Hub
        => GetDeliveryMethod<THub>("DeliverToConnection").Invoke(manager, [connectionId, methodName, args]);

    /// <summary>Invokes the private <c>DeliverToGroup</c> helper via reflection.</summary>
    public static void InvokeDeliverToGroup<THub>(
        PostgreSqlHubLifetimeManager<THub> manager,
        string groupName,
        string methodName,
        object?[] args,
        IReadOnlyList<string> excluded)
        where THub : Hub
        => GetDeliveryMethod<THub>("DeliverToGroup").Invoke(manager, [groupName, methodName, args, excluded]);

    /// <summary>Invokes the private <c>DeliverToUser</c> helper via reflection.</summary>
    public static void InvokeDeliverToUser<THub>(
        PostgreSqlHubLifetimeManager<THub> manager,
        string userId,
        string methodName,
        object?[] args)
        where THub : Hub
        => GetDeliveryMethod<THub>("DeliverToUser").Invoke(manager, [userId, methodName, args]);

    /// <summary>
    /// Invokes the private <c>PublishAsync</c> method via reflection, passing an internal
    /// <c>BackplaneMessage</c> created via <see cref="CreateBackplaneMessage"/>.
    /// </summary>
    public static async Task InvokePublishAsync<THub>(
        PostgreSqlHubLifetimeManager<THub> manager,
        object backplaneMessage,
        CancellationToken cancellationToken)
        where THub : Hub
    {
        MethodInfo method = typeof(PostgreSqlHubLifetimeManager<THub>)
            .GetMethod("PublishAsync", BindingFlags.NonPublic | BindingFlags.Instance)
            ?? throw new MissingMethodException(typeof(PostgreSqlHubLifetimeManager<THub>).FullName, "PublishAsync");

        Task task = (Task)method.Invoke(manager, [backplaneMessage, cancellationToken])!;
        await task;
    }

    /// <summary>
    /// Invokes the private <c>OnNotification</c> handler with a synthetic Npgsql notification payload.
    /// </summary>
    public static void InvokeOnNotification<THub>(
        PostgreSqlHubLifetimeManager<THub> manager,
        string payload)
        where THub : Hub
    {
        MethodInfo method = typeof(PostgreSqlHubLifetimeManager<THub>)
            .GetMethod("OnNotification", BindingFlags.NonPublic | BindingFlags.Instance)
            ?? throw new MissingMethodException(typeof(PostgreSqlHubLifetimeManager<THub>).FullName, "OnNotification");

        // NpgsqlNotificationEventArgs only exposes an internal ctor that consumes a wire-protocol buffer,
        // so we synthesize the instance and populate the auto-property backing fields directly.
        NpgsqlNotificationEventArgs e =
            (NpgsqlNotificationEventArgs)System.Runtime.CompilerServices.RuntimeHelpers.GetUninitializedObject(typeof(NpgsqlNotificationEventArgs));
        SetBackingField(e, "PID", 0);
        SetBackingField(e, "Channel", "test_channel");
        SetBackingField(e, "Payload", payload);

        method.Invoke(manager, [manager, e]);
    }

    private static void SetBackingField(object instance, string propertyName, object value)
    {
        FieldInfo field = instance.GetType()
            .GetField($"<{propertyName}>k__BackingField", BindingFlags.NonPublic | BindingFlags.Instance)
            ?? throw new MissingFieldException(instance.GetType().FullName, propertyName);
        field.SetValue(instance, value);
    }

    /// <summary>
    /// Creates an instance of the internal <c>BackplaneMessage</c> record via reflection,
    /// for tests that need to invoke private <c>PublishAsync</c> directly.
    /// </summary>
    public static object CreateBackplaneMessage(string serverInstanceId, byte type, string methodName)
    {
        Assembly asm = typeof(PostgreSqlHubLifetimeManager<>).Assembly;
        Type messageType = asm.GetType("Nodsoft.AspNetCore.SignalR.PostgreSQL.Internal.BackplaneMessage")
            ?? throw new InvalidOperationException("BackplaneMessage type not found.");
        Type enumType = asm.GetType("Nodsoft.AspNetCore.SignalR.PostgreSQL.Internal.BackplaneMessageType")
            ?? throw new InvalidOperationException("BackplaneMessageType enum not found.");

        object instance = Activator.CreateInstance(messageType)!;
        messageType.GetProperty("ServerInstanceId")!.SetValue(instance, serverInstanceId);
        messageType.GetProperty("Type")!.SetValue(instance, Enum.ToObject(enumType, type));
        messageType.GetProperty("MethodName")!.SetValue(instance, methodName);
        return instance;
    }

    private static MethodInfo GetDeliveryMethod<THub>(string name) where THub : Hub
        => typeof(PostgreSqlHubLifetimeManager<THub>)
            .GetMethod(name, BindingFlags.NonPublic | BindingFlags.Instance)
            ?? throw new MissingMethodException(typeof(PostgreSqlHubLifetimeManager<THub>).FullName, name);
}
