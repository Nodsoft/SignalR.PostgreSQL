using Nodsoft.AspNetCore.SignalR.PostgreSQL.IntegrationTests.Infrastructure;

namespace Nodsoft.AspNetCore.SignalR.PostgreSQL.IntegrationTests;

/// <summary>
/// Defines an xUnit collection that shares a single <see cref="PostgresContainerFixture"/>
/// across all test classes that declare <c>[Collection("Postgres")]</c>.
/// The fixture starts one PostgreSQL container for the entire test run and tears it
/// down once all tests in the collection have completed.
/// </summary>
[CollectionDefinition("Postgres")]
public sealed class PostgresCollection : ICollectionFixture<PostgresContainerFixture>;
