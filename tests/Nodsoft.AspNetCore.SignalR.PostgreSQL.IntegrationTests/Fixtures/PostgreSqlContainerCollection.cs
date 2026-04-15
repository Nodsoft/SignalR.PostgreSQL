namespace Nodsoft.AspNetCore.SignalR.PostgreSQL.IntegrationTests.Fixtures;

/// <summary>
/// xUnit v3 collection definition that associates <see cref="PostgreSqlContainerFixture"/>
/// with all tests in the <see cref="PostgreSqlBackplaneIntegrationTests"/> class.
/// The fixture starts the PostgreSQL container once for the entire collection.
/// </summary>
[CollectionDefinition(nameof(PostgreSqlContainerFixture))]
public sealed class PostgreSqlContainerCollection : ICollectionFixture<PostgreSqlContainerFixture>;
