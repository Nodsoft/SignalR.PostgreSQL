#:sdk Aspire.AppHost.Sdk@13.2.1
#:project ../Spike.Server/Spike.Server.csproj
#:project ../Spike.Client/Spike.Client.csproj
#:package Aspire.Hosting.PostgreSQL@*

using Microsoft.AspNetCore.Builder;
using Projects;

var builder = DistributedApplication.CreateBuilder(args);

var postgres = builder.AddPostgres("postgres");
postgres.WithPgAdmin(resourceBuilder =>
{
    resourceBuilder.WithHostPort(15432);
});

var signalrDb = postgres.AddDatabase("signalr");

var internalApi = builder.AddProject<Spike_Server>("internal-api")
    .WithReference(signalrDb)
    .WaitFor(signalrDb);

var publicApi = builder.AddProject<Spike_Server>("public-api", launchProfileName: "public")
    .WithReference(signalrDb)
    .WaitFor(signalrDb)
    .WithExternalHttpEndpoints();

var internalClient = builder.AddProject<Spike_Client>("internal-client")
    .WithReference(internalApi)
    .WaitFor(internalApi)
    .WithEnvironment("SignalR__HubUrl", $"{internalApi.GetEndpoint("http")}/hubs/chat");

var publicClient = builder.AddProject<Spike_Client>("public-client", launchProfileName: "public")
    .WithReference(publicApi)
    .WaitFor(publicApi)
    .WithEnvironment("SignalR__HubUrl", $"{publicApi.GetEndpoint("http")}/hubs/chat")
    // Expose to 0.0.0.0
    .WithExternalHttpEndpoints();


builder.Build().Run();
