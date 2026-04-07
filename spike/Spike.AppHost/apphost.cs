#:sdk Aspire.AppHost.Sdk@13.2.1
#:project ../Spike.Server/Spike.Server.csproj
#:project ../Spike.Client/Spike.Client.csproj
#:package Aspire.Hosting.PostgreSQL@*

using Projects;

var builder = DistributedApplication.CreateBuilder(args);

var postgres = builder.AddPostgres("postgres");
var signalrDb = postgres.AddDatabase("signalr");

var api = builder.AddProject<Spike_Server>("api")
    .WithReference(signalrDb)
    .WaitFor(signalrDb);

builder.AddProject<Spike_Client>("client")
    .WithReference(api)
    .WaitFor(api)
    .WithEnvironment("SignalR__HubUrl", api.GetEndpoint("http") + "/hubs/chat");

builder.Build().Run();
