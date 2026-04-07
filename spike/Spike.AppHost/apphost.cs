#:sdk Aspire.AppHost.Sdk@13.2.1
#:project ../Spike.Server/Spike.Server.csproj
#:package Aspire.Hosting.PostgreSQL@*

using Projects;

var builder = DistributedApplication.CreateBuilder(args);

var postgres = builder.AddPostgres("postgres");
var signalr = postgres.AddDatabase("signalr");

var api = builder.AddProject<Spike_Server>("api");

builder.Build().Run();
