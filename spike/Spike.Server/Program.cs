using Nodsoft.AspNetCore.SignalR.PostgreSQL;
using Npgsql;
using Spike.Server.Hubs;

var builder = WebApplication.CreateBuilder(args);

builder.AddServiceDefaults();

// ── SignalR + PostgreSQL backplane ──────────────────────────────────────────

var connectionString = builder.Configuration.GetConnectionString("signalr")
    ?? builder.Configuration["PostgreSQL:ConnectionString"]
    ?? throw new InvalidOperationException("Missing PostgreSQL connection string 'signalr'.");

builder.AddNpgsqlDataSource("signalr");

builder.Services.AddSignalR()
    .AddPostgreSqlBackplane(dataSource: new NpgsqlDataSourceBuilder(connectionString).Build()); 

builder.Services.AddOptions<PostgreSqlBackplaneOptions>()
    .PostConfigure<NpgsqlDataSource>((o, ds) => o.DataSource = ds);


// ── CORS (allow the Blazor client) ──────────────────────────────────────────

builder.Services.AddCors(options =>
{
    options.AddPolicy("BlazorClient", policy =>
    {
        if (builder.Environment.IsDevelopment())
        {
            // NOTE: Spike/demo only – allows any localhost origin so the Blazor client
            // can connect regardless of the dynamic port assigned by Aspire or launchSettings.
            // Restrict to specific origins in production.
            policy.SetIsOriginAllowed(origin =>
                    Uri.TryCreate(origin, UriKind.Absolute, out var uri)
                    && uri.Host is "localhost" or "127.0.0.1" or "::1")
                .AllowAnyHeader()
                .AllowAnyMethod()
                .AllowCredentials();
        }
        else
        {
            var allowedOrigins = builder.Configuration.GetSection("Cors:AllowedOrigins").Get<string[]>()
                ?? [];

            policy.SetIsOriginAllowed(_ => true) // Allow any origin but restrict allowed origins in production via CORS policy configuration
                .AllowAnyHeader()
                .AllowAnyMethod()
                .AllowCredentials();
        }
    });
});

// ── OpenAPI ──────────────────────────────────────────────────────────────────

builder.Services.AddOpenApi();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();
app.UseCors("BlazorClient");

app.MapHub<ChatHub>("/hubs/chat");

app.Run();
