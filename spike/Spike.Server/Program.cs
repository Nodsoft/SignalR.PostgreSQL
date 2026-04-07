using Nodsoft.AspNetCore.SignalR.PostgreSQL;
using Npgsql;
using Spike.Server.Hubs;

var builder = WebApplication.CreateBuilder(args);

// ── SignalR + PostgreSQL backplane ──────────────────────────────────────────

var connectionString = builder.Configuration.GetConnectionString("signalr")
    ?? builder.Configuration["PostgreSQL:ConnectionString"]
    ?? throw new InvalidOperationException("Missing PostgreSQL connection string 'signalr'.");

var dataSource = NpgsqlDataSource.Create(connectionString);

builder.Services.AddSignalR()
    .AddPostgreSqlBackplane(dataSource);

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
                    && (uri.Host == "localhost" || uri.Host == "127.0.0.1"))
                .AllowAnyHeader()
                .AllowAnyMethod()
                .AllowCredentials();
        }
        else
        {
            var allowedOrigins = builder.Configuration.GetSection("Cors:AllowedOrigins").Get<string[]>()
                ?? [];

            policy.WithOrigins(allowedOrigins)
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
