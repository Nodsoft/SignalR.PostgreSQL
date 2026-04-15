using System.Security.Claims;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Nodsoft.AspNetCore.SignalR.PostgreSQL.IntegrationTests.Infrastructure;

/// <summary>
/// An in-process ASP.NET Core test server that hosts <see cref="TestHub"/> backed
/// by the PostgreSQL SignalR backplane.
///
/// Multiple instances of this factory can be created with the same
/// <paramref name="connectionString"/> to simulate separate server nodes sharing
/// the same PostgreSQL channel — the central pattern for backplane integration tests.
/// </summary>
public sealed class BackplaneWebApplicationFactory : WebApplicationFactory<BackplaneWebApplicationFactory>
{
    private readonly string _connectionString;

    /// <summary>
    /// When non-null, all connections on this factory will be authenticated as this user ID.
    /// </summary>
    private readonly string? _userId;

    public BackplaneWebApplicationFactory(string connectionString, string? userId = null)
    {
        _connectionString = connectionString;
        _userId = userId;
    }

    /// <summary>
    /// Returns a new factory instance where every connected client is identified as <paramref name="userId"/>.
    /// Uses a simple claim-injection middleware — suitable for testing user-scoped backplane routing.
    /// </summary>
    public BackplaneWebApplicationFactory WithUserAuth(string userId)
        => new(_connectionString, userId);

    /// <inheritdoc />
    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseEnvironment(Environments.Development);

        builder.ConfigureServices(services =>
        {
            if (_userId is not null)
            {
                // Register a fake authentication scheme that identifies every connection
                // as the fixed user ID — used to test user-scoped backplane routing.
                services.AddAuthentication(TestAuthHandler.SchemeName)
                        .AddScheme<AuthenticationSchemeOptions, TestAuthHandler>(
                            TestAuthHandler.SchemeName, _ => { });
            }

            services.AddSignalR()
                    .AddPostgreSqlBackplane(_connectionString);
        });

        builder.Configure(app =>
        {
            app.UseRouting();

            if (_userId is not null)
            {
                // Inject the fixed user claim for every request.
                app.Use(async (ctx, next) =>
                {
                    var identity = new ClaimsIdentity(
                        [new Claim(ClaimTypes.Name, _userId!)],
                        authenticationType: "Test");
                    ctx.User = new ClaimsPrincipal(identity);
                    await next(ctx);
                });

                app.UseAuthentication();
                app.UseAuthorization();
            }

            app.UseEndpoints(endpoints => endpoints.MapHub<TestHub>("/hubs/test"));
        });
    }

    /// <summary>
    /// Creates a <see cref="HubConnection"/> that connects to <see cref="TestHub"/>
    /// using the <c>LongPolling</c> transport — compatible with the <see cref="TestServer"/>
    /// in-process HTTP handler.
    /// </summary>
    public HubConnection CreateHubConnection()
    {
        return new HubConnectionBuilder()
            .WithUrl(new Uri(Server.BaseAddress, "hubs/test"), options =>
            {
                options.Transports = Microsoft.AspNetCore.Http.Connections.HttpTransportType.LongPolling;
                options.HttpMessageHandlerFactory = _ => Server.CreateHandler();
            })
            .Build();
    }

    // ── Fake authentication handler ────────────────────────────────────────

    /// <summary>
    /// A pass-through <see cref="AuthenticationHandler{TOptions}"/> that always succeeds,
    /// allowing the injected middleware <see cref="ClaimsPrincipal"/> to propagate correctly.
    /// </summary>
    private sealed class TestAuthHandler(
        IOptionsMonitor<AuthenticationSchemeOptions> options,
        ILoggerFactory logger,
        System.Text.Encodings.Web.UrlEncoder encoder)
        : AuthenticationHandler<AuthenticationSchemeOptions>(options, logger, encoder)
    {
        internal const string SchemeName = "Test";

        protected override Task<AuthenticateResult> HandleAuthenticateAsync()
        {
            // The user is already set by the inline middleware; just return success.
            if (Context.User.Identity?.IsAuthenticated == true)
            {
                return Task.FromResult(
                    AuthenticateResult.Success(
                        new AuthenticationTicket(Context.User, SchemeName)));
            }

            return Task.FromResult(AuthenticateResult.NoResult());
        }
    }
}

