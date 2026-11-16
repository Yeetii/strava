using Backend;
using Hangfire;
using Microsoft.AspNetCore.Http;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Shared.Hosting;
using System.Text.Json;

// Plain .NET host replacing the Functions worker for the Backend project.
// WORKER_ROLE selects the container role:
//   api       -> ASP.NET Core (Kestrel) hosting the backend HTTP functions
//   schedule  -> Generic Host running Hangfire recurring jobs (the 12 crons)
//   heavy     -> Generic Host running Service Bus consumers + Cosmos feeds
//
// Service Bus remains the queue transport in P1a; P2 swaps the producers and
// consumers to Hangfire behind a feature flag.

var role = Environment.GetEnvironmentVariable("WORKER_ROLE")
           ?? Environment.GetEnvironmentVariable("WorkerRole")
           ?? "api";

var registry = FunctionWorkerRegistry.Discover(typeof(Program).Assembly);

if (string.Equals(role, "api", StringComparison.OrdinalIgnoreCase))
{
    var app = WebApplication.CreateBuilder(args);
    app.Services.AddBackendServices(app.Configuration);
    QueueTransport.AddHangfireStorage(app.Services, app.Configuration);
    app.Services.AddCors(options => options.AddDefaultPolicy(policy =>
    {
        var origins = app.Configuration.GetValue<string>("AllowedOrigins")?
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries) ?? [];
        if (origins.Length > 0)
            policy.WithOrigins(origins).AllowAnyHeader().AllowAnyMethod().AllowCredentials();
        else
            policy.AllowAnyOrigin().AllowAnyHeader().AllowAnyMethod();
    }));

    var webApp = app.Build();
    FunctionsJson.Options = webApp.Services.GetRequiredService<IOptions<JsonSerializerOptions>>().Value;
    webApp.UseCors();
    webApp.MapFunctions(typeof(Program).Assembly);
    webApp.MapGet("/health", () => Results.Ok("ok"));
    webApp.Run();
    return;
}

// Worker roles: a Generic Host with Hangfire for timers and hosted services
// for queue consumers / change feeds.
var hostBuilder = Host.CreateApplicationBuilder(args);
hostBuilder.Services.AddBackendServices(hostBuilder.Configuration);

// Hangfire storage is registered exactly once here, not in BackendServices.
QueueTransport.AddHangfireStorage(hostBuilder.Services, hostBuilder.Configuration);
hostBuilder.Services.AddHangfireServer(options => options.WorkerCount = 1);

var isHeavy = string.Equals(role, "heavy", StringComparison.OrdinalIgnoreCase);
var isSchedule = string.Equals(role, "schedule", StringComparison.OrdinalIgnoreCase);
if (isSchedule)
    hostBuilder.AddFunctionTimers(registry, (descriptor, services, ct) =>
        FunctionInvoker.InvokeAsync(descriptor, services, ct));
hostBuilder.AddFunctionWorkers(
    registry,
    (descriptor, services, ct) => FunctionInvoker.InvokeAsync(descriptor, services, ct),
    enableQueues: isHeavy,
    enableChangeFeeds: isHeavy,
    includeQueue: queue => !QueueTransport.IsHangfire(queue));

// Timers run only in the schedule role; queue consumers and feeds only in the
// heavy role. P1a keeps both roles available so they can be taken over
// independently per the plan's handshake.
if (isSchedule)
    hostBuilder.Services.AddSingleton<IHostedService>(sp =>
        new TimerRegistrationHostedService(registry));

var host = hostBuilder.Build();

FunctionsJson.Options = new JsonSerializerOptions(JsonSerializerDefaults.Web);

if (isHeavy)
{
    using (var scope = host.Services.CreateScope())
    {
        var configuration = scope.ServiceProvider.GetRequiredService<IConfiguration>();
        // Hangfire storage schema is created on first use; force it here so the
        // empty-state check (P0 step 4) is observable at startup.
        scope.ServiceProvider.GetRequiredService<IBackgroundJobClient>();
    }
}

host.Run();

/// <summary>
/// No-op marker hosted service so the schedule role still starts when only
/// Hangfire recurring jobs are registered.
/// </summary>
public sealed class TimerRegistrationHostedService(FunctionWorkerRegistry registry) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
