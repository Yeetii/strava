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
    var defaultOrigins = new[]
    {
        "https://peakshunters.erikmagnusson.com",
        "https://stigar.erikmagnusson.com",
    };

    var configuredOrigins = app.Configuration.GetValue<string>("AllowedOrigins")?
        .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries) ?? [];

    var origins = configuredOrigins.Length > 0
        ? configuredOrigins.Union(defaultOrigins, StringComparer.OrdinalIgnoreCase).ToArray()
        : defaultOrigins;

    app.Services.AddCors(options => options.AddDefaultPolicy(policy =>
    {
        policy.WithOrigins(origins)
            .AllowAnyHeader()
            .AllowAnyMethod()
            .AllowCredentials()
            .WithExposedHeaders("Content-Range", "Content-Length", "ETag", "Accept-Ranges");
    }));

    var webApp = app.Build();
    FunctionsJson.Options = webApp.Services.GetRequiredService<IOptions<JsonSerializerOptions>>().Value;
    webApp.Use((context, next) =>
    {
        if (context.Request.Headers.ContainsKey("Access-Control-Request-Private-Network") ||
            context.Request.Headers.ContainsKey("Origin"))
        {
            context.Response.Headers.Append("Access-Control-Allow-Private-Network", "true");
        }

        return next(context);
    });
    webApp.UseCors();
    webApp.MapFunctions(typeof(Program).Assembly);
    webApp.MapGet("/health", () => Results.Ok("ok"));
    webApp.Run();
    return;
}

// Worker roles: a Generic Host with Hangfire for timers and queued jobs, and
// hosted services for change feeds.
var hostBuilder = Host.CreateApplicationBuilder(args);
hostBuilder.Services.AddBackendServices(hostBuilder.Configuration);

// Hangfire storage is registered exactly once here, not in BackendServices.
QueueTransport.AddHangfireStorage(hostBuilder.Services, hostBuilder.Configuration);

var isHeavy = string.Equals(role, "heavy", StringComparison.OrdinalIgnoreCase);
var isSchedule = string.Equals(role, "schedule", StringComparison.OrdinalIgnoreCase);

hostBuilder.Services.AddHangfireServer(options =>
{
    options.WorkerCount = 1;
    if (isHeavy)
        options.Queues = [Shared.Constants.AppConfig.BackendPollingQueueName, Hangfire.States.EnqueuedState.DefaultQueue];
});

if (isHeavy)
{
    // Hangfire recurring-job targets that replace the two Cosmos change feeds.
    // Register the shared contracts against the same instances: every other host
    // only needs the interface types to deserialize the recurring jobs.
    hostBuilder.Services.AddSingleton<AdminBoundaryMetricsPollingJob>();
    hostBuilder.Services.AddSingleton<IAdminBoundaryMetricsPollingJob>(sp =>
        sp.GetRequiredService<AdminBoundaryMetricsPollingJob>());
    hostBuilder.Services.AddSingleton<QueueActivityJobs>();
    hostBuilder.Services.AddSingleton<IQueueActivityJobs>(sp =>
        sp.GetRequiredService<QueueActivityJobs>());
    hostBuilder.Services.AddSingleton<IHostedService, PollingJobRegistrationHostedService>();
}

if (isSchedule)
    hostBuilder.AddFunctionTimers(registry, (descriptor, services, ct) =>
        FunctionInvoker.InvokeAsync(descriptor, services, ct));
hostBuilder.AddFunctionWorkers(registry);

// Timers run only in the schedule role. Queued jobs and the polling jobs that
// replaced the Cosmos change feeds run on the heavy role's Hangfire server.
// [TimerTrigger] recurring-job registration is done by
// RecurringJobRegistrationHostedService; the two polling jobs are registered by
// PollingJobRegistrationHostedService on a queue only the heavy worker serves.

var host = hostBuilder.Build();

FunctionsJson.Options = new JsonSerializerOptions(JsonSerializerDefaults.Web);

if (isHeavy)
{
    using var scope = host.Services.CreateScope();
    // Hangfire storage schema is created on first use; force it here so the
    // empty-state check (P0 step 4) is observable at startup.
    scope.ServiceProvider.GetRequiredService<IBackgroundJobClient>();
}

host.Run();
