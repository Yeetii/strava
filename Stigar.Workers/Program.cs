using Stigar.Workers;
using Hangfire;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Stigar.Core.Constants;
using Stigar.Core.Jobs;

// Workers host. WORKER_ROLE selects which Hangfire queues this process serves:
//   schedule -> recurring jobs (weekly crons)
//   heavy    -> all typed job queues (queues and the two polling jobs)
// HTTP endpoints live in the API host; this process never serves HTTP.

var role = Environment.GetEnvironmentVariable("WORKER_ROLE")
           ?? Environment.GetEnvironmentVariable("WorkerRole")
           ?? "schedule";

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddBackendServices(builder.Configuration);
HangfireInfrastructure.AddHangfireStorage(builder.Services, builder.Configuration);

var assembly = typeof(BackendServices).Assembly;

// Register every native job implementation against both its concrete type and
// its Stigar.Core.Jobs contract so Hangfire can resolve it by interface.
foreach (var type in assembly.GetTypes().Where(type =>
             type.IsClass && !type.IsAbstract &&
             type.GetInterfaces().Any(IsJobContract)))
{
    builder.Services.AddSingleton(type);
    foreach (var contract in type.GetInterfaces().Where(IsJobContract))
        builder.Services.AddSingleton(contract, serviceProvider => serviceProvider.GetRequiredService(type));
}

var isHeavy = string.Equals(role, "heavy", StringComparison.OrdinalIgnoreCase);
var queues = QueueConfig.AllQueues.Select(JobEnqueueExtensions.NormalizeQueue).Distinct().ToList();

builder.Services.AddHangfireServer(options =>
{
    options.WorkerCount = 1;
    options.Queues = isHeavy
        ? [.. queues, AppConfig.BackendPollingQueueName]
        : [AppConfig.BackendPollingQueueName];
});

if (isHeavy)
{
    builder.Services.AddSingleton<AdminBoundaryMetricsPollingJob>();
    builder.Services.AddSingleton<IAdminBoundaryMetricsPollingJob>(sp =>
        sp.GetRequiredService<AdminBoundaryMetricsPollingJob>());
    builder.Services.AddSingleton<QueueActivityJobs>();
    builder.Services.AddSingleton<IQueueActivityJobs>(sp =>
        sp.GetRequiredService<QueueActivityJobs>());
    builder.Services.AddSingleton<IHostedService, PollingJobRegistrationHostedService>();
}

builder.AddRecurringJobs();

var host = builder.Build();

if (isHeavy)
{
    using var scope = host.Services.CreateScope();
    scope.ServiceProvider.GetRequiredService<IBackgroundJobClient>();
}

host.Run();
return 0;

static bool IsJobContract(Type type) =>
    type.Namespace == "Stigar.Core.Jobs" &&
    (type.Name.EndsWith("Job", StringComparison.Ordinal) || type == typeof(IRecurringJob));
