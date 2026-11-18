using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Hangfire;
using Npgsql;
using PmtilesJob;
using Shared.Constants;
using Shared.Hosting;
using Shared.Services;
using Shared.Services.Postgres;
using Shared.Services.Postgres.Admin;
using Shared.Services.Postgres.Spatial;
using System.Net.Http.Headers;

var builder = Host.CreateDefaultBuilder(args)
    .ConfigureAppConfiguration((context, config) =>
    {
        config.AddJsonFile("appsettings.json", optional: true, reloadOnChange: false);
        config.AddJsonFile("local.settings.json", optional: true, reloadOnChange: false);
        config.AddEnvironmentVariables();
        config.AddCommandLine(args);

        var tempConfig = config.Build();
        var valuesSection = tempConfig.GetSection("Values");
        foreach (var kvp in valuesSection.GetChildren())
        {
            config.AddInMemoryCollection([new KeyValuePair<string, string?>(kvp.Key, kvp.Value)]);
        }
    })
    .ConfigureServices((context, services) =>
    {
        var command = PmtilesCommandLine.Parse(args, context.Configuration);
        var configuration = context.Configuration;
        services.AddSingleton<PmtilesUtilityService>();

        if (command.Command is PmtilesCommandKind.BuildRaceTilesFromOrganizers
            or PmtilesCommandKind.ScheduleRaceTiles)
        {
            services.AddOrganizerStore(configuration);
            services.AddSharedGeocodingCache(configuration);
            services.AddHttpClient<ILocationGeocodingService, NominatimLocationGeocodingService>(
                client =>
                {
                    client.BaseAddress = new Uri("https://nominatim.openstreetmap.org/");
                    client.DefaultRequestHeaders.UserAgent.Add(new ProductInfoHeaderValue("(https://peakshunters.erikmagnusson.com)"));
                    client.Timeout = TimeSpan.FromSeconds(10);
                });
            services.AddSingleton(serviceProvider => new RaceFromOrganizersPmtilesBuildService(
                serviceProvider.GetRequiredService<IOrganizerStore>(),
                serviceProvider.GetRequiredService<PmtilesUtilityService>(),
                serviceProvider.GetRequiredService<ILocationGeocodingService>(),
                serviceProvider.GetRequiredService<ILogger<RaceFromOrganizersPmtilesBuildService>>(),
                configuration,
                command.OutputPath));
        }

        if (command.Command == PmtilesCommandKind.ScheduleRaceTiles)
        {
            QueueTransport.AddHangfireStorage(services, configuration);
            // This host only has the pmtiles build service registered. Keep it on
            // its own queue so it never activates backend QueueJobDispatcher jobs
            // (which need the backend FunctionWorkerRegistry) from the shared store.
            services.AddHangfireServer(options =>
            {
                options.WorkerCount = 1;
                options.Queues = [Shared.Constants.AppConfig.RaceTilesQueueName];
            });
            services.AddSingleton<IRaceTilesBuildJob, RaceTilesSchedulerJob>();
            services.AddSingleton<IHostedService, RaceTilesRecurringJobRegistrationService>();
        }

        if (command.Command is PmtilesCommandKind.BuildAdminAreas)
        {
            string postgresConnectionString = configuration.GetValue<string>(DatabaseConfig.PostgresConnectionStringKey)
                ?? "Host=localhost;Port=5432;Database=strava;Username=strava;Password=strava";
            services.AddSingleton(NpgsqlDataSource.Create(postgresConnectionString));
            services.AddPostgresSpatialRepositories();
            services.AddPostgresAdminQueries();
            services.AddSingleton<AdminAreaPmtilesBuildService>();
        }
    })
    .ConfigureLogging(logging =>
    {
        logging.ClearProviders();
        logging.AddConsole();
        logging.AddJobConsoleLogging();
        logging.AddFilter("System.Net.Http.HttpClient.ILocationGeocodingService", LogLevel.Warning);
    });

using var host = builder.Build();
await host.StartAsync();
using var scope = host.Services.CreateScope();
var configuration = host.Services.GetRequiredService<IConfiguration>();
var logger = scope.ServiceProvider.GetRequiredService<ILogger<Program>>();
var cancellationToken = host.Services.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping;
var command = PmtilesCommandLine.Parse(args, configuration);

try
{
    switch (command.Command)
    {
        case PmtilesCommandKind.FilterOutdoor:
        {
            var utilityService = scope.ServiceProvider.GetRequiredService<PmtilesUtilityService>();
                await utilityService.FilterOutdoorMapAsync(
                    command.InputPath!,
                    command.OutputPath!,
                    command.MaximumZoom,
                    command.ExcludeAllAttributes,
                    cancellationToken);
                return 0;
            }

        case PmtilesCommandKind.FilterAdminBoundaries:
        {
            var utilityService = scope.ServiceProvider.GetRequiredService<PmtilesUtilityService>();
                await utilityService.FilterAdminBoundariesMapAsync(
                    command.InputPath!,
                    command.OutputPath!,
                    command.MaximumZoom,
                    command.ExcludeAllAttributes,
                    cancellationToken);
                return 0;
            }

        case PmtilesCommandKind.BuildAdminAreas:
        {
            var job = scope.ServiceProvider.GetRequiredService<AdminAreaPmtilesBuildService>();
            await job.BuildAdminAreasAsync(command.OutputPath!, command.AdminLevels ?? AdminAreaPmtilesBuildService.DefaultAdminLevels, cancellationToken);
            return 0;
        }

        case PmtilesCommandKind.ScheduleRaceTiles:
        {
            // The hosted service registers the recurring job and the Hangfire server
            // executes it. Keep the generic host alive until shutdown is requested.
            await host.WaitForShutdownAsync();
            return 0;
        }

        case PmtilesCommandKind.BuildRaceTilesFromOrganizers:
        default:
        {
            var job = scope.ServiceProvider.GetRequiredService<RaceFromOrganizersPmtilesBuildService>();
	    	if (!string.IsNullOrWhiteSpace(command.OrganizerId))
            {
                await job.DebugAssembleOrganizerAsync(command.OrganizerId, cancellationToken);
                return 0;
            }

            await job.BuildAsync(command.WriteTransparency, cancellationToken);
            return 0;
        }
    }
}
catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
{
    logger.LogInformation("Pmtiles job cancelled.");
    return 130;
}
catch (Exception ex)
{
    logger.LogError(ex, "Pmtiles job failed.");
    return 1;
}
finally
{
    await host.StopAsync();
}
