using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Azure.Storage.Blobs;
using Microsoft.Azure.Cosmos;
using PmtilesJob;

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

        if (command.Command is PmtilesCommandKind.BuildRaceTiles or PmtilesCommandKind.BuildAdminAreas)
        {
            var cosmosConnection = configuration.GetConnectionString("CosmosDBConnection")
                ?? configuration["CosmosDBConnection"]
                ?? throw new InvalidOperationException("CosmosDBConnection is not configured.");

            services.AddSingleton(new CosmosClient(cosmosConnection));

            if (command.Command == PmtilesCommandKind.BuildRaceTiles)
            {
                var blobConnection = configuration.GetConnectionString("BlobStorageConnection")
                    ?? configuration["BlobStorageConnection"]
                    ?? throw new InvalidOperationException("BlobStorageConnection is not configured.");

                services.AddSingleton(new BlobServiceClient(blobConnection));
                services.AddSingleton<RacePmtilesBuildService>();
            }

            if (command.Command == PmtilesCommandKind.BuildAdminAreas)
            {
                services.AddSingleton<AdminAreaPmtilesBuildService>();
            }
        }
    })
    .ConfigureLogging(logging =>
    {
        logging.ClearProviders();
        logging.AddConsole();
    });

using var host = builder.Build();
using var scope = host.Services.CreateScope();
var configuration = host.Services.GetRequiredService<IConfiguration>();
var logger = scope.ServiceProvider.GetRequiredService<ILogger<Program>>();
var command = PmtilesCommandLine.Parse(args, configuration);

try
{
    switch (command.Command)
    {
        case PmtilesCommandKind.FilterOutdoor:
        {
            var utilityService = scope.ServiceProvider.GetRequiredService<PmtilesUtilityService>();
            await utilityService.FilterOutdoorMapAsync(command.InputPath!, command.OutputPath!, CancellationToken.None);
            return 0;
        }

        case PmtilesCommandKind.FilterAdminBoundaries:
        {
            var utilityService = scope.ServiceProvider.GetRequiredService<PmtilesUtilityService>();
            await utilityService.FilterAdminBoundariesMapAsync(command.InputPath!, command.OutputPath!, CancellationToken.None);
            return 0;
        }

        case PmtilesCommandKind.BuildAdminAreas:
        {
            var job = scope.ServiceProvider.GetRequiredService<AdminAreaPmtilesBuildService>();
            await job.BuildAdminAreasAsync(command.OutputPath!, command.AdminLevels ?? AdminAreaPmtilesBuildService.DefaultAdminLevels, CancellationToken.None);
            return 0;
        }

        case PmtilesCommandKind.BuildRaceTiles:
        default:
        {
            var job = scope.ServiceProvider.GetRequiredService<RacePmtilesBuildService>();
            await job.BuildIfDirtyAsync(CancellationToken.None, command.ForceBuild);
            return 0;
        }
    }
}
catch (Exception ex)
{
    logger.LogError(ex, "Pmtiles job failed.");
    return 1;
}
