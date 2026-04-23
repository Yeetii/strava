using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Azure.Storage.Blobs;
using Microsoft.Azure.Cosmos;
using RaceTileJob;

var builder = Host.CreateDefaultBuilder(args)
    .ConfigureAppConfiguration((context, config) =>
    {
        config.AddJsonFile("appsettings.json", optional: true, reloadOnChange: false);
        config.AddJsonFile("local.settings.json", optional: true, reloadOnChange: false);
        config.AddEnvironmentVariables();
        config.AddCommandLine(args);
    })
    .ConfigureServices((context, services) =>
    {
        var configuration = context.Configuration;
        var cosmosConnection = configuration.GetConnectionString("CosmosDBConnection")
            ?? configuration["CosmosDBConnection"]
            ?? throw new InvalidOperationException("CosmosDBConnection is not configured.");

        var blobConnection = configuration.GetConnectionString("BlobStorageConnection")
            ?? configuration["BlobStorageConnection"]
            ?? throw new InvalidOperationException("BlobStorageConnection is not configured.");

        services.AddSingleton(new CosmosClient(cosmosConnection));
        services.AddSingleton(new BlobServiceClient(blobConnection));
        services.AddSingleton<RaceTileBuildService>();
    })
    .ConfigureLogging(logging =>
    {
        logging.ClearProviders();
        logging.AddConsole();
    });

using var host = builder.Build();
using var scope = host.Services.CreateScope();
var configuration = host.Services.GetRequiredService<IConfiguration>();
var job = scope.ServiceProvider.GetRequiredService<RaceTileBuildService>();
var logger = scope.ServiceProvider.GetRequiredService<ILogger<Program>>();
var forceBuild = configuration.GetValue<bool>("ForceRaceTileBuild");

try
{
    await job.BuildIfDirtyAsync(CancellationToken.None, forceBuild);
    return 0;
}
catch (Exception ex)
{
    logger.LogError(ex, "Race tile job failed.");
    return 1;
}
