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

        var tempConfig = config.Build();
        var valuesSection = tempConfig.GetSection("Values");
        foreach (var kvp in valuesSection.GetChildren())
        {
            config.AddInMemoryCollection([new KeyValuePair<string, string>(kvp.Key, kvp.Value)]);
        }
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
// Treat --Force as true if present, even if no value is provided
var forceBuild = false;
var forceArg = args.FirstOrDefault(a => string.Equals(a, "--Force", StringComparison.OrdinalIgnoreCase));
if (forceArg != null)
{
    forceBuild = true;
}
else
{
    forceBuild = configuration.GetValue<bool>("Force");
}

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
