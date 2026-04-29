using Azure.Storage.Blobs;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Constants;
using Shared.Models;

namespace Backend;

public class QueueRaceTileBuildJobs
{
    private const string DirtyFlagBlobName = "dirty.flag";

    private readonly BlobServiceClient _blobServiceClient;
    private readonly ILogger<QueueRaceTileBuildJobs> _logger;

    public QueueRaceTileBuildJobs(
        BlobServiceClient blobServiceClient,
        ILogger<QueueRaceTileBuildJobs> logger)
    {
        _blobServiceClient = blobServiceClient;
        _logger = logger;
    }

    [Function(nameof(QueueRaceTileBuildJobs))]
    public async Task Run(
        [CosmosDBTrigger(
            databaseName: DatabaseConfig.CosmosDb,
            containerName: DatabaseConfig.RacesContainer,
            Connection = "CosmosDBConnection",
            LeaseContainerPrefix = "raceTileBuild",
            CreateLeaseContainerIfNotExists = true)]
        IReadOnlyList<StoredFeature> updatedRaces,
        CancellationToken cancellationToken)
    {
        if (updatedRaces is null || updatedRaces.Count == 0)
        {
            _logger.LogDebug("Race change feed triggered with no updated races.");
            return;
        }

        _logger.LogInformation("Race change feed detected {Count} updated race features. Skipping dirty flag (tile build now driven by raceOrganizers pipeline).", updatedRaces.Count);
    }

    private async Task MarkDirtyAsync(CancellationToken cancellationToken)
    {
        var container = _blobServiceClient.GetBlobContainerClient(BlobContainerNames.RaceTiles);
        await container.CreateIfNotExistsAsync(cancellationToken: cancellationToken);

        var dirtyBlob = container.GetBlobClient(DirtyFlagBlobName);
        await dirtyBlob.UploadAsync(BinaryData.FromString(DateTime.UtcNow.ToString("o")), overwrite: true, cancellationToken: cancellationToken);
        _logger.LogInformation("Marked race tiles dirty for rebuild in container {Container}.", BlobContainerNames.RaceTiles);
    }
}
