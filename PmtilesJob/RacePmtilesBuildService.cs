using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using BAMCIS.GeoJSON;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Shared.Constants;
using Shared.Models;
using Shared.Services;

namespace PmtilesJob;

public class RacePmtilesBuildService
{
    private const string DefaultBlobContainerName = "race-tiles";
    private const string DirtyFlagBlobName = "dirty.flag";
    private const string ProductionBlobName = "production/trails.pmtiles";
    private const string StagingPrefix = "staging";

    private readonly CosmosClient _cosmosClient;
    private readonly BlobServiceClient _blobServiceClient;
    private readonly PmtilesUtilityService _pmtilesUtilityService;
    private readonly ILogger<RacePmtilesBuildService> _logger;
    private readonly string _blobContainerName;

    public RacePmtilesBuildService(
        CosmosClient cosmosClient,
        BlobServiceClient blobServiceClient,
        PmtilesUtilityService pmtilesUtilityService,
        ILogger<RacePmtilesBuildService> logger,
        IConfiguration configuration)
    {
        _cosmosClient = cosmosClient;
        _blobServiceClient = blobServiceClient;
        _pmtilesUtilityService = pmtilesUtilityService;
        _logger = logger;
        _blobContainerName = configuration.GetValue<string>(AppConfig.RaceTilesBlobContainerName) ?? DefaultBlobContainerName;
    }

    public async Task MarkDirtyAsync(CancellationToken cancellationToken)
    {
        var container = await GetContainerAsync(cancellationToken);
        var dirtyBlob = container.GetBlobClient(DirtyFlagBlobName);
        await dirtyBlob.UploadAsync(BinaryData.FromString(DateTime.UtcNow.ToString("o")), overwrite: true, cancellationToken: cancellationToken);
        _logger.LogInformation("Marked race tiles dirty for rebuild in container {Container}.", _blobContainerName);
    }

    public async Task BuildIfDirtyAsync(CancellationToken cancellationToken, bool forceBuild = false)
    {
        var container = await GetContainerAsync(cancellationToken);
        var dirtyBlob = container.GetBlobClient(DirtyFlagBlobName);
        var dirtyExists = await dirtyBlob.ExistsAsync(cancellationToken);

        if (!forceBuild && !dirtyExists)
        {
            _logger.LogInformation("No dirty marker found in container {Container}; race tile build skipped.", _blobContainerName);
            return;
        }

        var leaseClient = dirtyBlob.GetBlobLeaseClient();
        var leaseAcquired = false;
        string? runId = null;
        try
        {
            if (dirtyExists)
            {
                await leaseClient.AcquireAsync(TimeSpan.FromSeconds(-1), cancellationToken: cancellationToken);
                leaseAcquired = true;
            }

            runId = Guid.NewGuid().ToString("N");
            _logger.LogInformation("Starting race tile build {RunId}. ForceBuild={ForceBuild}.", runId, forceBuild);

            var (geoJsonPath, _) = await ExportAllRaceFeaturesToGeoJsonAsync(runId, cancellationToken);
            var pmtilesPath = Path.Combine(CreateRunTempPath(runId), "trails.pmtiles");
            var pmtilesFeatureCount = await _pmtilesUtilityService.BuildPmtilesAsync(
                geoJsonPath,
                pmtilesPath,
                layerName: "trails",
                cancellationToken);
            await ValidateAndPublishPmtilesAsync(container, runId, pmtilesPath, cancellationToken);

            if (leaseAcquired)
            {
                await dirtyBlob.DeleteIfExistsAsync(conditions: new BlobRequestConditions { LeaseId = leaseClient.LeaseId }, cancellationToken: cancellationToken);
                leaseAcquired = false;
            }

            _logger.LogInformation("Race tile build completed successfully {RunId}. Wrote {FeatureCount} PMTiles features.", runId, pmtilesFeatureCount);
        }
        catch (RequestFailedException ex) when (
            dirtyExists &&
            (ex.ErrorCode == BlobErrorCode.LeaseAlreadyPresent ||
            ex.ErrorCode == BlobErrorCode.LeaseAlreadyBroken))
        {
            _logger.LogInformation("Race tile build is already in progress. Skipping this run.");
            return;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Race tile build failed; leaving dirty marker for retry.");
            throw;
        }
        finally
        {
            if (leaseAcquired)
            {
                await leaseClient.ReleaseAsync(cancellationToken: cancellationToken);
            }

            if (!string.IsNullOrWhiteSpace(runId))
            {
                DeleteRunTempPath(runId);
            }
        }
    }

    private void DeleteRunTempPath(string runId)
    {
        var tempPath = Path.Combine(Path.GetTempPath(), "race-tiles", runId);
        if (!Directory.Exists(tempPath))
            return;

        try
        {
            Directory.Delete(tempPath, recursive: true);
            _logger.LogTrace("Deleted race tile temp path {TempPath} for run {RunId}.", tempPath, runId);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to delete race tile temp path {TempPath} for run {RunId}.", tempPath, runId);
        }
    }

    private async Task<BlobContainerClient> GetContainerAsync(CancellationToken cancellationToken)
    {
        var container = _blobServiceClient.GetBlobContainerClient(_blobContainerName);
        await container.CreateIfNotExistsAsync(cancellationToken: cancellationToken);
        return container;
    }

    private async Task<(string GeoJsonPath, int FeatureCount)> ExportAllRaceFeaturesToGeoJsonAsync(string runId, CancellationToken cancellationToken)
    {
        var container = _cosmosClient.GetContainer(DatabaseConfig.CosmosDb, DatabaseConfig.RacesContainer);
        var query = new QueryDefinition("SELECT * FROM c WHERE c.kind = @kind")
            .WithParameter("@kind", FeatureKinds.Race);

        var iterator = container.GetItemQueryIterator<StoredFeature>(query);
        var features = new List<Feature>();

        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync(cancellationToken);
            foreach (var feature in page)
            {
                var geoJsonFeature = feature.ToFeature();
                if (geoJsonFeature.Id is not null)
                {
                    geoJsonFeature.Properties["featureId"] = geoJsonFeature.Id;
                }

                features.Add(geoJsonFeature);
            }
        }

        var collection = new FeatureCollection(features);
        var tempPath = CreateRunTempPath(runId);
        var geoJsonPath = Path.Combine(tempPath, "features.geojson");

        var geoJson = collection.ToJson();
        await File.WriteAllTextAsync(geoJsonPath, geoJson, cancellationToken);

        _logger.LogInformation("Exported {Count} race features to GeoJSON for run {RunId}.", features.Count, runId);
        return (geoJsonPath, features.Count);
    }

    private async Task ValidateAndPublishPmtilesAsync(BlobContainerClient container, string runId, string pmtilesPath, CancellationToken cancellationToken)
    {
        if (!PmtilesUtilityService.IsValidPmtilesFile(pmtilesPath))
            throw new InvalidOperationException("Built PMTiles file did not pass validation.");

        var stagingBlob = container.GetBlobClient($"{StagingPrefix}/{runId}/trails.pmtiles");
        await stagingBlob.UploadAsync(pmtilesPath, overwrite: true, cancellationToken: cancellationToken);

        var productionBlob = container.GetBlobClient(ProductionBlobName);
        await productionBlob.StartCopyFromUriAsync(stagingBlob.Uri, cancellationToken: cancellationToken);

        BlobProperties productionProperties;
        do
        {
            await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
            productionProperties = await productionBlob.GetPropertiesAsync(cancellationToken: cancellationToken);
        }
        while (productionProperties.CopyStatus == CopyStatus.Pending);

        if (productionProperties.CopyStatus != CopyStatus.Success)
            throw new InvalidOperationException($"PMTiles publish failed with status {productionProperties.CopyStatus}.");

        await stagingBlob.DeleteIfExistsAsync(cancellationToken: cancellationToken);
        _logger.LogInformation("Published PMTiles to {ProductionPath} for run {RunId}.", ProductionBlobName, runId);
    }

    private static string CreateRunTempPath(string runId)
    {
        var tempPath = Path.Combine(Path.GetTempPath(), "race-tiles", runId);
        Directory.CreateDirectory(tempPath);
        return tempPath;
    }
}