using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using BAMCIS.GeoJSON;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Shared.Constants;
using Shared.Models;
using Shared.Services;

namespace PmtilesJob;

/// <summary>
/// Builds race PMTiles directly from <c>raceOrganizers</c> Cosmos documents, bypassing the
/// <c>races</c> container. Runs assembly inline using <see cref="RaceAssembler"/>.
/// Intended to run in parallel with <see cref="RacePmtilesBuildService"/> during the transition.
/// </summary>
public class RaceFromOrganizersPmtilesBuildService
{
    private const string DefaultBlobContainerName = "race-tiles";
    private const string ProductionBlobName = "production/trails.pmtiles";
    private const string StagingPrefix = "staging";

    private readonly CosmosClient _cosmosClient;
    private readonly BlobServiceClient _blobServiceClient;
    private readonly PmtilesUtilityService _pmtilesUtilityService;
    private readonly ILogger<RaceFromOrganizersPmtilesBuildService> _logger;
    private readonly string _blobContainerName;

    public RaceFromOrganizersPmtilesBuildService(
        CosmosClient cosmosClient,
        BlobServiceClient blobServiceClient,
        PmtilesUtilityService pmtilesUtilityService,
        ILogger<RaceFromOrganizersPmtilesBuildService> logger,
        IConfiguration configuration)
    {
        _cosmosClient = cosmosClient;
        _blobServiceClient = blobServiceClient;
        _pmtilesUtilityService = pmtilesUtilityService;
        _logger = logger;
        _blobContainerName = configuration.GetValue<string>(AppConfig.RaceTilesBlobContainerName) ?? DefaultBlobContainerName;
    }

    public async Task BuildAsync(CancellationToken cancellationToken)
    {
        var runId = Guid.NewGuid().ToString("N");
        _logger.LogInformation("Starting race-from-organizers tile build {RunId}.", runId);

        try
        {
            var (geoJsonPath, featureCount) = await AssembleAndExportToGeoJsonAsync(runId, cancellationToken);

            _logger.LogInformation("Building PMTiles from {FeatureCount} features...", featureCount);
            var pmtilesPath = Path.Combine(CreateRunTempPath(runId), "trails-from-organizers.pmtiles");
            var pmtilesFeatureCount = await _pmtilesUtilityService.BuildPmtilesAsync(
                geoJsonPath,
                pmtilesPath,
                layerName: "trails",
                cancellationToken);

            _logger.LogInformation("Uploading PMTiles to blob storage...");
            var container = await GetContainerAsync(cancellationToken);
            await ValidateAndPublishPmtilesAsync(container, runId, pmtilesPath, cancellationToken);
            _logger.LogInformation("Race-from-organizers tile build completed successfully {RunId}. Wrote {FeatureCount} PMTiles features.", runId, pmtilesFeatureCount);
        }
        finally
        {
            DeleteRunTempPath(runId);
        }
    }

    private async Task<(string GeoJsonPath, int FeatureCount)> AssembleAndExportToGeoJsonAsync(string runId, CancellationToken cancellationToken)
    {
        var cosmosContainer = _cosmosClient.GetContainer(DatabaseConfig.CosmosDb, DatabaseConfig.RaceOrganizersContainer);
        var query = new QueryDefinition("SELECT * FROM c");
        var iterator = cosmosContainer.GetItemQueryIterator<RaceOrganizerDocument>(query);

        var features = new List<Feature>();
        var organizerCount = 0;

        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync(cancellationToken);
            foreach (var doc in page)
            {
                organizerCount++;
                var assembled = await RaceAssembler.AssembleRacesAsync(doc, geocodingService: null, cancellationToken);
                foreach (var storedFeature in assembled)
                {
                    var geoJsonFeature = storedFeature.ToFeature();
                    if (geoJsonFeature.Id is not null)
                    {
                        geoJsonFeature.Properties["featureId"] = geoJsonFeature.Id;
                    }

                    features.Add(geoJsonFeature);
                }

                if (organizerCount % 500 == 0)
                    _logger.LogInformation("Assembled {OrganizerCount} organizers so far, {FeatureCount} features...", organizerCount, features.Count);
            }
        }

        _logger.LogInformation("Assembled {FeatureCount} race features from {OrganizerCount} organizers. Serializing GeoJSON...", features.Count, organizerCount);

        var collection = new FeatureCollection(features);
        var tempPath = CreateRunTempPath(runId);
        var geoJsonPath = Path.Combine(tempPath, "features-from-organizers.geojson");

        var geoJson = collection.ToJson();
        await File.WriteAllTextAsync(geoJsonPath, geoJson, cancellationToken);

        return (geoJsonPath, features.Count);
    }

    private async Task ValidateAndPublishPmtilesAsync(BlobContainerClient container, string runId, string pmtilesPath, CancellationToken cancellationToken)
    {
        if (!PmtilesUtilityService.IsValidPmtilesFile(pmtilesPath))
            throw new InvalidOperationException("Built PMTiles file did not pass validation.");

        var stagingBlob = container.GetBlobClient($"{StagingPrefix}/{runId}/trails-from-organizers.pmtiles");
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

    private async Task<BlobContainerClient> GetContainerAsync(CancellationToken cancellationToken)
    {
        var container = _blobServiceClient.GetBlobContainerClient(_blobContainerName);
        await container.CreateIfNotExistsAsync(cancellationToken: cancellationToken);
        return container;
    }

    private void DeleteRunTempPath(string runId)
    {
        var tempPath = Path.Combine(Path.GetTempPath(), "race-tiles-from-organizers", runId);
        if (!Directory.Exists(tempPath))
            return;

        try
        {
            Directory.Delete(tempPath, recursive: true);
            _logger.LogTrace("Deleted temp path {TempPath} for run {RunId}.", tempPath, runId);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to delete temp path {TempPath} for run {RunId}.", tempPath, runId);
        }
    }

    private static string CreateRunTempPath(string runId)
    {
        var tempPath = Path.Combine(Path.GetTempPath(), "race-tiles-from-organizers", runId);
        Directory.CreateDirectory(tempPath);
        return tempPath;
    }
}
