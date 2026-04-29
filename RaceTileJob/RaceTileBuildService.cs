using System.Diagnostics;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
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

namespace RaceTileJob;

public class RaceTileBuildService
{
    private const string DefaultTippecanoeBinary = "/usr/local/bin/tippecanoe";
    private const string DefaultBlobContainerName = "race-tiles";
    private const string DirtyFlagBlobName = "dirty.flag";
    private const string ProductionBlobName = "production/trails.pmtiles";
    private const string StagingPrefix = "staging";
    private const int MinimumPmtilesSizeBytes = 1024;

    private readonly CosmosClient _cosmosClient;
    private readonly BlobServiceClient _blobServiceClient;
    private readonly ILogger<RaceTileBuildService> _logger;
    private readonly JsonSerializerOptions _jsonOptions;
    private readonly string _blobContainerName;
    private readonly string _tippecanoeBinary;

    public RaceTileBuildService(
        CosmosClient cosmosClient,
        BlobServiceClient blobServiceClient,
        ILogger<RaceTileBuildService> logger,
        IConfiguration configuration)
    {
        _cosmosClient = cosmosClient;
        _blobServiceClient = blobServiceClient;
        _logger = logger;
        _blobContainerName = configuration.GetValue<string>(AppConfig.RaceTilesBlobContainerName) ?? DefaultBlobContainerName;
        _tippecanoeBinary = configuration.GetValue<string>(AppConfig.TippecanoeBinaryPath)
            ?? FindTippecanoeBinary()
            ?? DefaultTippecanoeBinary;

        _jsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        };
        _jsonOptions.Converters.Add(new GeometrySystemTextJsonConverter());
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

            var (geoJsonPath, geoJsonFeatureCount) = await ExportAllRaceFeaturesToGeoJsonAsync(runId, cancellationToken);
            var (pmtilesPath, pmtilesFeatureCount) = await BuildPmtilesAsync(runId, geoJsonPath, cancellationToken);
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

    public static IReadOnlyList<string> GetTippecanoeArguments(string geoJsonPath, string outputPmtilesPath)
    {
        return
        [
            "--output", outputPmtilesPath,
            "--layer=trails",
            "--minimum-zoom=0",
            "-zg", // Automatically choose a maxzoom that should be sufficient to clearly distinguish the features and the detail within each feature
            "-r1",
            "--no-tile-size-limit",
            "--no-feature-limit",
            "--drop-densest-as-needed",
            "--force",
            geoJsonPath,
        ];
    }

    public static int ParseTippecanoeFeatureCount(string output)
    {
        if (string.IsNullOrWhiteSpace(output))
            return -1;

        var match = Regex.Match(output, @"Wrote\s+(?<count>\d+)\s+features", RegexOptions.IgnoreCase);
        if (match.Success && int.TryParse(match.Groups["count"].Value, out var count))
            return count;

        match = Regex.Match(output, @"(?<count>\d+)\s+features", RegexOptions.IgnoreCase);
        if (match.Success && int.TryParse(match.Groups["count"].Value, out count))
            return count;

        return -1;
    }

    public static bool IsValidPmtilesFile(string path, int minimumSizeBytes = MinimumPmtilesSizeBytes)
    {
        if (!File.Exists(path))
            return false;

        var fileInfo = new FileInfo(path);
        if (fileInfo.Length < minimumSizeBytes)
            return false;

        using var stream = File.OpenRead(path);
        var buffer = new byte[7];
        var bytesRead = stream.Read(buffer, 0, buffer.Length);
        if (bytesRead != buffer.Length)
            return false;

        return Encoding.UTF8.GetString(buffer) == "PMTiles";
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

    private async Task<(string OutputPath, int FeatureCount)> BuildPmtilesAsync(string runId, string geoJsonPath, CancellationToken cancellationToken)
    {
        if (!File.Exists(_tippecanoeBinary))
            throw new FileNotFoundException(
                $"Tippecanoe binary not found at '{_tippecanoeBinary}'. " +
                "Install tippecanoe or set the AppConfig.TippecanoeBinaryPath configuration.",
                _tippecanoeBinary);

        var tempPath = CreateRunTempPath(runId);
        var outputPath = Path.Combine(tempPath, "trails.pmtiles");

        var processStartInfo = new ProcessStartInfo(_tippecanoeBinary)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };

        foreach (var arg in GetTippecanoeArguments(geoJsonPath, outputPath))
        {
            processStartInfo.ArgumentList.Add(arg);
        }

        var stderr = new StringBuilder();
        var stdout = new StringBuilder();

        using var process = new Process { StartInfo = processStartInfo };
        process.OutputDataReceived += (_, e) => { if (!string.IsNullOrEmpty(e.Data)) stdout.AppendLine(e.Data); };
        process.ErrorDataReceived += (_, e) => { if (!string.IsNullOrEmpty(e.Data)) stderr.AppendLine(e.Data); };

        if (!process.Start())
            throw new InvalidOperationException("Failed to start tippecanoe process.");

        process.BeginOutputReadLine();
        process.BeginErrorReadLine();

        await process.WaitForExitAsync(cancellationToken);

        if (process.ExitCode != 0)
        {
            throw new InvalidOperationException($"Tippecanoe failed with exit code {process.ExitCode}: {stderr}");
        }

        var featureCount = ParseTippecanoeFeatureCount(stdout.ToString() + stderr.ToString());
        _logger.LogInformation("Tippecanoe finished for run {RunId} with {OutputBytes} bytes using binary {Binary}.", runId, new FileInfo(outputPath).Length, _tippecanoeBinary);
        return (outputPath, featureCount);
    }

    private static string? FindTippecanoeBinary()
    {
        var pathEnv = Environment.GetEnvironmentVariable("PATH");
        if (string.IsNullOrWhiteSpace(pathEnv))
            return null;

        foreach (var path in pathEnv.Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries))
        {
            var candidate = Path.Combine(path, "tippecanoe");
            if (File.Exists(candidate))
                return candidate;
        }

        return null;
    }

    private async Task ValidateAndPublishPmtilesAsync(BlobContainerClient container, string runId, string pmtilesPath, CancellationToken cancellationToken)
    {
        if (!IsValidPmtilesFile(pmtilesPath))
            throw new InvalidOperationException("Built PMTiles file did not pass validation.");

        var stagingBlob = container.GetBlobClient($"{StagingPrefix}/{runId}/trails.pmtiles");
        await stagingBlob.UploadAsync(pmtilesPath, overwrite: true, cancellationToken: cancellationToken);

        var productionBlob = container.GetBlobClient(ProductionBlobName);
        var copyResponse = await productionBlob.StartCopyFromUriAsync(stagingBlob.Uri, cancellationToken: cancellationToken);

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
