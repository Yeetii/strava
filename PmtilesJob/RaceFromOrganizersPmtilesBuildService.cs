using System.Diagnostics;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using BAMCIS.GeoJSON;
using Microsoft.Extensions.Logging;
using Shared.Constants;
using Shared.Models;
using Shared.Services;
using System.Text;
using System.Text.Json;

namespace PmtilesJob;

/// <summary>
/// Builds race PMTiles directly from organizer blob documents, running
/// assembly inline using <see cref="RaceAssembler"/>.
/// </summary>
public class RaceFromOrganizersPmtilesBuildService
{
    private const string ProductionBlobName = "production/trails.pmtiles";
    private const string StagingPrefix = "staging";
    private const int MaxPendingTransparencyWrites = 16;
    private static readonly bool UseAnsiColors = !Console.IsOutputRedirected && string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("NO_COLOR"));

    private readonly BlobOrganizerStore _organizerStore;
    private readonly BlobServiceClient _blobServiceClient;
    private readonly PmtilesUtilityService _pmtilesUtilityService;
    private readonly ILocationGeocodingService _geocodingService;
    private readonly ILogger<RaceFromOrganizersPmtilesBuildService> _logger;
    private static readonly JsonSerializerOptions ConsoleJsonOptions = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = true,
    };
    private static readonly JsonSerializerOptions GeoJsonSerializerOptions = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = false,
    };

    static RaceFromOrganizersPmtilesBuildService()
    {
        GeoJsonSerializerOptions.Converters.Add(new GeometrySystemTextJsonConverter());
        GeoJsonSerializerOptions.Converters.Add(new FeatureIdJsonConverter());
    }

    public RaceFromOrganizersPmtilesBuildService(
        BlobOrganizerStore organizerStore,
        BlobServiceClient blobServiceClient,
        PmtilesUtilityService pmtilesUtilityService,
        ILocationGeocodingService geocodingService,
        ILogger<RaceFromOrganizersPmtilesBuildService> logger)
    {
        _organizerStore = organizerStore;
        _blobServiceClient = blobServiceClient;
        _pmtilesUtilityService = pmtilesUtilityService;
        _geocodingService = geocodingService;
        _logger = logger;
    }

    public async Task BuildAsync(bool writeTransparency, CancellationToken cancellationToken)
    {
        var runId = Guid.NewGuid().ToString("N");
        var buildStopwatch = Stopwatch.StartNew();
        _logger.LogInformation("Starting race-from-organizers tile build {RunId}. WriteTransparency={WriteTransparency}", runId, writeTransparency);

        try
        {
            var (geoJsonPath, featureCount, assemblyMetrics) = await AssembleAndExportToGeoJsonAsync(runId, writeTransparency, cancellationToken);

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
            buildStopwatch.Stop();

            var organizerReadMetrics = _organizerStore.GetLastOrganizerReadMetricsSnapshot();
            var pmtilesMetrics = _pmtilesUtilityService.GetLastPmtilesBuildMetricsSnapshot();
            _logger.LogInformation(
                "Build stage summary {RunId}: organizers {OrganizerCount} | blob bytes {BlobBytes} | cache hits {CacheHits} | geocode requests {GeocodeRequests} | geocode misses {GeocodeMisses} | assemble {AssembleElapsed} | transparency wait {TransparencyWaitElapsed} | project {ProjectionElapsed} | serialize {SerializationElapsed} | geojson size {GeoJsonBytes} | tippecanoe {TippecanoeElapsed} | pmtiles size {PmtilesBytes} | total {TotalElapsed}",
                runId,
                assemblyMetrics.OrganizerCount,
                BlobOrganizerStore.FormatByteCountForLogging(organizerReadMetrics.DownloadedBytes),
                organizerReadMetrics.CacheHits,
                assemblyMetrics.GeocodingMetrics.TotalRequests,
                assemblyMetrics.GeocodingMetrics.MissedRequests,
                assemblyMetrics.AssemblyElapsed,
                assemblyMetrics.TransparencyWaitElapsed,
                assemblyMetrics.FeatureProjectionElapsed,
                assemblyMetrics.SerializationElapsed,
                BlobOrganizerStore.FormatByteCountForLogging(assemblyMetrics.GeoJsonBytes),
                pmtilesMetrics.Elapsed,
                BlobOrganizerStore.FormatByteCountForLogging(pmtilesMetrics.OutputBytes),
                buildStopwatch.Elapsed);
            _logger.LogInformation("Race-from-organizers tile build completed successfully {RunId}. Wrote {FeatureCount} PMTiles features.", runId, pmtilesFeatureCount);
        }
        finally
        {
            DeleteRunTempPath(runId);
        }
    }

    public async Task DebugAssembleOrganizerAsync(string organizerId, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(organizerId))
            throw new InvalidOperationException("Organizer id is required.");

        var resolvedOrganizerId = await _organizerStore.ResolveOrganizerKeyAsync(organizerId, cancellationToken);
        var doc = await _organizerStore.GetByIdAsync(resolvedOrganizerId, cancellationToken)
            ?? throw new InvalidOperationException($"Organizer '{resolvedOrganizerId}' was not found.");

        var assembled = await RaceAssembler.AssembleRacesAsync(doc, _geocodingService, cancellationToken, _logger);
        _logger.LogInformation(
            "Assembled {RaceCount} races for organizer {OrganizerId}. Printing to console.",
            assembled.Count,
            resolvedOrganizerId);

        Console.WriteLine(JsonSerializer.Serialize(assembled, ConsoleJsonOptions));
    }

    private async Task<(string GeoJsonPath, int FeatureCount, AssemblyMetrics Metrics)> AssembleAndExportToGeoJsonAsync(string runId, bool writeTransparency, CancellationToken cancellationToken)
    {
        var totalStopwatch = Stopwatch.StartNew();
        var intervalStopwatch = Stopwatch.StartNew();
        var pendingTransparencyWrites = new List<Task<BlobOrganizerStore.TransparencyWriteStats>>(MaxPendingTransparencyWrites);
        var tempPath = CreateRunTempPath(runId);
        var geoJsonPath = Path.Combine(tempPath, "features-from-organizers.geojson");
        await using var fileStream = File.Create(geoJsonPath);
        await using var writer = new StreamWriter(fileStream, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false));
        var organizerCount = 0;
        var featureCount = 0;
        var totalAssembly = TimeSpan.Zero;
        var totalTransparencyWriteBackpressure = TimeSpan.Zero;
        var totalTransparencyQueued = 0;
        var totalTransparencyCompleted = 0;
        var totalTransparencySkips = 0;
        var totalRaceUploads = 0;
        var totalRaceDeletes = 0;
        var totalRaceUnchanged = 0;
        var totalFeatureProjection = TimeSpan.Zero;
        var intervalAssembly = TimeSpan.Zero;
        var intervalTransparencyWriteBackpressure = TimeSpan.Zero;
        var intervalTransparencyQueued = 0;
        var intervalTransparencyCompleted = 0;
        var intervalTransparencySkips = 0;
        var intervalRaceUploads = 0;
        var intervalRaceDeletes = 0;
        var intervalRaceUnchanged = 0;
        var intervalFeatureProjection = TimeSpan.Zero;

        await writer.WriteAsync("{\"type\":\"FeatureCollection\",\"features\":[");

        await foreach (var item in _organizerStore.StreamAllWithSourceAsync(maxConcurrency: 32, cancellationToken))
        {
            var doc = item.Document;
            organizerCount++;

            var assemblyStopwatch = Stopwatch.StartNew();
            var assembled = await RaceAssembler.AssembleRacesAsync(doc, _geocodingService, cancellationToken, _logger);
            assemblyStopwatch.Stop();
            totalAssembly += assemblyStopwatch.Elapsed;
            intervalAssembly += assemblyStopwatch.Elapsed;

            if (writeTransparency && item.Source == BlobOrganizerStore.OrganizerDocumentSource.Blob)
            {
                pendingTransparencyWrites.Add(_organizerStore.WriteAssembledRacesAsync(doc.Id, assembled, cancellationToken));
                totalTransparencyQueued++;
                intervalTransparencyQueued++;

                if (pendingTransparencyWrites.Count >= MaxPendingTransparencyWrites)
                {
                    var backpressureStopwatch = Stopwatch.StartNew();
                    var completedWrite = await WaitForOneTransparencyWriteAsync(pendingTransparencyWrites);
                    backpressureStopwatch.Stop();
                    totalTransparencyWriteBackpressure += backpressureStopwatch.Elapsed;
                    intervalTransparencyWriteBackpressure += backpressureStopwatch.Elapsed;
                    ApplyTransparencyWriteStats(
                        completedWrite,
                        ref totalTransparencyCompleted,
                        ref totalRaceUploads,
                        ref totalRaceDeletes,
                        ref totalRaceUnchanged,
                        ref intervalTransparencyCompleted,
                        ref intervalRaceUploads,
                        ref intervalRaceDeletes,
                        ref intervalRaceUnchanged);
                }
            }
            else
            {
                totalTransparencySkips++;
                intervalTransparencySkips++;
            }

            var featureProjectionStopwatch = Stopwatch.StartNew();
            foreach (var storedFeature in assembled)
            {
                var geoJsonFeature = storedFeature.ToFeature();
                if (geoJsonFeature.Id is not null)
                {
                    geoJsonFeature.Properties["featureId"] = geoJsonFeature.Id;
                }

                if (featureCount > 0)
                    await writer.WriteAsync(",");

                await writer.WriteAsync(JsonSerializer.Serialize(geoJsonFeature, GeoJsonSerializerOptions));
                featureCount++;
            }
            featureProjectionStopwatch.Stop();
            totalFeatureProjection += featureProjectionStopwatch.Elapsed;
            intervalFeatureProjection += featureProjectionStopwatch.Elapsed;

            if (organizerCount % 500 == 0)
            {
                var progressBlock = BuildProgressBlock(
                    organizerCount,
                    featureCount,
                    500,
                    intervalStopwatch.Elapsed,
                    intervalAssembly,
                    intervalFeatureProjection,
                    intervalTransparencyWriteBackpressure,
                    intervalTransparencyQueued,
                    intervalTransparencyCompleted,
                    intervalTransparencySkips,
                    pendingTransparencyWrites.Count,
                    intervalRaceUploads,
                    intervalRaceDeletes,
                    intervalRaceUnchanged,
                    totalStopwatch.Elapsed);
                _logger.LogInformation("{ProgressBlock}\n", progressBlock);

                intervalStopwatch.Restart();
                intervalAssembly = TimeSpan.Zero;
                intervalTransparencyQueued = 0;
                intervalTransparencyCompleted = 0;
                                intervalTransparencyWriteBackpressure = TimeSpan.Zero;
                                intervalTransparencySkips = 0;
                                intervalRaceUploads = 0;
                                intervalRaceDeletes = 0;
                                intervalRaceUnchanged = 0;
                                intervalFeatureProjection = TimeSpan.Zero;
            }
        }

        if (pendingTransparencyWrites.Count > 0)
        {
            var finalDrainStopwatch = Stopwatch.StartNew();
            var finalDrainResults = await Task.WhenAll(pendingTransparencyWrites);
            finalDrainStopwatch.Stop();
            totalTransparencyWriteBackpressure += finalDrainStopwatch.Elapsed;

            foreach (var completedWrite in finalDrainResults)
            {
                ApplyTransparencyWriteStats(
                    completedWrite,
                    ref totalTransparencyCompleted,
                    ref totalRaceUploads,
                    ref totalRaceDeletes,
                    ref totalRaceUnchanged,
                    ref intervalTransparencyCompleted,
                    ref intervalRaceUploads,
                    ref intervalRaceDeletes,
                    ref intervalRaceUnchanged);
            }
        }

        _logger.LogInformation(
                                "{CompletionBlock}",
                        BuildCompletionBlock(
                                organizerCount,
                                featureCount,
                                totalStopwatch.Elapsed,
                                totalAssembly,
                                totalFeatureProjection,
                                totalTransparencyQueued,
                                totalTransparencyCompleted,
                                totalTransparencySkips,
                                totalRaceUploads,
                                totalRaceDeletes,
                                totalRaceUnchanged,
                                totalTransparencyWriteBackpressure));

        var serializationStopwatch = Stopwatch.StartNew();

        await writer.WriteAsync("]}");
        await writer.FlushAsync(cancellationToken);
        await fileStream.FlushAsync(cancellationToken);
        serializationStopwatch.Stop();
        _logger.LogInformation(
            "GeoJSON serialization finished in {Elapsed}. Wrote {GeoJsonBytes} bytes to {GeoJsonPath}.",
            serializationStopwatch.Elapsed,
            new FileInfo(geoJsonPath).Length,
            geoJsonPath);

        var geocodingMetrics = default(GeocodingMetricsSnapshot);
        if (_geocodingService is NominatimLocationGeocodingService nominatim)
        {
            geocodingMetrics = nominatim.GetMetricsSnapshot();
            _logger.LogInformation(
                "Geocoding summary for build: total {TotalRequests} | cache hits {CacheHits} | cache misses {CacheMisses} | live requests {LiveRequests} | resolved {ResolvedRequests} | misses {MissedRequests} | 429s {RateLimitedResponses}",
                geocodingMetrics.TotalRequests,
                geocodingMetrics.CacheHits,
                geocodingMetrics.CacheMisses,
                geocodingMetrics.LiveRequests,
                geocodingMetrics.ResolvedRequests,
                geocodingMetrics.MissedRequests,
                geocodingMetrics.RateLimitedResponses);
        }

        return (
            geoJsonPath,
            featureCount,
            new AssemblyMetrics(
                organizerCount,
                totalAssembly,
                totalTransparencyWriteBackpressure,
                totalFeatureProjection,
                serializationStopwatch.Elapsed,
                new FileInfo(geoJsonPath).Length,
                geocodingMetrics));
    }

    private readonly record struct AssemblyMetrics(
        int OrganizerCount,
        TimeSpan AssemblyElapsed,
        TimeSpan TransparencyWaitElapsed,
        TimeSpan FeatureProjectionElapsed,
        TimeSpan SerializationElapsed,
        long GeoJsonBytes,
        GeocodingMetricsSnapshot GeocodingMetrics);

    private static string FormatElapsed(TimeSpan elapsed)
        => elapsed.ToString(@"hh\:mm\:ss\.fff");

    private static string BuildProgressBlock(
        int organizerCount,
        int featureCount,
        int intervalCount,
        TimeSpan intervalElapsed,
        TimeSpan intervalAssembly,
        TimeSpan intervalFeatureProjection,
        TimeSpan intervalTransparencyBackpressure,
        int intervalTransparencyQueued,
        int intervalTransparencyCompleted,
        int intervalTransparencySkips,
        int pendingTransparencyWrites,
        int intervalRaceUploads,
        int intervalRaceDeletes,
        int intervalRaceUnchanged,
        TimeSpan totalElapsed)
    {
        return string.Join(Environment.NewLine,
        [
            $"Progress: {AccentCount(organizerCount)} organizers | {AccentCount(featureCount)} features",
            $"  Last {AccentCount(intervalCount)}: elapsed {AccentTime(intervalElapsed)} | assemble {AccentTime(intervalAssembly)} | project {AccentTime(intervalFeatureProjection)} | write-wait {AccentTime(intervalTransparencyBackpressure)}",
            $"  Transparency organizers: queued {AccentWarn(intervalTransparencyQueued)} | written {AccentGood(intervalTransparencyCompleted)} | skipped {AccentMuted(intervalTransparencySkips)} | pending {AccentWarn(pendingTransparencyWrites)}",
            $"  Transparency races:      uploaded {AccentGood(intervalRaceUploads)} | deleted {AccentWarn(intervalRaceDeletes)} | unchanged {AccentMuted(intervalRaceUnchanged)}",
            $"  Total elapsed: {AccentTime(totalElapsed)}"
        ]);
    }

    private static string BuildCompletionBlock(
        int organizerCount,
        int featureCount,
        TimeSpan totalElapsed,
        TimeSpan totalAssembly,
        TimeSpan totalFeatureProjection,
        int totalTransparencyQueued,
        int totalTransparencyCompleted,
        int totalTransparencySkips,
        int totalRaceUploads,
        int totalRaceDeletes,
        int totalRaceUnchanged,
        TimeSpan totalTransparencyBackpressure)
    {
        return string.Join(Environment.NewLine,
        [
            $"Assembly complete: {AccentCount(organizerCount)} organizers | {AccentCount(featureCount)} features",
            $"  Total elapsed: {AccentTime(totalElapsed)}",
            $"  CPU-ish work: assemble {AccentTime(totalAssembly)} | project {AccentTime(totalFeatureProjection)}",
            $"  Transparency organizers: queued {AccentWarn(totalTransparencyQueued)} | written {AccentGood(totalTransparencyCompleted)} | skipped {AccentMuted(totalTransparencySkips)}",
            $"  Transparency races:      uploaded {AccentGood(totalRaceUploads)} | deleted {AccentWarn(totalRaceDeletes)} | unchanged {AccentMuted(totalRaceUnchanged)}",
            $"  Transparency wait: {AccentTime(totalTransparencyBackpressure)}",
            "Serializing GeoJSON..."
        ]);
    }

    private static string AccentCount(int value) => Colorize(value.ToString("N0", System.Globalization.CultureInfo.InvariantCulture), "1;36");

    private static string AccentGood(int value) => Colorize(value.ToString("N0", System.Globalization.CultureInfo.InvariantCulture), "1;32");

    private static string AccentWarn(int value) => Colorize(value.ToString("N0", System.Globalization.CultureInfo.InvariantCulture), "1;33");

    private static string AccentMuted(int value) => Colorize(value.ToString("N0", System.Globalization.CultureInfo.InvariantCulture), "0;37");

    private static string AccentTime(TimeSpan value) => Colorize(FormatElapsed(value), "1;35");

    private static string Colorize(string value, string ansiCode)
        => UseAnsiColors ? $"\u001b[{ansiCode}m{value}\u001b[0m" : value;

    private static void ApplyTransparencyWriteStats(
        BlobOrganizerStore.TransparencyWriteStats stats,
        ref int totalTransparencyCompleted,
        ref int totalRaceUploads,
        ref int totalRaceDeletes,
        ref int totalRaceUnchanged,
        ref int intervalTransparencyCompleted,
        ref int intervalRaceUploads,
        ref int intervalRaceDeletes,
        ref int intervalRaceUnchanged)
    {
        totalTransparencyCompleted += stats.OrganizerWrites;
        totalRaceUploads += stats.RaceUploads;
        totalRaceDeletes += stats.RaceDeletes;
        totalRaceUnchanged += stats.RaceUnchanged;
        intervalTransparencyCompleted += stats.OrganizerWrites;
        intervalRaceUploads += stats.RaceUploads;
        intervalRaceDeletes += stats.RaceDeletes;
        intervalRaceUnchanged += stats.RaceUnchanged;
    }

    private static async Task<BlobOrganizerStore.TransparencyWriteStats> WaitForOneTransparencyWriteAsync(
        List<Task<BlobOrganizerStore.TransparencyWriteStats>> pendingTransparencyWrites)
    {
        var completed = await Task.WhenAny(pendingTransparencyWrites);
        pendingTransparencyWrites.Remove(completed);
        return await completed;
    }

    private async Task ValidateAndPublishPmtilesAsync(BlobContainerClient container, string runId, string pmtilesPath, CancellationToken cancellationToken)
    {
        if (!PmtilesUtilityService.IsValidPmtilesFile(pmtilesPath))
            throw new InvalidOperationException("Built PMTiles file did not pass validation.");

        var pmtilesHttpHeaders = new BlobHttpHeaders { CacheControl = "no-cache", ContentType = "application/octet-stream" };

        var stagingBlob = container.GetBlobClient($"{StagingPrefix}/{runId}/trails-from-organizers.pmtiles");
        await stagingBlob.UploadAsync(pmtilesPath, new BlobUploadOptions { HttpHeaders = pmtilesHttpHeaders }, cancellationToken);

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
        var container = _blobServiceClient.GetBlobContainerClient(BlobContainerNames.RaceTiles);
        await container.CreateIfNotExistsAsync(PublicAccessType.Blob, cancellationToken: cancellationToken);
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
