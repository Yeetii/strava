using BAMCIS.GeoJSON;
using Microsoft.Extensions.Logging;
using Shared.Models;
using Shared.Services.Postgres.Spatial;

namespace PmtilesJob;

public class AdminAreaPmtilesBuildService(
    IAdminBoundaryRepository adminBoundaries,
    PmtilesUtilityService pmtilesUtilityService,
    ILogger<AdminAreaPmtilesBuildService> logger)
{
    public static IReadOnlyList<int> DefaultAdminLevels { get; } = [2, 4];

    private readonly IAdminBoundaryRepository _adminBoundaries = adminBoundaries;
    private readonly PmtilesUtilityService _pmtilesUtilityService = pmtilesUtilityService;
    private readonly ILogger<AdminAreaPmtilesBuildService> _logger = logger;

    public async Task<int> BuildAdminAreasAsync(string outputPmtilesPath, IReadOnlyCollection<int> adminLevels, CancellationToken cancellationToken)
    {
        if (adminLevels.Count == 0)
            throw new InvalidOperationException("At least one admin level is required to build admin areas.");

        var runId = Guid.NewGuid().ToString("N");

        try
        {
            var orderedLevels = adminLevels
                .Distinct()
                .Order()
                .ToArray();

            var (layerInputs, featureCount) = await ExportAdminAreasToGeoJsonAsync(runId, orderedLevels, cancellationToken);
            await _pmtilesUtilityService.BuildPmtilesAsync(
                layerInputs,
                outputPmtilesPath,
                cancellationToken);

            _logger.LogInformation(
                "Built admin area PMTiles at {OutputPath} for admin levels {AdminLevels} with {FeatureCount} features.",
                outputPmtilesPath,
                string.Join(",", orderedLevels),
                featureCount);

            return featureCount;
        }
        finally
        {
            DeleteRunTempPath(runId);
        }
    }

    private async Task<(IReadOnlyCollection<(string LayerName, string GeoJsonPath)> LayerInputs, int FeatureCount)> ExportAdminAreasToGeoJsonAsync(
        string runId,
        IReadOnlyCollection<int> adminLevels,
        CancellationToken cancellationToken)
    {
        var featuresByLayer = new Dictionary<string, List<Feature>>(StringComparer.Ordinal);
        var totalFeatureCount = 0;

        foreach (var level in adminLevels)
        {
            var boundaries = await _adminBoundaries.GetByLevel(level, cancellationToken);
            foreach (var feature in boundaries)
            {
                var geoJsonFeature = feature.ToFeature();
                if (geoJsonFeature.Id is not null)
                {
                    geoJsonFeature.Properties["featureId"] = geoJsonFeature.Id;
                }

                var layerName = GetLayerName(feature, level);
                if (!featuresByLayer.TryGetValue(layerName, out var layerFeatures))
                {
                    layerFeatures = [];
                    featuresByLayer[layerName] = layerFeatures;
                }

                layerFeatures.Add(geoJsonFeature);
                totalFeatureCount++;
            }
        }

        var tempPath = CreateRunTempPath(runId);
        var levelSegment = string.Join("-", adminLevels.Order());
        var layerInputs = new List<(string LayerName, string GeoJsonPath)>();

        foreach (var (layerName, layerFeatures) in featuresByLayer.OrderBy(static entry => entry.Key, StringComparer.Ordinal))
        {
            var collection = new FeatureCollection(layerFeatures);
            var geoJsonPath = Path.Combine(tempPath, $"admin-levels-{levelSegment}-{layerName}.geojson");
            await File.WriteAllTextAsync(geoJsonPath, collection.ToJson(), cancellationToken);
            layerInputs.Add((layerName, geoJsonPath));
        }

        _logger.LogInformation(
            "Exported {Count} admin area features at levels {AdminLevels} across layers {LayerNames}.",
            totalFeatureCount,
            string.Join(",", adminLevels.Order()),
            string.Join(",", layerInputs.Select(static input => input.LayerName)));

        return (layerInputs, totalFeatureCount);
    }

    private static string GetLayerName(StoredFeature feature, int fallbackLevel)
    {
        if (feature.Properties.TryGetValue("adminLevel", out var adminLevelValue))
        {
            var adminLevelText = adminLevelValue?.ToString();
            if (int.TryParse(adminLevelText, out int parsedAdminLevel))
                return LayerNameForLevel(parsedAdminLevel);
        }

        return LayerNameForLevel(fallbackLevel);
    }

    private static string LayerNameForLevel(int adminLevel) => adminLevel switch
    {
        2 => "countries",
        4 => "regions",
        _ => $"admin_level_{adminLevel}",
    };

    private static string CreateRunTempPath(string runId)
    {
        var tempPath = Path.Combine(Path.GetTempPath(), "admin-area-tiles", runId);
        Directory.CreateDirectory(tempPath);
        return tempPath;
    }

    private void DeleteRunTempPath(string runId)
    {
        var tempPath = Path.Combine(Path.GetTempPath(), "admin-area-tiles", runId);
        if (!Directory.Exists(tempPath))
            return;

        try
        {
            Directory.Delete(tempPath, recursive: true);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to delete admin area temp path {TempPath} for run {RunId}.", tempPath, runId);
        }
    }
}
