using Microsoft.Extensions.Logging;
using Shared.Jobs;
using Shared.Constants;
using Shared.Services;
using Shared.Services.Postgres.Spatial;

namespace Backend;

public class AdminBoundaryEnrichmentWorker(
    AdminBoundaryMetricsEnricher enricher,
    IAdminBoundaryRepository adminBoundaries,
    ILogger<AdminBoundaryEnrichmentWorker> logger) : IAdminBoundaryEnrichmentJob
{
    public async Task ExecuteAsync(AdminBoundaryIdJob job, CancellationToken cancellationToken)
    {
        var boundaryId = job.BoundaryId;

        IEnumerable<Shared.Models.StoredFeature> documents;
        try
        {
            documents = await adminBoundaries.GetByIds([boundaryId], cancellationToken);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[AdminBoundaryEnrichmentWorker] Failed to load admin boundary document");
            throw;
        }

        var documentsById = documents.ToDictionary(document => document.Id, StringComparer.Ordinal);
        if (!documentsById.TryGetValue(boundaryId, out var document))
        {
            logger.LogWarning("[AdminBoundaryEnrichmentWorker] Admin boundary document {BoundaryId} not found", boundaryId);
            return;
        }

        if (!OsmFeaturesChangeTrigger.ShouldEnrich(document))
        {
            logger.LogInformation("[AdminBoundaryEnrichmentWorker] Skipping already-enriched admin boundary {BoundaryId}", boundaryId);
            return;
        }

        try
        {
            logger.LogInformation("[AdminBoundaryEnrichmentWorker] Enriching admin boundary {BoundaryId}", document.Id);
            var metrics = await enricher.CalculateMetricsAsync(document, cancellationToken);
            await adminBoundaries.UpdateMetrics(
                document.Id, metrics, AdminBoundaryMetricsEnricher.MetricsVersion, cancellationToken);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[AdminBoundaryEnrichmentWorker] Failed to enrich admin boundary {BoundaryId}", boundaryId);
            throw;
        }
    }
}
