using Microsoft.Extensions.Logging;
using Stigar.Core.Jobs;
using Stigar.Core.Constants;
using Stigar.Core.Services;
using Stigar.Core.Services.Postgres.Spatial;

namespace Stigar.Workers;

public class AdminBoundaryEnrichmentWorker(
    AdminBoundaryMetricsEnricher enricher,
    IAdminBoundaryRepository adminBoundaries,
    ILogger<AdminBoundaryEnrichmentWorker> logger) : IAdminBoundaryEnrichmentJob
{
    public async Task ExecuteAsync(AdminBoundaryIdJob job, CancellationToken cancellationToken)
    {
        var boundaryId = job.BoundaryId;

        IEnumerable<Stigar.Core.Models.StoredFeature> documents;
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

        try
        {
            logger.LogInformation("[AdminBoundaryEnrichmentWorker] Enriching admin boundary {BoundaryId}", document.Id);
            var metrics = await enricher.CalculateMetricsAsync(document, cancellationToken);
            await adminBoundaries.UpdateMetrics(
                document.Id, metrics, job.Epoch, cancellationToken);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[AdminBoundaryEnrichmentWorker] Failed to enrich admin boundary {BoundaryId}", boundaryId);
            throw;
        }
    }
}
