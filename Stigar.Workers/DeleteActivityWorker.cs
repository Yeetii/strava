using Stigar.Core.Jobs;
using Microsoft.Extensions.Logging;
using Stigar.Core.Logging;
using Stigar.Core.Constants;
using Stigar.Core.Models;
using Stigar.Core.Services;
using Stigar.Core.Services.Postgres;

namespace Stigar.Workers;

public class DeleteActivityWorker(
    ILogger<DeleteActivityWorker> logger,
    IActivityRepository activities) : IActivityDeleteJob
{
    public async Task ExecuteAsync(ActivityDeleteJob job, CancellationToken cancellationToken)
    {
        // activity_summits, activity_visited_tiles and activity_visited_areas all have
        // activity_id FKs with ON DELETE CASCADE, so deleting the activity removes every
        // linked row. The old "find linked documents, then delete/patch each" walk is obsolete.
        await DeleteActivityIfPresent(job, cancellationToken);
    }

    private async Task DeleteActivityIfPresent(ActivityDeleteJob deleteJob, CancellationToken cancellationToken)
    {
        // Delete is idempotent in PostgreSQL: a missing row affects zero rows.
        await activities.Delete(deleteJob.ActivityId, cancellationToken);
        logger.LogInformation(
            "Deleted activity {ActivityId} for user {UserId}",
            LogInput.Sanitize(deleteJob.ActivityId),
            LogInput.Sanitize(deleteJob.UserId));
    }

    // The plan helpers below are kept because they are part of the type's public/internal contract
    // (and covered by tests). They are no longer used by the cascade-based delete path above.

    internal static CleanupPlan BuildCleanupPlan(
        IEnumerable<ActivityLinkedDocumentProjection> linkedDocs,
        string activityId)
    {
        var documentIdsToDelete = new List<string>();
        var patches = new List<ActivityIdsPatch>();

        foreach (var doc in linkedDocs)
        {
            var remainingActivityIds = (doc.ActivityIds ?? [])
                .Where(id => !string.Equals(id, activityId, StringComparison.Ordinal))
                .Distinct(StringComparer.Ordinal)
                .ToList();

            if (remainingActivityIds.Count == 0)
            {
                documentIdsToDelete.Add(doc.Id);
                continue;
            }

            patches.Add(new ActivityIdsPatch(doc.Id, remainingActivityIds));
        }

        return new CleanupPlan(documentIdsToDelete, patches);
    }

    internal sealed class ActivityLinkedDocumentProjection
    {
        public required string Id { get; set; }
        public HashSet<string>? ActivityIds { get; set; }
    }

    internal sealed record ActivityIdsPatch(string DocumentId, IReadOnlyList<string> RemainingActivityIds);

    internal sealed record CleanupPlan(
        IReadOnlyList<string> DocumentIdsToDelete,
        IReadOnlyList<ActivityIdsPatch> Patches);
}
