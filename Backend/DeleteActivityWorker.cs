using System.Text.Json;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Constants;
using Shared.Models;
using Shared.Services;
using Shared.Services.Postgres;

namespace Backend;

public class DeleteActivityWorker(
    ILogger<DeleteActivityWorker> logger,
    IActivityRepository activities)
{
    [Function(nameof(DeleteActivityWorker))]
    public async Task Run(
        [QueueTrigger(QueueConfig.ActivityDeleteJobs, AutoCompleteMessages = false)] QueueMessage message,
        QueueMessageActions actions,
        CancellationToken cancellationToken)
    {
        var deleteJob = message.Body.ToObjectFromJson<ActivityDeleteJob>(new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
        if (deleteJob == null)
            throw new InvalidOperationException("Activity delete job payload is invalid.");

        // activity_summits, activity_visited_paths and activity_visited_areas all have
        // activity_id FKs with ON DELETE CASCADE, so deleting the activity removes every
        // linked row. The old "find linked documents, then delete/patch each" walk is obsolete.
        await DeleteActivityIfPresent(deleteJob, cancellationToken);
        await actions.CompleteMessageAsync(message, cancellationToken);
    }

    private async Task DeleteActivityIfPresent(ActivityDeleteJob deleteJob, CancellationToken cancellationToken)
    {
        // Delete is idempotent in PostgreSQL: a missing row affects zero rows.
        await activities.Delete(deleteJob.ActivityId, cancellationToken);
        logger.LogInformation(
            "Deleted activity {ActivityId} for user {UserId}",
            deleteJob.ActivityId,
            deleteJob.UserId);
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
