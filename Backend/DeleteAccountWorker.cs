using Shared.Jobs;
using System.Text.Json;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Constants;
using Shared.Models;
using Shared.Services;
using Shared.Services.Postgres;

namespace Backend;

public class DeleteAccountWorker(
    ILogger<DeleteAccountWorker> logger,
    IUserRepository users)
{
    [Function(nameof(DeleteAccountWorker))]
    public async Task Run(
        [QueueTrigger(QueueConfig.AccountDeleteJobs, AutoCompleteMessages = false)] QueueMessage message,
        QueueMessageActions actions,
        CancellationToken cancellationToken)
    {
        var deleteJob = JsonSerializer.Deserialize<AccountDeleteJob>(message.Body, new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
        if (deleteJob == null)
            throw new InvalidOperationException("Account delete job payload is invalid.");

        // The relational schema cascades user_id FKs from sessions, user_sync_items, activities
        // and every activity_* junction table. A single hard delete replaces the old per-container
        // TTL expiry (which only achieved eventual deletion) and the explicit session/user deletes.
        await users.Delete(deleteJob.UserId, cancellationToken);
        logger.LogInformation("Deleted account {UserId} and cascaded children", deleteJob.UserId);

        await actions.CompleteMessageAsync(message, cancellationToken);
    }
}
