using Shared.Jobs;
using Microsoft.Extensions.Logging;
using Shared.Constants;
using Shared.Models;
using Shared.Services;
using Shared.Services.Postgres;

namespace Backend;

public class DeleteAccountWorker(
    ILogger<DeleteAccountWorker> logger,
    IUserRepository users) : IAccountDeleteJob
{
    public async Task ExecuteAsync(AccountDeleteJob job, CancellationToken cancellationToken)
    {
        // The relational schema cascades user_id FKs from sessions, user_sync_items, activities
        // and every activity_* junction table. A single hard delete replaces the old per-container
        // TTL expiry (which only achieved eventual deletion) and the explicit session/user deletes.
        await users.Delete(job.UserId, cancellationToken);
        logger.LogInformation("Deleted account {UserId} and cascaded children", job.UserId);
    }
}
