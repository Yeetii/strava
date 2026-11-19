using Stigar.Core.Jobs;
using Microsoft.Extensions.Logging;
using Stigar.Core.Logging;
using Stigar.Core.Constants;
using Stigar.Core.Models;
using Stigar.Core.Services;
using Stigar.Core.Services.Postgres;

namespace Stigar.Workers;

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
        logger.LogInformation("Deleted account {UserId} and cascaded children", LogInput.Sanitize(job.UserId));
    }
}
