using System.Text.Json;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Shared.Constants;
using Shared.Models;
using Shared.Services;

namespace Backend;

public class DeleteAccountWorker(
    ILogger<DeleteAccountWorker> _logger,
    CollectionClient<Shared.Models.User> _usersCollection,
    CollectionClient<Session> _sessionsCollection,
    CollectionClient<Activity> _activitiesCollection,
    CollectionClient<SummitedPeak> _summitedPeaksCollection,
    CollectionClient<VisitedPath> _visitedPathsCollection,
    CollectionClient<VisitedArea> _visitedAreasCollection,
    CollectionClient<UserSyncItem> _userSyncItemsCollection,
    QueueClient _queueClient)
{
    private const int DeleteExpiryTtlSeconds = 1;

    [Function(nameof(DeleteAccountWorker))]
    public async Task Run(
        [QueueTrigger(QueueConfig.AccountDeleteJobs, AutoCompleteMessages = false)] QueueMessage message,
        QueueMessageActions actions,
        CancellationToken cancellationToken)
    {
        var deleteJob = message.Body.ToObjectFromJson<AccountDeleteJob>(new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
        if (deleteJob == null)
            throw new InvalidOperationException("Account delete job payload is invalid.");


        try
        {
            await _summitedPeaksCollection.ExpireDocumentsByKey("userId", deleteJob.UserId, deleteJob.UserId, DeleteExpiryTtlSeconds, cancellationToken);
            await _activitiesCollection.ExpireDocumentsByKey("userId", deleteJob.UserId, deleteJob.UserId, DeleteExpiryTtlSeconds, cancellationToken);
            await _visitedPathsCollection.ExpireDocumentsByKey("userId", deleteJob.UserId, deleteJob.UserId, DeleteExpiryTtlSeconds, cancellationToken);
            await _visitedAreasCollection.ExpireDocumentsByKey("userId", deleteJob.UserId, deleteJob.UserId, DeleteExpiryTtlSeconds, cancellationToken);
            await _userSyncItemsCollection.ExpireDocumentsByKey("userId", deleteJob.UserId, deleteJob.UserId, DeleteExpiryTtlSeconds, cancellationToken);
            await _sessionsCollection.DeleteDocumentsByKey("userId", deleteJob.UserId);
            await DeleteUserIfPresent(deleteJob, cancellationToken);

            await actions.CompleteMessageAsync(message, cancellationToken);
        }
        catch (Exception ex)
        {
            throw;
        }
    }

    private async Task DeleteUserIfPresent(AccountDeleteJob deleteJob, CancellationToken cancellationToken)
    {
        try
        {
            await _usersCollection.DeleteDocument(deleteJob.UserId, new PartitionKey(deleteJob.UserId), cancellationToken);
        }
        catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
        {
            _logger.LogInformation(
                "User {UserId} was already deleted or not found.",
                deleteJob.UserId);
        }
    }
}
