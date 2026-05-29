using System.Text.Json;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
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
    ServiceBusClient _serviceBusClient,
    ServiceBusAdministrationClient serviceBusAdministrationClient)
{
    [Function(nameof(DeleteAccountWorker))]
    public async Task Run(
        [ServiceBusTrigger(ServiceBusConfig.AccountDeleteJobs, Connection = "ServicebusConnection", AutoCompleteMessages = false)] ServiceBusReceivedMessage message,
        ServiceBusMessageActions actions,
        CancellationToken cancellationToken)
    {
        var deleteJob = message.Body.ToObjectFromJson<AccountDeleteJob>(new JsonSerializerOptions { PropertyNameCaseInsensitive = true });
        if (deleteJob == null)
            throw new InvalidOperationException("Account delete job payload is invalid.");

        if (await ServiceBusRescheduler.TryDeferForBackpressureAsync(
                serviceBusAdministrationClient,
                _serviceBusClient,
                ServiceBusConfig.AccountDeleteJobs,
                message,
                actions,
                _logger,
                cancellationToken))
        {
            return;
        }

        try
        {
            await _summitedPeaksCollection.DeleteDocumentsByKey("userId", deleteJob.UserId, deleteJob.UserId);
            await _activitiesCollection.DeleteDocumentsByKey("userId", deleteJob.UserId, deleteJob.UserId);
            await _visitedPathsCollection.DeleteDocumentsByKey("userId", deleteJob.UserId, deleteJob.UserId);
            await _visitedAreasCollection.DeleteDocumentsByKey("userId", deleteJob.UserId, deleteJob.UserId);
            await _userSyncItemsCollection.DeleteDocumentsByKey("userId", deleteJob.UserId, deleteJob.UserId);
            await _sessionsCollection.DeleteDocumentsByKey("userId", deleteJob.UserId);
            await _usersCollection.DeleteDocument(deleteJob.UserId, new PartitionKey(deleteJob.UserId));

            await actions.CompleteMessageAsync(message, cancellationToken);
        }
        catch (Exception ex)
        {
            if (ex is CosmosException { StatusCode: System.Net.HttpStatusCode.TooManyRequests })
            {
                ServiceBusRescheduler.RecordCosmosThrottle();
            }

            await ServiceBusRescheduler.HandleRetryAsync(
                ex,
                actions,
                message,
                _serviceBusClient,
                ServiceBusConfig.AccountDeleteJobs,
                _logger,
                cancellationToken);
        }
    }
}
