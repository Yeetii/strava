using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Shared.Constants;
using Shared.Models;
using Shared.Services;

namespace Backend
{
    public class QueueAllVisitedPathsJobs(CollectionClient<Activity> _cosmosClient, ServiceBusClient serviceBusClient)
    {
        [Function(nameof(QueueAllVisitedPathsJobs))]
        public async Task Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequestData req)
        {
            await QueueActivityCollectionJobs.QueueAllActivitiesAsync(
                _cosmosClient,
                serviceBusClient,
                ServiceBusConfig.CalculateVisitedPathsJobs);
        }
    }
}
