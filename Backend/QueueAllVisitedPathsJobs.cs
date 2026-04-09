using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
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
            var activities = await _cosmosClient.FetchWholeCollection();
            var sender = serviceBusClient.CreateSender("calculateVisitedPathsJobs");
            foreach (var activity in activities)
            {
                await sender.SendMessageAsync(new ServiceBusMessage(activity.Id));
            }
        }
    }
}
