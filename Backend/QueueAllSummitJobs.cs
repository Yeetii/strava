using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Shared.Models;
using Shared.Services;

namespace Backend
{
    public class QueueAllSummitJobs(CollectionClient<Activity> _cosmosClient)
    {
        [ServiceBusOutput("calculateSummitsJobs", Connection = "ServicebusConnection")]
        [Function(nameof(QueueAllSummitJobs))]
        public async Task<IEnumerable<string>> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequestData req)
        {
            var activities = await _cosmosClient.FetchWholeCollection();
            return activities.Select(x => x.Id);
        }
    }
}