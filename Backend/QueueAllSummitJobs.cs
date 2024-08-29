using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Shared.Models;
using Shared.Services;
using static Backend.QueueSummitJobs;

namespace Backend
{
    public class QueueAllSummitJobs(CollectionClient<Activity> _cosmosClient)
    {
        [ServiceBusOutput("calculateSummitsJobs", Connection = "ServicebusConnection")]
        [Function(nameof(QueueAllSummitJobs))]
        public async Task<IEnumerable<CalculateSummitJob>> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequestData req)
        {
            var ids = await _cosmosClient.GetAllIds();
            return ids.Select(x => new CalculateSummitJob{ActivityId = x});
        }
    }
}