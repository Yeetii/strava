using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Shared.Models;
using Shared.Services;

namespace Backend
{
    public class QueueSummitJobs(CollectionClient<Activity> _cosmosClient)
    {
        public class CalculateSummitJob{
            public required string ActivityId { get; set; }
        }

        [ServiceBusOutput("calculateSummitsJobs", Connection = "ServicebusConnection")]
        [Function("QueueSummitJobs")]
        public async Task<IEnumerable<CalculateSummitJob>> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequestData req)
        {
            var ids = await _cosmosClient.GetAllIds();
            return ids.Select(x => new CalculateSummitJob{ActivityId = x});
        }
    }
}